import base64
import json
import re
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# -----------------------------
# Constants
# -----------------------------
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PRIMARY = "#B04EF0"
KC_ACCENT = "#E060F0"
KC_DEEP = "#8030F0"
KC_SOFT = "#F6F0FF"

EXCLUDED_OWNER_CANON = "pipedrive krispcall"
CREDIT_EXCLUDE_DESCS = {"purchased credit", "credit purchased", "amount recharged"}

HEADER_FILL = PatternFill("solid", fgColor="EEEAFB")
HEADER_FONT = Font(bold=True)
HEADER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _get_secret(path: List[str], default=None):
    cur = st.secrets
    for key in path:
        if key not in cur:
            return default
        cur = cur[key]
    return cur


def _mixpanel_headers() -> Dict[str, str]:
    auth = _get_secret(["mixpanel", "authorization"])
    if not auth:
        raise RuntimeError("Missing mixpanel.authorization in Streamlit secrets.")
    return {"accept": "text/plain", "authorization": str(auth).strip()}


def _inject_brand_css():
    st.markdown(
        f"""
        <style>
          .kc-hero {{ padding: 18px 18px; border-radius: 18px; background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PRIMARY} 45%, {KC_ACCENT} 100%); color: white; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
          .kc-hero h1 {{ margin: 0; font-size: 28px; line-height: 1.2; }}
          .kc-hero p {{ margin: 6px 0 0 0; opacity: 0.95; font-size: 14px; }}
          .kc-card {{ background: white; border: 1px solid rgba(176, 78, 240, 0.18); border-radius: 16px; padding: 14px 14px; box-shadow: 0 10px 24px rgba(20, 6, 31, 0.04); }}
          .kc-muted {{ color: rgba(20, 6, 31, 0.72); }}
          div.stButton > button {{ border-radius: 14px !important; border: 0 !important; background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PRIMARY} 55%, {KC_ACCENT} 100%) !important; color: white !important; padding: 0.55rem 1rem !important; font-weight: 600 !important; }}
          section[data-testid="stFileUploaderDropzone"] {{ border-radius: 14px; border: 2px dashed rgba(176, 78, 240, 0.35); background: {KC_SOFT}; }}
          div[data-testid="stDataFrame"] {{ border-radius: 14px; overflow: hidden; }}
          .block-container {{ padding-top: 1.2rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _logo_html(width_px: int = 220, top_pad_px: int = 8) -> str:
    logo_path = Path(__file__).parent / "assets" / "KrispCallLogo.png"
    if not logo_path.exists():
        return ""
    b64 = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
    return f'<div style="padding-top:{top_pad_px}px;"><img src="data:image/png;base64,{b64}" style="width:{width_px}px; height:auto;" /></div>'


def require_login():
    st.session_state.setdefault("authenticated", False)
    if st.session_state["authenticated"]:
        return

    _inject_brand_css()
    c1, c2 = st.columns([1, 2], vertical_alignment="center")
    with c1:
        st.markdown(_logo_html(width_px=260, top_pad_px=14), unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="kc-hero"><h1>Payment Summary</h1><p>Secure login required.</p></div>', unsafe_allow_html=True)

    u = _get_secret(["auth", "username"])
    p = _get_secret(["auth", "password"])
    if not u or not p:
        st.error("Missing auth secrets. Add them to Streamlit secrets before using this app.")
        st.stop()

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Login"):
            if username == str(u) and password == str(p):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid credentials.")
    st.stop()


def _pick_col_ci(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _extract_emails(value) -> List[str]:
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass
    found = EMAIL_REGEX.findall(str(value))
    out: List[str] = []
    seen = set()
    for e in found:
        e2 = e.strip().lower()
        if e2 and e2 not in seen:
            seen.add(e2)
            out.append(e2)
    return out


def _normalize_first_email(value) -> Optional[str]:
    ems = _extract_emails(value)
    return ems[0] if ems else None


def _split_labels(value) -> List[str]:
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass
    parts = [p.strip() for p in str(value).split(",")]
    return [p for p in parts if p]


def _connected_from_labels(labels: List[str]) -> bool:
    labs = [str(l).strip().lower() for l in (labels or [])]
    if any(l == "not connected" for l in labs):
        return False
    if any("connected" in l for l in labs):
        return True
    return False


def _parse_time_to_dt(series: pd.Series) -> pd.Series:
    t = pd.to_numeric(series, errors="coerce")
    if t.dropna().empty:
        return pd.to_datetime(series, errors="coerce", utc=True)
    if float(t.median()) > 1e11:  # ms
        t = (t // 1000)
    return pd.to_datetime(t, unit="s", utc=True)


def dedupe_mixpanel_export(df: pd.DataFrame) -> pd.DataFrame:
    required = ["event", "distinct_id", "time", "$insert_id"]
    if df.empty or any(c not in df.columns for c in required):
        return df

    d = df.copy()

    t = pd.to_numeric(d["time"], errors="coerce")
    if t.notna().all() and not t.empty:
        if float(t.median()) > 1e11:
            t = (t // 1000)
        d["_time_s"] = t.astype("Int64")
    else:
        dt = pd.to_datetime(d["time"], errors="coerce", utc=True)
        d["_time_s"] = (dt.view("int64") // 10**9).astype("Int64")

    sort_cols = ["_time_s"]
    if "mp_processing_time_ms" in d.columns:
        sort_cols = ["mp_processing_time_ms"] + sort_cols

    d = d.sort_values(sort_cols, kind="mergesort")
    d = d.drop_duplicates(subset=["event", "distinct_id", "_time_s", "$insert_id"], keep="last")
    return d.drop(columns=["_time_s"], errors="ignore")


def _filter_credit_excluded(df: pd.DataFrame, text_col: Optional[str]) -> pd.DataFrame:
    if df.empty or not text_col or text_col not in df.columns:
        return df
    vals = df[text_col].astype(str).str.strip().str.lower()
    return df[~vals.isin(CREDIT_EXCLUDE_DESCS)].copy()


def _expand_leads_for_multiple_emails(df: pd.DataFrame, email_cols_priority: List[str]) -> Tuple[pd.DataFrame, List[int]]:
    missing_rows: List[int] = []
    expanded: List[dict] = []

    for i, row in df.iterrows():
        emails: List[str] = []
        for col in email_cols_priority:
            if col in df.columns:
                found = _extract_emails(row[col])
                if found:
                    emails = found
                    break

        if not emails:
            rec = row.to_dict()
            rec["email"] = None
            expanded.append(rec)
            missing_rows.append(i + 2)
            continue

        for e in emails:
            rec = row.to_dict()
            rec["email"] = e
            expanded.append(rec)

    return pd.DataFrame(expanded), missing_rows


def _windowed_payments_dual(
    payments_gross: pd.DataFrame,
    payments_credit_excluded: pd.DataFrame,
    amount_col: str,
    desc_col: Optional[str],
    days: int = 7,
) -> pd.DataFrame:
    d = payments_gross.dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(
            columns=[
                "email",
                "Total_Amount",
                "Total_Amount_creditExcluded",
                "First_Subscription",
                "First_Payment_Date",
                "Earliest_Event_Date",
            ]
        )

    d[amount_col] = pd.to_numeric(d[amount_col], errors="coerce").fillna(0.0)

    ce_groups = {}
    if not payments_credit_excluded.empty:
        ce = payments_credit_excluded.dropna(subset=["email"]).copy()
        ce[amount_col] = pd.to_numeric(ce[amount_col], errors="coerce").fillna(0.0)
        ce_groups = {e: g.sort_values("_dt", kind="mergesort") for e, g in ce.groupby("email", sort=False)}

    out = []
    for email, g in d.groupby("email", sort=False):
        g = g.sort_values("_dt", kind="mergesort")
        earliest_dt = g["_dt"].min()

        trigger = False
        start = None
        if desc_col and desc_col in g.columns:
            mask = g[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
            if mask.any():
                trigger = True
                start = g.loc[mask, "_dt"].min()

        if start is None:
            start = g["_dt"].min()

        end = start + timedelta(days=days)

        gross_total = float(g[(g["_dt"] >= start) & (g["_dt"] <= end)][amount_col].sum())

        g_ce = ce_groups.get(email)
        if g_ce is None:
            ce_total = 0.0
        else:
            ce_total = float(g_ce[(g_ce["_dt"] >= start) & (g_ce["_dt"] <= end)][amount_col].sum())

        out.append(
            {
                "email": email,
                "Total_Amount": gross_total,
                "Total_Amount_creditExcluded": ce_total,
                "First_Subscription": "TRUE" if trigger else "FALSE",
                "First_Payment_Date": start,
                "Earliest_Event_Date": earliest_dt,
            }
        )

    df = pd.DataFrame(out)
    if not df.empty:
        df["First_Payment_Date"] = pd.to_datetime(df["First_Payment_Date"], errors="coerce", utc=True).dt.tz_convert(None)
        df["Earliest_Event_Date"] = pd.to_datetime(df["Earliest_Event_Date"], errors="coerce", utc=True).dt.tz_convert(None)
    return df


def _nonzero_users_count(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    if "Total_Amount" not in df.columns:
        return pd.DataFrame(columns=group_cols + ["NonZero_Users"])
    d = df[df["Total_Amount"].fillna(0).ne(0)].dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(columns=group_cols + ["NonZero_Users"])
    return d.groupby(group_cols, as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})


def _reorder_cols(df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    pref = key_cols + [
        "Net_Amount",
        "Net_Amount_creditExcluded",
        "Total_Amount",
        "Total_Amount_creditExcluded",
        "Refund_Amount",
        "Refund_Amount_creditExcluded",
        "NonZero_Users",
    ]
    cols = [c for c in pref if c in df.columns] + [c for c in df.columns if c not in pref]
    return df[cols]


def _add_totals_row(df: pd.DataFrame, key_col: str, money_cols: List[str], nonzero_mode: str = "sum") -> pd.DataFrame:
    d = df.copy()
    totals = {}
    for c in money_cols:
        if c in d.columns:
            totals[c] = float(pd.to_numeric(d[c], errors="coerce").fillna(0).sum())

    if "NonZero_Users" in d.columns:
        if nonzero_mode == "sum":
            totals["NonZero_Users"] = int(pd.to_numeric(d["NonZero_Users"], errors="coerce").fillna(0).sum())
        elif nonzero_mode == "unique":
            totals["NonZero_Users"] = int(pd.to_numeric(d["NonZero_Users"], errors="coerce").fillna(0).max())

    row = {c: "" for c in d.columns}
    row[key_col] = "TOTAL"
    row.update(totals)
    return pd.concat([d, pd.DataFrame([row])], ignore_index=True)


@st.cache_data(show_spinner=False, ttl=600)
def fetch_mixpanel_event_export(project_id: int, base_url: str, from_date: date, to_date: date, event_name: str) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/api/2.0/export"
    params = {
        "project_id": int(project_id),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps([event_name]),
    }
    resp = requests.get(url, params=params, headers=_mixpanel_headers(), timeout=180)
    if resp.status_code != 200:
        body = (resp.text or "")[:500]
        raise RuntimeError(f"Mixpanel export failed for '{event_name}'. Status {resp.status_code}. Body: {body}")

    rows = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            rows.append(obj.get("properties", {}))
        except json.JSONDecodeError:
            continue

    df = pd.DataFrame(rows)
    if not df.empty and "time" in df.columns:
        df["_dt"] = _parse_time_to_dt(df["time"])
    return df


def _apply_header(ws):
    for cell in ws[1]:
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGN


def _freeze_header(ws):
    ws.freeze_panes = "A2"


def _autosize(ws, max_width: int = 60):
    for col_cells in ws.columns:
        col_letter = col_cells[0].column_letter
        length = 0
        for cell in col_cells[:200]:
            v = "" if cell.value is None else str(cell.value)
            length = max(length, len(v))
        ws.column_dimensions[col_letter].width = min(max(10, length + 2), max_width)


def _add_sheet(wb: Workbook, title: str, df: pd.DataFrame):
    ws = wb.create_sheet(title)
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)
    _apply_header(ws)
    _freeze_header(ws)
    _autosize(ws)
    return ws


def _build_excel(
    joined_export: pd.DataFrame,
    joined_nonzero_export: pd.DataFrame,
    email_summary: pd.DataFrame,
    owner_summary: pd.DataFrame,
    owner_x_connected: pd.DataFrame,
    connected_summary: pd.DataFrame,
    label_summary: pd.DataFrame,
    time_summary: pd.DataFrame,
    duplicates_df: pd.DataFrame,
    self_fact: pd.DataFrame,
    missing_email_rows: List[int],
    self_stats: Dict[str, float],
    sales_stats: Dict[str, float],
    from_date: date,
    to_date: date,
) -> Tuple[str, bytes]:
    wb = Workbook()
    wb.remove(wb.active)

    _add_sheet(wb, "Leads_with_Payments", joined_export)
    _add_sheet(wb, "Leads_Payments_NonZero", joined_nonzero_export)
    _add_sheet(wb, "Email_Summary", email_summary)

    ws_owner = _add_sheet(wb, "Owner_Summary", owner_summary)

    r0 = ws_owner.max_row + 2
    ws_owner.cell(row=r0, column=1, value="Self-Converted Revenue (workspace subscription emails not in leads summaries)")
    ws_owner.cell(row=r0 + 1, column=1, value="Self_Converted_Contacts")
    ws_owner.cell(row=r0 + 1, column=2, value=int(self_stats.get("count", 0)))
    ws_owner.cell(row=r0 + 2, column=1, value="Self_Converted_Net_Amount")
    ws_owner.cell(row=r0 + 2, column=2, value=float(self_stats.get("net", 0.0)))
    ws_owner.cell(row=r0 + 3, column=1, value="Self_Converted_Net_Amount_creditExcluded")
    ws_owner.cell(row=r0 + 3, column=2, value=float(self_stats.get("net_ce", 0.0)))

    ws_owner.cell(row=r0 + 5, column=1, value="Sales Attempt Revenue (leads summaries, Pipedrive KrispCall excluded)")
    ws_owner.cell(row=r0 + 6, column=1, value="Sales_Net_Amount")
    ws_owner.cell(row=r0 + 6, column=2, value=float(sales_stats.get("net", 0.0)))
    ws_owner.cell(row=r0 + 7, column=1, value="Sales_Net_Amount_creditExcluded")
    ws_owner.cell(row=r0 + 7, column=2, value=float(sales_stats.get("net_ce", 0.0)))

    _add_sheet(wb, "Owner_x_Connected", owner_x_connected)
    _add_sheet(wb, "Connected_Summary", connected_summary)
    _add_sheet(wb, "Label_Summary", label_summary)
    _add_sheet(wb, "Time_Summary", time_summary)

    if duplicates_df.empty:
        ws_dup = wb.create_sheet("Duplicate_Leads_By_Email")
        ws_dup.append(["No duplicate emails found in leads summaries (after excluding Pipedrive KrispCall)."])
        _apply_header(ws_dup)
        _freeze_header(ws_dup)
        _autosize(ws_dup)
    else:
        _add_sheet(wb, "Duplicate_Leads_By_Email", duplicates_df)

    _add_sheet(wb, "SelfConverted_Emails", self_fact)

    ws_log = wb.create_sheet("Logs")
    ws_log.append(["Missing email rows (Excel row numbers):"])
    ws_log.append([", ".join(map(str, missing_email_rows[:200]))])
    ws_log.append([f"Total missing email rows: {len(missing_email_rows)}"])
    _apply_header(ws_log)
    _freeze_header(ws_log)
    _autosize(ws_log)

    out = BytesIO()
    wb.save(out)
    fname = f"payment_summary_{from_date.strftime('%b%d').lower()}_{to_date.strftime('%b%d').lower()}.xlsx"
    return fname, out.getvalue()


def main():
    st.set_page_config(page_title="KrispCall Payment Summary", page_icon="📈", layout="wide")
    require_login()
    _inject_brand_css()

    st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
    l, r = st.columns([1, 3], vertical_alignment="center")
    with l:
        st.markdown(_logo_html(width_px=240, top_pad_px=10), unsafe_allow_html=True)
    with r:
        st.markdown(
            '<div class="kc-hero"><h1>KrispCall Payment Summary</h1><p>Leads reconciliation with Mixpanel transactions</p></div>',
            unsafe_allow_html=True,
        )

    with st.sidebar:
        st.markdown("### Date Selection")
        today = date.today()
        first_of_month = today.replace(day=1)
        default_start = first_of_month - timedelta(days=4)
        default_end = today - timedelta(days=1)
        from_date = st.date_input("Date from", value=default_start)
        to_date = st.date_input("Date to", value=default_end)

        st.markdown("---")
        st.markdown("### Rules")
        st.info(
            "Summaries exclude owner 'Pipedrive KrispCall', then dedupe by email keeping the lowest Lead created datetime. "
            "Time summary groups by Lead created date. Main tables keep all rows."
        )

    st.markdown('<div class="kc-card">', unsafe_allow_html=True)
    leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])
    run = st.button("Run Analysis", type="primary", disabled=(leads_file is None))
    st.markdown("</div>", unsafe_allow_html=True)

    if not run:
        st.stop()

    logs: List[str] = []
    with st.spinner("Running analysis..."):
        leads_raw = pd.read_csv(leads_file)

        owner_col = _pick_col_ci(leads_raw, ["Lead - Owner", "Deal - Owner", "Owner", "owner"]) or "Owner"
        lead_created_col = _pick_col_ci(leads_raw, ["Lead - Lead created on", "Lead created on", "Lead - Created on", "Created on"])

        email_cols_priority: List[str] = []
        for cand in [
            "Person - Email",
            "Lead - User Email",
            "Person - Email - Work",
            "Person - Email - Other",
            "Person - Email - Home",
            "email",
        ]:
            c = _pick_col_ci(leads_raw, [cand])
            if c and c not in email_cols_priority:
                email_cols_priority.append(c)

        leads_expanded, missing_email_rows = _expand_leads_for_multiple_emails(leads_raw, email_cols_priority)
        if missing_email_rows:
            logs.append(f"Missing email for {len(missing_email_rows)} row(s). Example rows: {missing_email_rows[:15]}")

        label_col = _pick_col_ci(leads_expanded, ["Lead - Label", "Label", "Labels"])
        if label_col:
            leads_expanded["_labels_list_obj"] = leads_expanded[label_col].apply(_split_labels)
        else:
            leads_expanded["_labels_list_obj"] = [[]] * len(leads_expanded)
        leads_expanded["Connected"] = leads_expanded["_labels_list_obj"].apply(_connected_from_labels)

        if lead_created_col and lead_created_col in leads_expanded.columns:
            leads_expanded["_lead_created_dt"] = pd.to_datetime(leads_expanded[lead_created_col], errors="coerce")
        else:
            leads_expanded["_lead_created_dt"] = pd.NaT

        leads_expanded["labels_list"] = leads_expanded["_labels_list_obj"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")

        exclude_mask = leads_expanded[owner_col].astype(str).str.strip().str.lower().eq(EXCLUDED_OWNER_CANON)
        leads_for_sum = leads_expanded[~exclude_mask].copy()

        leads_for_sum["_rank_dt"] = leads_for_sum["_lead_created_dt"].fillna(pd.Timestamp.max)
        leads_for_sum["_row_order"] = np.arange(len(leads_for_sum))
        leads_for_sum = leads_for_sum.sort_values(["email", "_rank_dt", "_row_order"], kind="mergesort")

        dup_email_mask = leads_for_sum["email"].notna() & leads_for_sum.duplicated(subset=["email"], keep=False)
        duplicates_df = leads_for_sum[dup_email_mask].copy()
        if not duplicates_df.empty:
            duplicates_df["Is_Chosen_For_Summaries"] = duplicates_df.groupby("email").cumcount().eq(0)

        chosen = leads_for_sum.dropna(subset=["email"]).drop_duplicates(subset=["email"], keep="first").copy()
        lead_email_set = set(chosen["email"].dropna().unique())
        lead_emails_excl_owner = set(leads_for_sum["email"].dropna().unique())

        pid = int(_get_secret(["mixpanel", "project_id"]))
        base_url = _get_secret(["mixpanel", "base_url"], "https://data-eu.mixpanel.com")

        payments_raw = fetch_mixpanel_event_export(pid, base_url, from_date, to_date, "New Payment Made")
        window_days = (to_date - from_date).days
        refund_from = (to_date - timedelta(days=60)) if window_days < 60 else from_date
        refunds_raw = fetch_mixpanel_event_export(pid, base_url, refund_from, to_date, "Refund Granted")

        payments = dedupe_mixpanel_export(payments_raw)
        refunds = dedupe_mixpanel_export(refunds_raw)

        if not payments.empty and "time" in payments.columns and "_dt" not in payments.columns:
            payments["_dt"] = _parse_time_to_dt(payments["time"])
        if not refunds.empty and "time" in refunds.columns and "_dt" not in refunds.columns:
            refunds["_dt"] = _parse_time_to_dt(refunds["time"])

        pay_email_col = _pick_col_ci(payments, ["$email", "email", "Email", "EMAIL", "User Email", "user.email", "User.Email"])
        ref_email_col = _pick_col_ci(refunds, ["User Email", "user.email", "User.Email", "$email", "email", "Email", "EMAIL"])

        payments["email"] = payments[pay_email_col].apply(_normalize_first_email) if pay_email_col else None
        refunds["email"] = refunds[ref_email_col].apply(_normalize_first_email) if ref_email_col else None

        amount_col = _pick_col_ci(payments, ["Amount", "amount", "Amount Paid"])
        desc_col = _pick_col_ci(payments, ["Amount Description", "description", "Plan"])
        refund_amount_col = _pick_col_ci(refunds, ["Refund Amount", "refund_amount", "Amount"])
        refund_desc_col = _pick_col_ci(
            refunds,
            ["Refunded Transaction description", "Refunded Transaction Description", "Refunded Transaction", "Refunded transaction description"],
        )

        if not amount_col:
            raise RuntimeError("Could not find payment amount column in Mixpanel export (expected 'Amount').")
        if not refund_amount_col:
            refunds["Refund Amount"] = 0.0
            refund_amount_col = "Refund Amount"

        payments[amount_col] = pd.to_numeric(payments[amount_col], errors="coerce").fillna(0.0)
        refunds[refund_amount_col] = pd.to_numeric(refunds[refund_amount_col], errors="coerce").fillna(0.0)

        payments_ce = _filter_credit_excluded(payments, desc_col)
        refunds_ce = _filter_credit_excluded(refunds, refund_desc_col)

        pay_leads = payments[payments["email"].isin(lead_email_set)].copy()
        pay_leads_ce = payments_ce[payments_ce["email"].isin(lead_email_set)].copy()
        ref_leads = refunds[refunds["email"].isin(lead_email_set)].copy()
        ref_leads_ce = refunds_ce[refunds_ce["email"].isin(lead_email_set)].copy()

        pay_summary = _windowed_payments_dual(pay_leads, pay_leads_ce, amount_col, desc_col, days=7)

        ref_sum = (
            ref_leads.dropna(subset=["email"])
            .groupby("email", as_index=False)[refund_amount_col]
            .sum()
            .rename(columns={refund_amount_col: "Refund_Amount"})
        )
        ref_sum_ce = (
            ref_leads_ce.dropna(subset=["email"])
            .groupby("email", as_index=False)[refund_amount_col]
            .sum()
            .rename(columns={refund_amount_col: "Refund_Amount_creditExcluded"})
        )

        email_fact = pay_summary.merge(ref_sum, on="email", how="outer").merge(ref_sum_ce, on="email", how="outer")

        for c in ["Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded"]:
            if c not in email_fact.columns:
                email_fact[c] = 0.0
            email_fact[c] = pd.to_numeric(email_fact[c], errors="coerce").fillna(0.0)

        email_fact["First_Subscription"] = email_fact.get("First_Subscription", "FALSE").fillna("FALSE")
        email_fact["Net_Amount"] = email_fact["Total_Amount"] - email_fact["Refund_Amount"]
        email_fact["Net_Amount_creditExcluded"] = email_fact["Total_Amount_creditExcluded"] - email_fact["Refund_Amount_creditExcluded"]

        fact_pref = [
            "email",
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
            "First_Subscription",
            "First_Payment_Date",
            "Earliest_Event_Date",
        ]
        email_fact = email_fact[[c for c in fact_pref if c in email_fact.columns] + [c for c in email_fact.columns if c not in fact_pref]]

        joined = leads_expanded.merge(email_fact[[c for c in fact_pref if c in email_fact.columns]], on="email", how="left")
        for c in ["Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded", "Net_Amount", "Net_Amount_creditExcluded"]:
            if c in joined.columns:
                joined[c] = pd.to_numeric(joined[c], errors="coerce").fillna(0.0)
        joined["First_Subscription"] = joined.get("First_Subscription", "FALSE").fillna("FALSE")

        joined_export = joined.copy()
        joined_nonzero = joined_export[joined_export["Total_Amount"].fillna(0).ne(0)].copy()

        totals_row = {c: "" for c in joined_export.columns}
        totals_row["email"] = "TOTAL (unique emails)"
        for c in ["Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded"]:
            if c in totals_row:
                totals_row[c] = float(pd.to_numeric(email_fact.get(c, 0.0), errors="coerce").fillna(0).sum())
        joined_export = pd.concat([joined_export, pd.DataFrame([totals_row])], ignore_index=True)
        joined_nonzero = pd.concat([joined_nonzero, pd.DataFrame([totals_row])], ignore_index=True)

        money_cols = [
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
        ]

        chosen_base = chosen.merge(email_fact, on="email", how="left")
        for c in money_cols:
            if c not in chosen_base.columns:
                chosen_base[c] = 0.0
            chosen_base[c] = pd.to_numeric(chosen_base[c], errors="coerce").fillna(0.0)

        owner_summary = chosen_base.groupby(owner_col, as_index=False)[money_cols].sum().rename(columns={owner_col: "Lead - Owner"})
        owner_summary = owner_summary.merge(
            _nonzero_users_count(chosen_base.rename(columns={owner_col: "Lead - Owner"}), ["Lead - Owner"]),
            on="Lead - Owner",
            how="left",
        ).fillna({"NonZero_Users": 0})
        owner_summary = owner_summary.sort_values("Net_Amount", ascending=False)
        owner_summary = _reorder_cols(owner_summary, ["Lead - Owner"])
        owner_summary = _add_totals_row(owner_summary, "Lead - Owner", money_cols, nonzero_mode="sum")

        connected_summary = chosen_base.groupby("Connected", as_index=False)[money_cols].sum()
        connected_summary = connected_summary.merge(_nonzero_users_count(chosen_base, ["Connected"]), on="Connected", how="left").fillna({"NonZero_Users": 0})
        connected_summary = connected_summary.sort_values("Net_Amount", ascending=False)
        connected_summary = _reorder_cols(connected_summary, ["Connected"])
        connected_summary = _add_totals_row(connected_summary, "Connected", money_cols, nonzero_mode="sum")

        owner_x_connected = chosen_base.groupby([owner_col, "Connected"], as_index=False)[money_cols].sum().rename(columns={owner_col: "Lead - Owner"})
        owner_x_connected = owner_x_connected.merge(
            _nonzero_users_count(chosen_base.rename(columns={owner_col: "Lead - Owner"}), ["Lead - Owner", "Connected"]),
            on=["Lead - Owner", "Connected"],
            how="left",
        ).fillna({"NonZero_Users": 0})
        owner_x_connected = owner_x_connected.sort_values("Net_Amount", ascending=False)
        owner_x_connected = _reorder_cols(owner_x_connected, ["Lead - Owner", "Connected"])
        owner_x_connected = _add_totals_row(owner_x_connected, "Lead - Owner", money_cols, nonzero_mode="sum")

        labels_exp = chosen_base[["email", "labels_list"] + money_cols].copy()
        labels_exp["labels_list"] = labels_exp["labels_list"].astype(str).str.split(r"\s*,\s*")
        labels_exp = labels_exp.explode("labels_list").rename(columns={"labels_list": "Label"})
        labels_exp["Label"] = labels_exp["Label"].fillna("").astype(str).str.strip()
        labels_exp = labels_exp[labels_exp["Label"] != ""].drop_duplicates(subset=["email", "Label"])
        labels_exp["Connected_Label"] = labels_exp["Label"].str.lower().apply(lambda s: False if s.strip() == "not connected" else ("connected" in s))

        label_summary = labels_exp.groupby(["Label", "Connected_Label"], as_index=False)[money_cols].sum()
        label_counts = (
            labels_exp[labels_exp["Total_Amount"].fillna(0).ne(0)]
            .dropna(subset=["email"])
            .groupby(["Label", "Connected_Label"], as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "NonZero_Users"})
        )
        label_summary = label_summary.merge(label_counts, on=["Label", "Connected_Label"], how="left").fillna({"NonZero_Users": 0})
        label_summary = label_summary.sort_values("Net_Amount", ascending=False)
        label_summary = _reorder_cols(label_summary, ["Label", "Connected_Label"])
        label_summary = _add_totals_row(label_summary, "Label", money_cols, nonzero_mode="sum")

        email_summary = email_fact[email_fact["email"].isin(lead_email_set)].sort_values("Net_Amount", ascending=False).copy()
        email_summary = _reorder_cols(email_summary, ["email"])

        time_base = chosen_base[["email", "_lead_created_dt"] + money_cols].copy()
        time_base["Lead_Created_Date"] = pd.to_datetime(time_base["_lead_created_dt"], errors="coerce").dt.date
        time_base = time_base[time_base["Lead_Created_Date"].notna()].copy()

        time_summary = time_base.groupby("Lead_Created_Date", as_index=False)[money_cols].sum()
        nonzero_by_day = time_base[time_base["Total_Amount"].fillna(0).ne(0)].groupby("Lead_Created_Date", as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})
        time_summary = time_summary.merge(nonzero_by_day, on="Lead_Created_Date", how="left").fillna({"NonZero_Users": 0})
        time_summary = time_summary.sort_values("Lead_Created_Date")
        time_summary = _reorder_cols(time_summary, ["Lead_Created_Date"])
        time_summary = _add_totals_row(time_summary, "Lead_Created_Date", money_cols, nonzero_mode="sum")

        if desc_col and desc_col in payments.columns:
            ws_mask = payments[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
            ws_emails = set(payments.loc[ws_mask, "email"].dropna().unique())
        else:
            ws_emails = set()

        self_emails = sorted(ws_emails - lead_emails_excl_owner)

        pay_self = payments[payments["email"].isin(self_emails)].copy()
        pay_self_ce = payments_ce[payments_ce["email"].isin(self_emails)].copy()
        ref_self = refunds[refunds["email"].isin(self_emails)].copy()
        ref_self_ce = refunds_ce[refunds_ce["email"].isin(self_emails)].copy()

        if not pay_self.empty:
            self_pay_summary = _windowed_payments_dual(pay_self, pay_self_ce, amount_col, desc_col, days=7)
        else:
            self_pay_summary = pd.DataFrame(columns=["email", "Total_Amount", "Total_Amount_creditExcluded", "First_Subscription", "First_Payment_Date", "Earliest_Event_Date"])

        self_ref_sum = (
            ref_self.dropna(subset=["email"])
            .groupby("email", as_index=False)[refund_amount_col]
            .sum()
            .rename(columns={refund_amount_col: "Refund_Amount"})
        )
        self_ref_sum_ce = (
            ref_self_ce.dropna(subset=["email"])
            .groupby("email", as_index=False)[refund_amount_col]
            .sum()
            .rename(columns={refund_amount_col: "Refund_Amount_creditExcluded"})
        )

        self_fact = self_pay_summary.merge(self_ref_sum, on="email", how="outer").merge(self_ref_sum_ce, on="email", how="outer")
        for c in ["Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded"]:
            if c not in self_fact.columns:
                self_fact[c] = 0.0
            self_fact[c] = pd.to_numeric(self_fact[c], errors="coerce").fillna(0.0)
        self_fact["First_Subscription"] = self_fact.get("First_Subscription", "FALSE").fillna("FALSE")
        self_fact["Net_Amount"] = self_fact["Total_Amount"] - self_fact["Refund_Amount"]
        self_fact["Net_Amount_creditExcluded"] = self_fact["Total_Amount_creditExcluded"] - self_fact["Refund_Amount_creditExcluded"]
        self_fact = _reorder_cols(self_fact, ["email"])

        self_stats = {
            "count": int(len(self_emails)),
            "net": float(pd.to_numeric(self_fact.get("Net_Amount", 0.0), errors="coerce").fillna(0).sum()),
            "net_ce": float(pd.to_numeric(self_fact.get("Net_Amount_creditExcluded", 0.0), errors="coerce").fillna(0).sum()),
        }
        sales_stats = {
            "net": float(pd.to_numeric(email_fact.get("Net_Amount", 0.0), errors="coerce").fillna(0).sum()),
            "net_ce": float(pd.to_numeric(email_fact.get("Net_Amount_creditExcluded", 0.0), errors="coerce").fillna(0).sum()),
        }

        fname, excel_bytes = _build_excel(
            joined_export=joined_export,
            joined_nonzero_export=joined_nonzero,
            email_summary=email_summary,
            owner_summary=owner_summary,
            owner_x_connected=owner_x_connected,
            connected_summary=connected_summary,
            label_summary=label_summary,
            time_summary=time_summary,
            duplicates_df=duplicates_df,
            self_fact=self_fact,
            missing_email_rows=missing_email_rows,
            self_stats=self_stats,
            sales_stats=sales_stats,
            from_date=from_date,
            to_date=to_date,
        )

    tab_overview, tab_main, tab_summaries, tab_time, tab_export, tab_logs = st.tabs(
        ["Overview", "Main Tables", "Summaries", "Time", "Exports", "Logs"]
    )

    with tab_overview:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Payments (windowed)", f"{email_summary['Total_Amount'].sum():,.2f}")
        c2.metric("Total Refunds", f"{email_summary['Refund_Amount'].sum():,.2f}")
        c3.metric("Net Revenue", f"{email_summary['Net_Amount'].sum():,.2f}")
        nonzero_users = int((email_summary["Total_Amount"].fillna(0).ne(0)).sum())
        c4.metric("Non-zero Users (emails)", f"{nonzero_users:,}")

        st.caption("Overview reflects summaries only (Pipedrive KrispCall excluded and deduped).")

    with tab_main:
        st.markdown("#### Leads with payments (includes all owners)")
        st.dataframe(joined_export, use_container_width=True)
        st.markdown("#### Leads with non-zero payments only")
        st.dataframe(joined_nonzero, use_container_width=True)

    with tab_summaries:
        st.markdown("#### Owner summary (excluded owner removed. Deduped)")
        st.dataframe(owner_summary, use_container_width=True)
        st.markdown("#### Owner x Connected")
        st.dataframe(owner_x_connected, use_container_width=True)
        st.markdown("#### Connected summary")
        st.dataframe(connected_summary, use_container_width=True)
        st.markdown("#### Label summary")
        st.dataframe(label_summary, use_container_width=True)
        st.markdown("#### Email summary")
        st.dataframe(email_summary, use_container_width=True)
        if not duplicates_df.empty:
            st.markdown("#### Duplicate emails in leads summaries (debug)")
            st.dataframe(duplicates_df, use_container_width=True)

    with tab_time:
        st.markdown("#### Time summary grouped by Lead created date")
        st.dataframe(time_summary, use_container_width=True)

    with tab_export:
        st.download_button(
            "Download Excel report",
            data=excel_bytes,
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with tab_logs:
        if logs:
            for line in logs:
                st.info(line)
        else:
            st.write("No issues logged.")


if __name__ == "__main__":
    main()
