import base64
import json
import re
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

# -----------------------------
# Constants / Branding
# -----------------------------
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PRIMARY = "#B04EF0"
KC_ACCENT = "#E060F0"
KC_DEEP = "#8030F0"
KC_SOFT = "#F6F0FF"

EXCLUDED_OWNER_CANON = "pipedrive krispcall"
CREDIT_EXCLUDE_DESCS = {"purchased credit", "credit purchased", "amount recharged"}


# -----------------------------
# Secrets / Auth
# -----------------------------
def _get_secret(path: List[str], default=None):
    cur = st.secrets
    for key in path:
        if key not in cur:
            return default
        cur = cur[key]
    return cur


def require_login():
    st.session_state.setdefault("authenticated", False)
    if st.session_state["authenticated"]:
        return

    u = _get_secret(["auth", "username"])
    p = _get_secret(["auth", "password"])
    if not u or not p:
        st.error("Missing auth secrets. Add [auth] username/password to Streamlit secrets.")
        st.stop()

    st.markdown("### Login")
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


def _mixpanel_headers() -> Dict[str, str]:
    auth = _get_secret(["mixpanel", "authorization"])
    if not auth:
        raise RuntimeError("Missing mixpanel.authorization in Streamlit secrets.")
    # ensure 'Basic ' prefix exists
    a = str(auth).strip()
    if not a.lower().startswith("basic "):
        a = "Basic " + a
    return {"accept": "text/plain", "authorization": a}


# -----------------------------
# Helpers
# -----------------------------
def _inject_brand_css():
    st.markdown(
        f"""
        <style>
          .kc-hero {{ padding: 18px 18px; border-radius: 18px; background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PRIMARY} 45%, {KC_ACCENT} 100%); color: white; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
          .kc-hero h1 {{ margin: 0; font-size: 28px; line-height: 1.2; }}
          .kc-hero p {{ margin: 6px 0 0 0; opacity: 0.95; font-size: 14px; }}
          .kc-card {{ background: white; border: 1px solid rgba(176, 78, 240, 0.18); border-radius: 16px; padding: 14px 14px; box-shadow: 0 10px 24px rgba(20, 6, 31, 0.04); }}
          div.stButton > button {{ border-radius: 14px !important; border: 0 !important; background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PRIMARY} 55%, {KC_ACCENT} 100%) !important; color: white !important; padding: 0.55rem 1rem !important; font-weight: 600 !important; }}
          section[data-testid="stFileUploaderDropzone"] {{ border-radius: 14px; border: 2px dashed rgba(176, 78, 240, 0.35); background: {KC_SOFT}; }}
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


def _parse_time_to_dt(series: pd.Series) -> pd.Series:
    t = pd.to_numeric(series, errors="coerce")
    if t.dropna().empty:
        return pd.to_datetime(series, errors="coerce", utc=True)
    if float(t.median()) > 1e11:
        t = (t // 1000)
    return pd.to_datetime(t, unit="s", utc=True)


def _extract_emails(value) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    found = EMAIL_REGEX.findall(str(value))
    out: List[str] = []
    seen = set()
    for e in found:
        e2 = e.strip().lower()
        if e2 and e2 not in seen:
            seen.add(e2)
            out.append(e2)
    return out


def _normalize_email(value) -> Optional[str]:
    ems = _extract_emails(value)
    return ems[0] if ems else None


def _split_labels(value) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    parts = [p.strip() for p in str(value).split(",")]
    return [p for p in parts if p]


def _connected_from_labels(labels: List[str]) -> bool:
    labs = [l.strip().lower() for l in labels]
    if any(l == "not connected" for l in labs):
        return False
    if any("connected" in l for l in labs):
        return True
    return False


def _norm_text(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val).strip().lower()


def _filter_credit_excluded(df: pd.DataFrame, text_col: Optional[str]) -> pd.DataFrame:
    if df.empty or not text_col or text_col not in df.columns:
        return df
    mask = df[text_col].apply(_norm_text).isin(CREDIT_EXCLUDE_DESCS)
    return df[~mask].copy()


def dedupe_mixpanel_export(df: pd.DataFrame) -> pd.DataFrame:
    # Exactly matches your notebook logic
    required = ["event", "distinct_id", "time", "$insert_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns: {missing}. Available: {list(df.columns)}")

    df = df.copy()
    t = pd.to_numeric(df["time"], errors="coerce")
    if t.notna().all():
        if float(t.median()) > 1e11:
            t = (t // 1000)
        df["_time_s"] = t.astype("Int64")
    else:
        dt = pd.to_datetime(df["time"], errors="coerce", utc=True)
        df["_time_s"] = (dt.view("int64") // 10**9).astype("Int64")

    sort_cols = ["_time_s"]
    if "mp_processing_time_ms" in df.columns:
        sort_cols = ["mp_processing_time_ms"] + sort_cols

    df = df.sort_values(sort_cols, kind="mergesort")
    df = df.drop_duplicates(subset=["event", "distinct_id", "_time_s", "$insert_id"], keep="last")
    return df.drop(columns=["_time_s"], errors="ignore")


def _expand_leads_for_multiple_emails(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[int]]:
    missing_rows: List[int] = []
    expanded = []
    priority_cols = [c for c in ["Person - Email", "Lead - User Email"] if c in df.columns]

    for i, row in df.iterrows():
        emails: List[str] = []
        for col in priority_cols:
            found = _extract_emails(row.get(col))
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


@st.cache_data(show_spinner=False, ttl=600)
def fetch_mixpanel_event_export(project_id: int, base_url: str, from_date: date, to_date: date, event_name: str) -> pd.DataFrame:
    """
    Fresh approach: we flatten *exactly* like your notebook:
    - Parse NDJSON lines -> DataFrame of top-level keys (includes event, distinct_id, time)
    - json_normalize(properties) -> DataFrame with $insert_id, $email, etc.
    - concat to get a single wide DataFrame
    """
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

    records = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not records:
        return pd.DataFrame()

    raw = pd.DataFrame(records)
    props = pd.json_normalize(raw["properties"]) if "properties" in raw.columns else pd.DataFrame()
    top = raw.drop(columns=["properties"], errors="ignore")
    df = pd.concat([top.reset_index(drop=True), props.reset_index(drop=True)], axis=1)

    if "time" in df.columns:
        df["_dt"] = _parse_time_to_dt(df["time"])

    return df


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
            columns=["email", "Total_Amount", "Total_Amount_creditExcluded", "First_Subscription", "First_Payment_Date"]
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
        ce_total = float(g_ce[(g_ce["_dt"] >= start) & (g_ce["_dt"] <= end)][amount_col].sum()) if g_ce is not None else 0.0

        out.append(
            {
                "email": email,
                "Total_Amount": gross_total,
                "Total_Amount_creditExcluded": ce_total,
                "First_Subscription": "TRUE" if trigger else "FALSE",
                "First_Payment_Date": start,
            }
        )

    df = pd.DataFrame(out)
    if not df.empty:
        df["First_Payment_Date"] = pd.to_datetime(df["First_Payment_Date"], errors="coerce", utc=True).dt.tz_convert(None)
    return df


def _nonzero_users_count(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    d = df[df["Total_Amount"].fillna(0).ne(0)].dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(columns=list(group_cols) + ["NonZero_Users"])
    return d.groupby(group_cols, as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})


def _add_totals_row(df: pd.DataFrame, money_cols: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    totals = {}
    for c in df.columns:
        if c in money_cols:
            totals[c] = float(pd.to_numeric(df[c], errors="coerce").fillna(0).sum())
        else:
            totals[c] = ""
    totals[df.columns[0]] = "TOTAL"
    return pd.concat([df, pd.DataFrame([totals])], ignore_index=True)



def _reorder_main_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Put the key computed metrics up front in a consistent order:
    email, Net_Amount, Net_Amount_creditExcluded, Total_Amount, Total_Amount_creditExcluded,
    Refund_Amount, Refund_Amount_creditExcluded, First_Subscription, First_Payment_Date, then the rest.
    """
    if df.empty:
        return df
    preferred = [
        "email",
        "Net_Amount",
        "Net_Amount_creditExcluded",
        "Total_Amount",
        "Total_Amount_creditExcluded",
        "Refund_Amount",
        "Refund_Amount_creditExcluded",
        "First_Subscription",
        "First_Payment_Date",
    ]
    cols = [c for c in preferred if c in df.columns]
    rest = [c for c in df.columns if c not in cols]
    return df[cols + rest]


def _write_df(ws, df: pd.DataFrame):
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    header_font = Font(bold=True)
    header_fill = PatternFill("solid", fgColor="EEEAFB")
    for cell in ws[1]:
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.fill = header_fill

    ws.freeze_panes = "A2"

    for col_idx, col in enumerate(df.columns, 1):
        maxlen = max([len(str(col))] + [len(str(v)) for v in df[col].astype(str).values[:200]])
        ws.column_dimensions[get_column_letter(col_idx)].width = min(45, max(10, maxlen + 2))


def _build_excel(
    leads_with_payments: pd.DataFrame,
    leads_nonzero: pd.DataFrame,
    email_summary: pd.DataFrame,
    owner_breakdown_including_pipedrive: pd.DataFrame,
    owner_x_connected: pd.DataFrame,
    label_breakdown: pd.DataFrame,
    connected_breakdown: pd.DataFrame,
    time_breakdown: pd.DataFrame,
    flags_df: pd.DataFrame,
    fig_owner,
    fig_owner_conn,
    fig_time,
    from_date: date,
    to_date: date,
    self_conv_block: pd.DataFrame,
) -> Tuple[str, bytes]:
    wb = Workbook()

    def add_sheet(title: str, df: pd.DataFrame):
        ws = wb.create_sheet(title)
        _write_df(ws, df)
        return ws

    ws0 = wb.active
    ws0.title = "Leads_with_Payments"
    _write_df(ws0, leads_with_payments)

    add_sheet("Leads_Payments_NonZero", leads_nonzero)
    add_sheet("Email_Summary", email_summary)

    ws_owner = add_sheet("Owner_Breakdown", owner_breakdown_including_pipedrive)

    # Self-converted / Sales attempt block
    start_row = ws_owner.max_row + 3
    ws_owner.cell(row=start_row, column=1, value="Self-Converted vs Sales Attempt").font = Font(bold=True)
    for r_idx, row in enumerate(dataframe_to_rows(self_conv_block, index=False, header=True), start_row + 1):
        for c_idx, value in enumerate(row, 1):
            ws_owner.cell(row=r_idx, column=c_idx, value=value)
            if r_idx == start_row + 1:
                ws_owner.cell(row=r_idx, column=c_idx).font = Font(bold=True)

    add_sheet("Owner_x_Connected", owner_x_connected)
    add_sheet("Label_Breakdown", label_breakdown)
    add_sheet("Connected_Breakdown", connected_breakdown)
    add_sheet("Time_Breakdown_Daily", time_breakdown)

    if flags_df.empty:
        ws_flags = wb.create_sheet("Flags_ConvertedYes_Zero")
        ws_flags.append(["No rows found where Person - Converted = Yes and Total_Amount = 0 (excluding Pipedrive KrispCall)"])
    else:
        add_sheet("Flags_ConvertedYes_Zero", flags_df)

    ws_chart = wb.create_sheet("Charts")

    def add_fig(ws, fig, anchor):
        img_bytes = BytesIO()
        fig.savefig(img_bytes, format="png", dpi=150, bbox_inches="tight")
        img_bytes.seek(0)
        img = XLImage(img_bytes)
        img.anchor = anchor
        ws.add_image(img)

    add_fig(ws_chart, fig_owner, "A1")
    add_fig(ws_chart, fig_owner_conn, "A25")
    add_fig(ws_chart, fig_time, "A49")

    out = BytesIO()
    wb.save(out)

    fname = f"payment_summary_{from_date.strftime('%b%d').lower()}_{to_date.strftime('%b%d').lower()}.xlsx"
    return fname, out.getvalue()


def main():
    st.set_page_config(page_title="KrispCall Payment Summary", page_icon="📈", layout="wide")
    require_login()
    _inject_brand_css()

    st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
    l, r = st.columns([1, 3], vertical_alignment="center")
    with l:
        st.markdown(_logo_html(width_px=240, top_pad_px=10), unsafe_allow_html=True)
    with r:
        st.markdown('<div class="kc-hero"><h1>KrispCall Payment Summary</h1><p>Fresh logic. No double-counting across duplicated leads rows.</p></div>', unsafe_allow_html=True)

    with st.sidebar:
        today = date.today()
        first_of_month = today.replace(day=1)
        default_start = first_of_month - timedelta(days=4)  # Jan -> Dec 28
        default_end = today - timedelta(days=1)
        from_date = st.date_input("Date from", value=default_start)
        to_date = st.date_input("Date to", value=default_end)
        exclude_owner_ui = st.text_input("Exclude owner from summaries", value="Pipedrive KrispCall")

        st.markdown("---")
        st.caption("Owner summaries are computed per UNIQUE email to prevent inflation when leads rows are duplicated.")

    st.markdown('<div class="kc-card">', unsafe_allow_html=True)
    leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])
    run = st.button("Run Analysis", type="primary", disabled=(leads_file is None))
    st.markdown("</div>", unsafe_allow_html=True)
    if not run:
        st.stop()

    logs: List[str] = []
    with st.spinner("Running analysis..."):
        leads_raw = pd.read_csv(leads_file)
        expanded_leads, missing_rows = _expand_leads_for_multiple_emails(leads_raw)
        if missing_rows:
            logs.append(f"Missing email for {len(missing_rows)} lead row(s). Example rows: {missing_rows[:15]}")

        # labels + connected on expanded leads (row-level)
        label_col = "Lead - Label" if "Lead - Label" in expanded_leads.columns else None
        expanded_leads["labels_list"] = expanded_leads[label_col].apply(_split_labels) if label_col else [[]] * len(expanded_leads)
        expanded_leads["Connected"] = expanded_leads["labels_list"].apply(_connected_from_labels)

        owner_col = "Lead - Owner" if "Lead - Owner" in expanded_leads.columns else "Owner"
        if owner_col not in expanded_leads.columns:
            expanded_leads[owner_col] = "Unknown"

        # ---- Create a single owner/labels mapping per email (prevents duplication in summaries) ----
        tmp = expanded_leads.copy()
        tmp["_created"] = pd.to_datetime(tmp.get("Lead - Lead created on"), errors="coerce")
        tmp["_conv_yes"] = tmp.get("Person - Converted", "").astype(str).str.strip().str.lower().eq("yes")
        tmp = tmp.sort_values(["_conv_yes", "_created"], ascending=[False, False], kind="mergesort")

        email_dim = (
            tmp.dropna(subset=["email"])
            .groupby("email", as_index=False)
            .first()[["email", owner_col, "Connected", "Lead - Label", "Person - Converted", "Lead - Lead created on"]]
        )
        email_dim = email_dim.rename(columns={owner_col: "Owner"})
        lead_emails = set(email_dim["email"].dropna().unique())

        # ---- Mixpanel Export ----
        pid = int(_get_secret(["mixpanel", "project_id"]))
        base = _get_secret(["mixpanel", "base_url"], "https://data-eu.mixpanel.com")

        payments_raw = fetch_mixpanel_event_export(pid, base, from_date, to_date, "New Payment Made")

        # Refund window rule
        window_days = (to_date - from_date).days
        refund_from = (to_date - timedelta(days=60)) if window_days < 60 else from_date
        refunds_raw = fetch_mixpanel_event_export(pid, base, refund_from, to_date, "Refund Granted")

        # Dedupe (your exact logic)
        payments = dedupe_mixpanel_export(payments_raw)
        refunds = dedupe_mixpanel_export(refunds_raw)

        logs.append(f"Payments raw: {len(payments_raw):,}. After dedupe: {len(payments):,}.")
        logs.append(f"Refunds raw: {len(refunds_raw):,}. After dedupe: {len(refunds):,}.")

        # normalize dt/email/amount/desc
        if "_dt" not in payments.columns and "time" in payments.columns:
            payments["_dt"] = _parse_time_to_dt(payments["time"])
        if "_dt" not in refunds.columns and "time" in refunds.columns:
            refunds["_dt"] = _parse_time_to_dt(refunds["time"])

        if "$email" not in payments.columns:
            raise KeyError("Payments export missing $email. Ensure you flatten properties as in this app.")
        if "User Email" not in refunds.columns:
            raise KeyError("Refund export missing User Email. Ensure you flatten properties as in this app.")

        payments["email"] = payments["$email"].apply(_normalize_email)
        refunds["email"] = refunds["User Email"].apply(_normalize_email)

        # Filter to leads emails
        payments = payments[payments["email"].isin(lead_emails)].copy()
        refunds = refunds[refunds["email"].isin(lead_emails)].copy()

        amount_col = "Amount" if "Amount" in payments.columns else None
        desc_col = "Amount Description" if "Amount Description" in payments.columns else None
        refund_amount_col = "Refund Amount" if "Refund Amount" in refunds.columns else None
        refund_desc_col = "Refunded Transaction Description" if "Refunded Transaction Description" in refunds.columns else None

        if not amount_col:
            raise RuntimeError("Could not find payment Amount column.")
        if not refund_amount_col:
            refunds["Refund Amount"] = 0.0
            refund_amount_col = "Refund Amount"

        payments[amount_col] = pd.to_numeric(payments[amount_col], errors="coerce").fillna(0.0)
        refunds[refund_amount_col] = pd.to_numeric(refunds[refund_amount_col], errors="coerce").fillna(0.0)

        # credit excluded versions
        payments_ce = _filter_credit_excluded(payments, desc_col)
        refunds_ce = _filter_credit_excluded(refunds, refund_desc_col)

        # per-email windowed payment sums (gross + creditExcluded)
        pay_summary = _windowed_payments_dual(payments, payments_ce, amount_col, desc_col, days=7)

        # refunds are not windowed (they're already exported for the appropriate window)
        ref_summary = refunds.dropna(subset=["email"]).groupby("email", as_index=False)[refund_amount_col].sum().rename(columns={refund_amount_col: "Refund_Amount"})
        ref_summary_ce = refunds_ce.dropna(subset=["email"]).groupby("email", as_index=False)[refund_amount_col].sum().rename(columns={refund_amount_col: "Refund_Amount_creditExcluded"})

        summary = pay_summary.merge(ref_summary, on="email", how="left").merge(ref_summary_ce, on="email", how="left").fillna(0.0)
        summary["Net_Amount"] = summary["Total_Amount"] - summary["Refund_Amount"]
        summary["Net_Amount_creditExcluded"] = summary["Total_Amount_creditExcluded"] - summary["Refund_Amount_creditExcluded"]

        # ---- Main tables (preserve lead rows) ----
        joined = expanded_leads.merge(
            summary,
            on="email",
            how="left",
        )
        money_cols = [
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
        ]
        for c in money_cols:
            joined[c] = pd.to_numeric(joined.get(c, 0.0), errors="coerce").fillna(0.0)
        joined["First_Subscription"] = joined.get("First_Subscription", "FALSE").fillna("FALSE")

        joined_nonzero = joined[joined["Total_Amount"].fillna(0).ne(0)].copy()

        # ---- Summaries computed per UNIQUE email (prevents inflation) ----
        email_fact = summary.merge(email_dim[["email", "Owner", "Connected", "Lead - Label"]], on="email", how="left")
        email_fact["Owner"] = email_fact["Owner"].fillna("Unknown")

        exclude_owner_val = str(exclude_owner_ui).strip().lower()
        excl_mask = email_fact["Owner"].astype(str).str.strip().str.lower().eq(exclude_owner_val)
        email_fact_excl = email_fact[~excl_mask].copy()

        # email summary table (excluded owner removed)
        email_summary = email_fact_excl[["email", "First_Subscription", "First_Payment_Date"] + money_cols].copy()
        email_summary = email_summary.sort_values("Net_Amount", ascending=False)

        # owner breakdown (INCLUDE pipedrive here only)
        owner_breakdown_incl = (
            email_fact.groupby("Owner", as_index=False)[money_cols].sum()
            .merge(_nonzero_users_count(email_fact.rename(columns={"Owner": "Owner"}), ["Owner"]), on="Owner", how="left")
            .fillna({"NonZero_Users": 0})
        )
        owner_breakdown_incl = owner_breakdown_incl[["Owner", "Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded", "NonZero_Users"]]
        owner_breakdown_incl = owner_breakdown_incl.rename(columns={"Owner": "Lead - Owner"}).sort_values("Net_Amount", ascending=False)

        # owner x connected (exclude pipedrive)
        owner_x_connected = (
            email_fact_excl.groupby(["Owner", "Connected"], as_index=False)[money_cols].sum()
            .merge(_nonzero_users_count(email_fact_excl.rename(columns={"Owner": "Owner"}), ["Owner", "Connected"]), on=["Owner", "Connected"], how="left")
            .fillna({"NonZero_Users": 0})
        )
        owner_x_connected = owner_x_connected.rename(columns={"Owner": "Lead - Owner"})
        owner_x_connected = owner_x_connected[["Lead - Owner", "Connected", "Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded", "NonZero_Users"]]
        owner_x_connected = owner_x_connected.sort_values("Net_Amount", ascending=False)

        # connected breakdown (exclude pipedrive)
        connected_breakdown = (
            email_fact_excl.groupby("Connected", as_index=False)[money_cols].sum()
            .merge(_nonzero_users_count(email_fact_excl, ["Connected"]), on="Connected", how="left")
            .fillna({"NonZero_Users": 0})
        )
        connected_breakdown = connected_breakdown[["Connected", "Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded", "NonZero_Users"]]

        # label breakdown (exclude pipedrive) computed on unique email-label pairs
        lbl = email_fact_excl[["email", "Lead - Label"] + money_cols].copy()
        lbl["labels_list"] = lbl["Lead - Label"].apply(_split_labels)
        lbl = lbl.explode("labels_list").rename(columns={"labels_list": "Label"})
        lbl["Label"] = lbl["Label"].fillna("").astype(str).str.strip()
        lbl = lbl[lbl["Label"] != ""].copy()
        lbl["Connected_Label"] = lbl["Label"].str.lower().apply(lambda s: False if s.strip() == "not connected" else ("connected" in s))
        lbl = lbl.drop_duplicates(subset=["email", "Label", "Connected_Label"])

        label_breakdown = lbl.groupby(["Label", "Connected_Label"], as_index=False)[money_cols].sum()
        label_counts = lbl[lbl["Total_Amount"].fillna(0).ne(0)].groupby(["Label", "Connected_Label"], as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})
        label_breakdown = label_breakdown.merge(label_counts, on=["Label", "Connected_Label"], how="left").fillna({"NonZero_Users": 0})
        label_breakdown = label_breakdown[["Label", "Connected_Label", "Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded", "NonZero_Users"]].sort_values("Net_Amount", ascending=False)

        # time breakdown (daily) from exports, excluding pipedrive via emails in email_fact_excl
        emails_scope = set(email_fact_excl["email"].dropna().unique())
        p_sc = payments[payments["email"].isin(emails_scope)].copy()
        r_sc = refunds[refunds["email"].isin(emails_scope)].copy()
        p_sc["date"] = p_sc["_dt"].dt.date
        r_sc["date"] = r_sc["_dt"].dt.date

        payments_daily = p_sc.groupby("date", as_index=False).agg(Payments_Amount=(amount_col, "sum"))
        refunds_daily = r_sc.groupby("date", as_index=False).agg(Refunds_Amount=(refund_amount_col, "sum"))

        pce_sc = payments_ce[payments_ce["email"].isin(emails_scope)].copy()
        rce_sc = refunds_ce[refunds_ce["email"].isin(emails_scope)].copy()
        if not pce_sc.empty:
            pce_sc["date"] = pce_sc["_dt"].dt.date
        if not rce_sc.empty:
            rce_sc["date"] = rce_sc["_dt"].dt.date

        payments_ce_daily = pce_sc.groupby("date", as_index=False).agg(Payments_Amount_creditExcluded=(amount_col, "sum")) if not pce_sc.empty else pd.DataFrame(columns=["date", "Payments_Amount_creditExcluded"])
        refunds_ce_daily = rce_sc.groupby("date", as_index=False).agg(Refunds_Amount_creditExcluded=(refund_amount_col, "sum")) if not rce_sc.empty else pd.DataFrame(columns=["date", "Refunds_Amount_creditExcluded"])

        time_breakdown = (
            payments_daily.merge(refunds_daily, on="date", how="outer")
            .merge(payments_ce_daily, on="date", how="outer")
            .merge(refunds_ce_daily, on="date", how="outer")
            .fillna(0)
        )
        time_breakdown["Net_Amount"] = time_breakdown["Payments_Amount"] - time_breakdown["Refunds_Amount"]
        time_breakdown["Net_Amount_creditExcluded"] = time_breakdown["Payments_Amount_creditExcluded"] - time_breakdown["Refunds_Amount_creditExcluded"]
        time_breakdown = time_breakdown[["date", "Net_Amount", "Net_Amount_creditExcluded", "Payments_Amount", "Payments_Amount_creditExcluded", "Refunds_Amount", "Refunds_Amount_creditExcluded"]].sort_values("date")

        # flags (converted yes but total=0) from lead rows excluding pipedrive via owner at row-level
        flags_df = pd.DataFrame()
        if "Person - Converted" in joined.columns:
            m_yes = joined["Person - Converted"].astype(str).str.strip().str.lower().eq("yes")
            # exclude owner rows
            row_owner = joined.get(owner_col, "Unknown").astype(str).str.strip().str.lower()
            m_excl = row_owner.eq(exclude_owner_val)
            m_zero = joined["Total_Amount"].fillna(0).eq(0)
            flags_df = joined[m_yes & ~m_excl & m_zero].copy()
            if not flags_df.empty:
                flags_df.insert(0, "Row_Number", flags_df.index + 2)

        # ---- Totals rows + ordering (Net first) ----
        def totals_ready(df: pd.DataFrame) -> pd.DataFrame:
            return _add_totals_row(df, money_cols + ["Payments_Amount", "Payments_Amount_creditExcluded", "Refunds_Amount", "Refunds_Amount_creditExcluded"])

        joined_export = joined.copy()
        joined_export["labels_list"] = joined_export["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        joined_nonzero_export = joined_nonzero.copy()
        joined_nonzero_export["labels_list"] = joined_nonzero_export["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        joined_export = _reorder_main_columns(joined_export)
        joined_nonzero_export = _reorder_main_columns(joined_nonzero_export)

        joined_export = totals_ready(joined_export)
        joined_nonzero_export = totals_ready(joined_nonzero_export)
        email_summary = _reorder_main_columns(email_summary)
        email_summary = totals_ready(email_summary)
        owner_breakdown_incl = totals_ready(owner_breakdown_incl)
        owner_x_connected = totals_ready(owner_x_connected)
        connected_breakdown = totals_ready(connected_breakdown)
        label_breakdown = totals_ready(label_breakdown)
        time_breakdown = totals_ready(time_breakdown)

        # Self-converted vs sales attempt block (from pipedrive owner)
        pipe_mask = email_fact["Owner"].astype(str).str.strip().str.lower().eq(EXCLUDED_OWNER_CANON)
        pipe = email_fact[pipe_mask]
        other = email_fact[~pipe_mask]
        self_conv_block = pd.DataFrame(
            [
                {
                    "Type": "Self-Converted Revenue (Pipedrive KrispCall)",
                    "Net_Amount": float(pipe["Net_Amount"].sum()),
                    "Net_Amount_creditExcluded": float(pipe["Net_Amount_creditExcluded"].sum()),
                },
                {
                    "Type": "Sales Attempt Revenue (All other owners)",
                    "Net_Amount": float(other["Net_Amount"].sum()),
                    "Net_Amount_creditExcluded": float(other["Net_Amount_creditExcluded"].sum()),
                },
            ]
        )

        # Charts
        fig_owner, ax_owner = plt.subplots(figsize=(10, 6))
        ob = owner_breakdown_incl.copy()
        ob = ob[ob["Lead - Owner"] != "TOTAL"].copy()
        if not ob.empty:
            ob = ob.set_index("Lead - Owner")[["Net_Amount", "Net_Amount_creditExcluded"]]
            ob.plot(kind="bar", ax=ax_owner)
        ax_owner.set_title("Owner Summary (includes Pipedrive KrispCall)")
        plt.tight_layout()

        fig_owner_conn, ax_owner_conn = plt.subplots(figsize=(10, 6))
        oc = owner_x_connected.copy()
        oc = oc[oc["Lead - Owner"] != "TOTAL"].copy()
        if not oc.empty:
            pivot = oc.pivot_table(index="Lead - Owner", columns="Connected", values="Net_Amount", aggfunc="sum").fillna(0.0)
            pivot = pivot.rename(columns={False: "FALSE", True: "TRUE"})
            pivot.plot(kind="bar", stacked=True, ax=ax_owner_conn)
        ax_owner_conn.set_title(f"Net Revenue by Owner and Connected (excl. {exclude_owner_ui})")
        plt.tight_layout()

        fig_time, ax_time = plt.subplots(figsize=(10, 5))
        tb = time_breakdown.copy()
        tb = tb[tb["date"] != "TOTAL"].copy()
        if not tb.empty:
            ax_time.plot(pd.to_datetime(tb["date"]), tb["Net_Amount"])
        ax_time.set_title(f"Net Amount by Day (excl. {exclude_owner_ui})")
        plt.tight_layout()

        fname, excel_bytes = _build_excel(
            leads_with_payments=joined_export,
            leads_nonzero=joined_nonzero_export,
            email_summary=email_summary,
            owner_breakdown_including_pipedrive=owner_breakdown_incl,
            owner_x_connected=owner_x_connected,
            label_breakdown=label_breakdown,
            connected_breakdown=connected_breakdown,
            time_breakdown=time_breakdown,
            flags_df=flags_df,
            fig_owner=fig_owner,
            fig_owner_conn=fig_owner_conn,
            fig_time=fig_time,
            from_date=from_date,
            to_date=to_date,
            self_conv_block=self_conv_block,
        )

    # UI
    tab_overview, tab_main, tab_summaries, tab_time, tab_export, tab_logs = st.tabs(
        ["Overview", "Main Tables", "Summaries", "Time", "Exports", "Logs"]
    )

    with tab_overview:
        c1, c2, c3 = st.columns(3)
        # Use email_fact_excl totals to match "excluded owner removed" for KPIs
        c1.metric("Net Revenue (excl. owner)", f"{email_fact_excl['Net_Amount'].sum():,.2f}")
        c2.metric("Net Revenue creditExcluded (excl. owner)", f"{email_fact_excl['Net_Amount_creditExcluded'].sum():,.2f}")
        c3.metric("Non-zero users (emails)", f"{int((email_fact_excl['Total_Amount'].fillna(0).ne(0)).sum()):,}")
        st.pyplot(fig_owner, use_container_width=True)

    with tab_main:
        st.markdown("#### Leads with payments (row-preserving)")
        st.dataframe(joined_export, use_container_width=True)
        st.markdown("#### Leads with non-zero payments only")
        st.dataframe(joined_nonzero_export, use_container_width=True)

    with tab_summaries:
        st.markdown("#### Owner summary (includes Pipedrive KrispCall)")
        st.dataframe(owner_breakdown_incl, use_container_width=True)
        st.markdown("#### Owner x Connected (excluded owner removed)")
        st.dataframe(owner_x_connected, use_container_width=True)
        st.pyplot(fig_owner_conn, use_container_width=True)
        st.markdown("#### Connected breakdown")
        st.dataframe(connected_breakdown, use_container_width=True)
        st.markdown("#### Label breakdown")
        st.dataframe(label_breakdown, use_container_width=True)
        st.markdown("#### Email summary (excluded owner removed)")
        st.dataframe(email_summary, use_container_width=True)

    with tab_time:
        st.markdown("#### Daily breakdown (excluded owner removed)")
        st.dataframe(time_breakdown, use_container_width=True)
        st.pyplot(fig_time, use_container_width=True)

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
