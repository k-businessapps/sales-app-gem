import base64
import json
import re
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.utils.dataframe import dataframe_to_rows

EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PRIMARY = "#B04EF0"
KC_ACCENT = "#E060F0"
KC_DEEP = "#8030F0"
KC_SOFT = "#F6F0FF"

EXCLUDED_OWNER_CANON = "pipedrive krispcall"

CREDIT_EXCLUDE_DESCS = {"purchased credit", "credit purchased", "amount recharged"}


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
    """Remove credit-related rows by exact (case-insensitive) match on the provided text column."""
    if df.empty or not text_col or text_col not in df.columns:
        return df
    mask = df[text_col].apply(_norm_text).isin(CREDIT_EXCLUDE_DESCS)
    return df[~mask].copy()



def _get_secret(path: List[str], default=None):
    cur = st.secrets
    for key in path:
        if key not in cur:
            return default
        cur = cur[key]
    return cur


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

    # Hard fail if secrets missing so user knows what to set
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


def _mixpanel_headers() -> Dict[str, str]:
    auth = _get_secret(["mixpanel", "authorization"])
    if not auth:
        raise RuntimeError("Missing mixpanel.authorization in Streamlit secrets.")
    return {"accept": "text/plain", "authorization": str(auth).strip()}


def _parse_time_to_dt(series: pd.Series) -> pd.Series:
    t = pd.to_numeric(series, errors="coerce")
    if t.dropna().empty:
        return pd.to_datetime(series, errors="coerce", utc=True)
    if float(t.median()) > 1e11:  # ms
        t = (t // 1000)
    return pd.to_datetime(t, unit="s", utc=True)


def _dedupe_mixpanel_export(df: pd.DataFrame) -> pd.DataFrame:
    # Original approach: based on insert_id + second-resolution time + distinct_id + event
    need = ["event", "distinct_id", "time", "$insert_id"]
    if df.empty or any(c not in df.columns for c in need):
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
            rows.append(json.loads(line).get("properties", {}))
        except json.JSONDecodeError:
            continue
    df = pd.DataFrame(rows)
    if not df.empty and "time" in df.columns:
        df["_dt"] = _parse_time_to_dt(df["time"])
    return df


def _pick_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    # Case-insensitive match first, then exact
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _resolve_owner_column(df: pd.DataFrame) -> str:
    return _pick_first_existing_column(df, ["Lead - Owner", "Deal - Owner", "Owner", "owner"]) or "Owner"


def _expand_leads_for_multiple_emails(df: pd.DataFrame, email_cols: List[str]) -> Tuple[pd.DataFrame, List[int]]:
    """
    For each row, resolve emails from the first non-empty email column (in priority order).
    If that cell contains multiple emails, duplicate the row for each email.
    Returns expanded_df and list of original row numbers (1-based data rows, including header offset) with missing email.
    """
    missing_rows: List[int] = []
    expanded = []

    for i, row in df.iterrows():
        emails: List[str] = []
        for col in email_cols:
            if col in df.columns:
                found = _extract_emails(row[col])
                if found:
                    emails = found
                    break

        if not emails:
            # keep row, but email is None
            rec = row.to_dict()
            rec["email"] = None
            expanded.append(rec)
            # Excel-like row number (header=1, first data row=2)
            missing_rows.append(i + 2)
            continue

        for e in emails:
            rec = row.to_dict()
            rec["email"] = e
            expanded.append(rec)

    return pd.DataFrame(expanded), missing_rows


def _windowed_payments(payments: pd.DataFrame, amount_col: str, desc_col: Optional[str], days: int = 7) -> pd.DataFrame:
    """
    7-day window per email.
    Start date is earliest "Workspace Subscription" event if present, else earliest payment event.
    Adds First_Subscription TRUE/FALSE and First_Payment_Date.
    """
    d = payments.dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["email", "Total_Amount", "First_Subscription", "First_Payment_Date"])

    d[amount_col] = pd.to_numeric(d[amount_col], errors="coerce").fillna(0.0)

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
        total = float(g[(g["_dt"] >= start) & (g["_dt"] <= end)][amount_col].sum())
        out.append(
            {
                "email": email,
                "Total_Amount": total,
                "First_Subscription": "TRUE" if trigger else "FALSE",
                "First_Payment_Date": start,
            }
        )
    df = pd.DataFrame(out)
    if not df.empty:
        df["First_Payment_Date"] = pd.to_datetime(df["First_Payment_Date"], errors="coerce", utc=True).dt.tz_convert(None)
    return df



def _windowed_payments_dual(
    payments_gross: pd.DataFrame,
    payments_credit_excluded: pd.DataFrame,
    amount_col: str,
    desc_col: Optional[str],
    days: int = 7,
) -> pd.DataFrame:
    """
    Same 7-day window definition as gross:
    - Start date is earliest "Workspace Subscription" if present (in gross), else earliest payment event (in gross).
    - Total_Amount computed from gross payments inside window.
    - Total_Amount_creditExcluded computed from credit-excluded payments inside the SAME window.
    """
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


def _build_excel(
    leads_with_payments: pd.DataFrame,
    leads_nonzero: pd.DataFrame,
    email_summary: pd.DataFrame,
    owner_breakdown: pd.DataFrame,
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
) -> Tuple[str, bytes]:
    wb = Workbook()

    def add_sheet(title: str, df: pd.DataFrame):
        ws = wb.create_sheet(title)
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)
        return ws

    ws0 = wb.active
    ws0.title = "Leads_with_Payments"
    for r in dataframe_to_rows(leads_with_payments, index=False, header=True):
        ws0.append(r)

    add_sheet("Leads_Payments_NonZero", leads_nonzero)
    add_sheet("Email_Summary", email_summary)
    add_sheet("Owner_Breakdown", owner_breakdown)
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

    def add_fig(ws, fig, anchor, rounded_df: pd.DataFrame, rounded_anchor_col, rounded_anchor_row):
        img_bytes = BytesIO()
        fig.savefig(img_bytes, format="png", dpi=150, bbox_inches="tight")
        img_bytes.seek(0)
        img = XLImage(img_bytes)
        img.anchor = anchor
        ws.add_image(img)

        for r_idx, row in enumerate(dataframe_to_rows(rounded_df, index=False, header=True), rounded_anchor_row):
            for c_idx, value in enumerate(row, rounded_anchor_col):
                ws.cell(row=r_idx, column=c_idx, value=value)

    owner_round = owner_breakdown.copy()
    for c in ["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]:
        if c in owner_round.columns:
            owner_round[c] = pd.to_numeric(owner_round[c], errors="coerce").fillna(0).round(0).astype(int)

    owner_conn_round = owner_x_connected.copy()
    for c in ["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]:
        if c in owner_conn_round.columns:
            owner_conn_round[c] = pd.to_numeric(owner_conn_round[c], errors="coerce").fillna(0).round(0).astype(int)

    time_round = time_breakdown.copy()
    for c in ["Payments_Amount", "Refunds_Amount", "Net_Amount", "Payments_Amount_creditExcluded", "Refunds_Amount_creditExcluded", "Net_Amount_creditExcluded"]:
        if c in time_round.columns:
            time_round[c] = pd.to_numeric(time_round[c], errors="coerce").fillna(0).round(0).astype(int)

    add_fig(ws_chart, fig_owner, "A1", owner_round, 8, 1)
    add_fig(ws_chart, fig_owner_conn, "A25", owner_conn_round, 8, 25)
    add_fig(ws_chart, fig_time, "A49", time_round, 8, 49)

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
        st.markdown('<div class="kc-hero"><h1>KrispCall Payment Summary</h1><p>Leads reconciliation with Mixpanel transactions</p></div>', unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Date Selection")
        today = date.today()
        first_of_month = today.replace(day=1)
        # Based on your example: Jan -> Dec 28
        default_start = first_of_month - timedelta(days=4)
        default_end = today - timedelta(days=1)

        from_date = st.date_input("Date from", value=default_start)
        to_date = st.date_input("Date to", value=default_end)

        st.markdown("---")
        st.markdown("### Settings")
        exclude_owner_ui = st.text_input("Exclude owner from summaries", value="Pipedrive KrispCall")
        st.caption("This exclusion applies to ALL summary tables and charts. Main tables still include it.")

    st.markdown('<div class="kc-card">', unsafe_allow_html=True)
    leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])
    run = st.button("Run Analysis", type="primary", disabled=(leads_file is None))
    st.markdown("</div>", unsafe_allow_html=True)

    if not run:
        st.stop()

    logs: List[str] = []
    with st.spinner("Running analysis..."):
        leads_raw = pd.read_csv(leads_file)

        # Priority order for leads emails (two variables, as requested)
        email_cols_priority = []
        # Use exact column names if present; otherwise try close matches
        for cand in ["Person - Email", "Lead - User Email"]:
            col = _pick_first_existing_column(leads_raw, [cand])
            if col and col not in email_cols_priority:
                email_cols_priority.append(col)

        expanded_leads, missing_rows = _expand_leads_for_multiple_emails(leads_raw, email_cols_priority)
        if missing_rows:
            logs.append(f"Missing email for {len(missing_rows)} row(s). Example rows: {missing_rows[:15]}")

        # Labels and Connected
        label_col = _pick_first_existing_column(expanded_leads, ["Lead - Label", "Label", "Labels"])
        if label_col:
            expanded_leads["labels_list"] = expanded_leads[label_col].apply(_split_labels)
        else:
            expanded_leads["labels_list"] = [[]] * len(expanded_leads)

        expanded_leads["Connected"] = expanded_leads["labels_list"].apply(_connected_from_labels)

        # Resolve owner column (kept in main tables)
        owner_col = _resolve_owner_column(expanded_leads)

        # Mixpanel export
        pid = int(_get_secret(["mixpanel", "project_id"]))
        base = _get_secret(["mixpanel", "base_url"], "https://data-eu.mixpanel.com")

        payments_raw = fetch_mixpanel_event_export(pid, base, from_date, to_date, "New Payment Made")

        # Refund window rule
        window_days = (to_date - from_date).days
        refund_from = (to_date - timedelta(days=60)) if window_days < 60 else from_date
        refunds_raw = fetch_mixpanel_event_export(pid, base, refund_from, to_date, "Refund Granted")

        payments = _dedupe_mixpanel_export(payments_raw)
        refunds = _dedupe_mixpanel_export(refunds_raw)

        # Email columns for exports
        pay_email_col = _pick_first_existing_column(payments, ["$email", "email", "Email", "EMAIL"])
        ref_email_col = _pick_first_existing_column(refunds, ["User Email", "user.email", "email", "Email", "EMAIL"])

        payments["email"] = payments[pay_email_col].apply(_normalize_email) if pay_email_col and pay_email_col in payments.columns else None
        refunds["email"] = refunds[ref_email_col].apply(_normalize_email) if ref_email_col and ref_email_col in refunds.columns else None

        amount_col = _pick_first_existing_column(payments, ["Amount", "amount", "Amount Paid"])
        desc_col = _pick_first_existing_column(payments, ["Amount Description", "description", "Plan"])
        refund_amount_col = _pick_first_existing_column(refunds, ["Refund Amount", "refund_amount", "Amount"])
        refund_desc_col = _pick_first_existing_column(refunds, ["Refunded Transaction description", "Refunded Transaction Description", "Refunded Transaction", "Refunded transaction description"])

        if not amount_col:
            raise RuntimeError("Could not find payment amount column in Mixpanel export (expected 'Amount').")
        if not refund_amount_col:
            # allow empty refunds
            refunds["Refund Amount"] = 0.0
            refund_amount_col = "Refund Amount"

        payments[amount_col] = pd.to_numeric(payments[amount_col], errors="coerce").fillna(0.0)
        refunds[refund_amount_col] = pd.to_numeric(refunds[refund_amount_col], errors="coerce").fillna(0.0)

        # Email set from leads for efficiency
        lead_emails = set(expanded_leads["email"].dropna().unique())
        payments = payments[payments["email"].isin(lead_emails)].copy()
        refunds = refunds[refunds["email"].isin(lead_emails)].copy()

        payments_ce = _filter_credit_excluded(payments, desc_col)
        refunds_ce = _filter_credit_excluded(refunds, refund_desc_col)

        # Per-email summaries
        pay_summary = _windowed_payments_dual(payments, payments_ce, amount_col, desc_col, days=7)
        ref_summary = (
            refunds.dropna(subset=["email"])
            .groupby("email", as_index=False)[refund_amount_col]
            .sum()
            .rename(columns={refund_amount_col: "Refund_Amount"})
        )

        ref_summary_ce = (
            refunds_ce.dropna(subset=["email"])
            .groupby("email", as_index=False)[refund_amount_col]
            .sum()
            .rename(columns={refund_amount_col: "Refund_Amount_creditExcluded"})
        )

        summary = pay_summary.merge(ref_summary, on="email", how="outer").merge(ref_summary_ce, on="email", how="outer")
        summary["Total_Amount"] = pd.to_numeric(summary.get("Total_Amount", 0.0), errors="coerce").fillna(0.0)
        summary["Refund_Amount"] = pd.to_numeric(summary.get("Refund_Amount", 0.0), errors="coerce").fillna(0.0)
        summary["Total_Amount_creditExcluded"] = pd.to_numeric(summary.get("Total_Amount_creditExcluded", 0.0), errors="coerce").fillna(0.0)
        summary["Refund_Amount_creditExcluded"] = pd.to_numeric(summary.get("Refund_Amount_creditExcluded", 0.0), errors="coerce").fillna(0.0)
        summary["First_Subscription"] = summary.get("First_Subscription", "FALSE").fillna("FALSE")
        summary["Net_Amount"] = summary["Total_Amount"] - summary["Refund_Amount"]
        summary["Net_Amount_creditExcluded"] = summary["Total_Amount_creditExcluded"] - summary["Refund_Amount_creditExcluded"]

        # Join back to leads (preserve all)
        joined = expanded_leads.merge(
            summary[["email", "Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded", "First_Subscription", "First_Payment_Date"]],
            on="email",
            how="left",
        )
        for c in ["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]:
            joined[c] = pd.to_numeric(joined[c], errors="coerce").fillna(0.0)
        joined["First_Subscription"] = joined["First_Subscription"].fillna("FALSE")

        # Non-zero table
        joined_nonzero = joined[joined["Total_Amount"].fillna(0).ne(0)].copy()

        # Exclusion for ALL summaries
        exclude_owner_val = str(exclude_owner_ui).strip().lower()
        exclude_mask = joined[owner_col].astype(str).str.strip().str.lower().eq(exclude_owner_val)
        agg_base = joined[~exclude_mask].copy()
        emails_in_agg = set(agg_base["email"].dropna().unique())

        # Filter email summary
        email_summary = summary[summary["email"].isin(emails_in_agg)].sort_values("Net_Amount", ascending=False).copy()

        # Owner breakdown with NonZero_Users
        owner_breakdown = (
            agg_base.groupby(owner_col, as_index=False)[["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]]
            .sum()
            .rename(columns={owner_col: "Lead - Owner"})
        )
        owner_counts = _nonzero_users_count(agg_base.rename(columns={owner_col: "Lead - Owner"}), ["Lead - Owner"])
        owner_breakdown = owner_breakdown.merge(owner_counts, on="Lead - Owner", how="left").fillna({"NonZero_Users": 0})
        owner_breakdown = owner_breakdown.sort_values("Net_Amount", ascending=False)

        # Connected breakdown with NonZero_Users
        connected_breakdown = agg_base.groupby("Connected", as_index=False)[["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]].sum()
        conn_counts = _nonzero_users_count(agg_base, ["Connected"])
        connected_breakdown = connected_breakdown.merge(conn_counts, on="Connected", how="left").fillna({"NonZero_Users": 0})
        connected_breakdown = connected_breakdown.sort_values("Net_Amount", ascending=False)

        # Owner x Connected with NonZero_Users
        owner_x_connected = (
            agg_base.groupby([owner_col, "Connected"], as_index=False)[["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]]
            .sum()
            .rename(columns={owner_col: "Lead - Owner"})
        )
        owner_conn_counts = _nonzero_users_count(agg_base.rename(columns={owner_col: "Lead - Owner"}), ["Lead - Owner", "Connected"])
        owner_x_connected = owner_x_connected.merge(owner_conn_counts, on=["Lead - Owner", "Connected"], how="left").fillna({"NonZero_Users": 0})
        owner_x_connected = owner_x_connected.sort_values("Net_Amount", ascending=False)

        # Label breakdown with counts
        labels_expanded = agg_base[["email", "Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded", "labels_list"]].copy()
        labels_expanded = labels_expanded.explode("labels_list").rename(columns={"labels_list": "Label"})
        labels_expanded["Label"] = labels_expanded["Label"].fillna("").astype(str).str.strip()
        labels_expanded = labels_expanded[labels_expanded["Label"] != ""].copy()
        labels_expanded["Connected_Label"] = labels_expanded["Label"].str.lower().apply(lambda s: False if s.strip() == "not connected" else ("connected" in s))

        label_breakdown = labels_expanded.groupby(["Label", "Connected_Label"], as_index=False)[["Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded"]].sum()
        label_counts = (
            labels_expanded[labels_expanded["Total_Amount"].fillna(0).ne(0)]
            .dropna(subset=["email"])
            .groupby(["Label", "Connected_Label"], as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "NonZero_Users"})
        )
        label_breakdown = label_breakdown.merge(label_counts, on=["Label", "Connected_Label"], how="left").fillna({"NonZero_Users": 0})
        label_breakdown = label_breakdown.sort_values("Net_Amount", ascending=False)

        # Time breakdown (daily), excluding owner (filter by emails_in_agg)
        payments_agg = payments[payments["email"].isin(emails_in_agg)].copy()
        refunds_agg = refunds[refunds["email"].isin(emails_in_agg)].copy()

        payments_agg["date"] = payments_agg["_dt"].dt.date
        refunds_agg["date"] = refunds_agg["_dt"].dt.date

        payments_daily = (
            payments_agg.groupby("date", as_index=False)
            .agg(
                Payments_Amount=(amount_col, "sum"),
                Payments_NonZero_Users=("email", lambda s: s[payments_agg.loc[s.index, amount_col].fillna(0).ne(0)].nunique()),
            )
        )
        refunds_daily = (
            refunds_agg.groupby("date", as_index=False)
            .agg(
                Refunds_Amount=(refund_amount_col, "sum"),
                Refunds_NonZero_Users=("email", lambda s: s[refunds_agg.loc[s.index, refund_amount_col].fillna(0).ne(0)].nunique()),
            )
        )
        # Ensure credit-excluded frames also have a 'date' column before grouping
        if not payments_ce.empty and "_dt" in payments_ce.columns:
            payments_ce["date"] = payments_ce["_dt"].dt.date
        if not refunds_ce.empty and "_dt" in refunds_ce.columns:
            refunds_ce["date"] = refunds_ce["_dt"].dt.date

        payments_ce_daily = (
            payments_ce.groupby("date", as_index=False).agg(Payments_Amount_creditExcluded=(amount_col, "sum"))
            if (not payments_ce.empty and "date" in payments_ce.columns)
            else pd.DataFrame(columns=["date", "Payments_Amount_creditExcluded"])
        )
        refunds_ce_daily = (
            refunds_ce.groupby("date", as_index=False).agg(Refunds_Amount_creditExcluded=(refund_amount_col, "sum"))
            if (not refunds_ce.empty and "date" in refunds_ce.columns)
            else pd.DataFrame(columns=["date", "Refunds_Amount_creditExcluded"])
        )

        time_breakdown = (
            payments_daily.merge(refunds_daily, on="date", how="outer")
            .merge(payments_ce_daily, on="date", how="outer")
            .merge(refunds_ce_daily, on="date", how="outer")
            .fillna(0)
        )
        time_breakdown["Net_Amount"] = time_breakdown["Payments_Amount"] - time_breakdown["Refunds_Amount"]
        time_breakdown["Net_Amount_creditExcluded"] = (
            time_breakdown["Payments_Amount_creditExcluded"] - time_breakdown["Refunds_Amount_creditExcluded"]
        )
        time_breakdown["NonZero_Users"] = time_breakdown["Payments_NonZero_Users"]
        time_breakdown = time_breakdown.sort_values("date")

        # Flags: converted yes but total=0 (excluding owner)
        flags_df = pd.DataFrame()
        conv_col = _pick_first_existing_column(agg_base, ["Person - Converted", "Converted"])
        if conv_col:
            mask_yes = agg_base[conv_col].astype(str).str.strip().str.lower().eq("yes")
            mask_zero = agg_base["Total_Amount"].fillna(0).eq(0)
            flags_df = agg_base[mask_yes & mask_zero].copy()
            if not flags_df.empty:
                flags_df.insert(0, "Row_Number", flags_df.index + 2)

        # Charts (matplotlib default colors)
        fig_owner, ax_owner = plt.subplots(figsize=(10, 6))
        if not owner_breakdown.empty:
            owner_breakdown.set_index("Lead - Owner")[["Total_Amount", "Refund_Amount", "Net_Amount"]].plot(kind="bar", ax=ax_owner)
        ax_owner.set_title(f"Revenue by Lead Owner (excl. {exclude_owner_ui})")
        ax_owner.set_xlabel("Lead - Owner")
        ax_owner.set_ylabel("Amount")
        for container in ax_owner.containers:
            ax_owner.bar_label(container, labels=[str(int(round(v))) for v in container.datavalues], padding=2, fontsize=8)
        plt.tight_layout()

        fig_owner_conn, ax_owner_conn = plt.subplots(figsize=(10, 6))
        if not owner_x_connected.empty:
            pivot = owner_x_connected.pivot_table(index="Lead - Owner", columns="Connected", values="Net_Amount", aggfunc="sum").fillna(0.0)
            pivot = pivot.rename(columns={False: "FALSE", True: "TRUE"})
            cols = [c for c in ["FALSE", "TRUE"] if c in pivot.columns]
            if cols:
                pivot = pivot.loc[:, cols]
            pivot.plot(kind="bar", stacked=True, ax=ax_owner_conn)
        ax_owner_conn.set_title(f"Net Revenue by Owner and Connected (excl. {exclude_owner_ui})")
        ax_owner_conn.set_xlabel("Lead - Owner")
        ax_owner_conn.set_ylabel("Net Amount")
        for container in ax_owner_conn.containers:
            ax_owner_conn.bar_label(container, labels=[str(int(round(v))) for v in container.datavalues], padding=2, fontsize=8)
        plt.tight_layout()

        fig_time, ax_time = plt.subplots(figsize=(10, 5))
        if not time_breakdown.empty:
            ax_time.plot(pd.to_datetime(time_breakdown["date"]), time_breakdown["Net_Amount"])
        ax_time.set_title(f"Net Amount by Day (excl. {exclude_owner_ui})")
        ax_time.set_xlabel("Date")
        ax_time.set_ylabel("Net Amount")
        plt.tight_layout()

        # Prepare export tables (remove non-Excel-friendly lists)
        joined_export = joined.copy()
        joined_export["labels_list"] = joined_export["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")

        joined_nonzero_export = joined_nonzero.copy()
        joined_nonzero_export["labels_list"] = joined_nonzero_export["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")

        # Build excel
        fname, excel_bytes = _build_excel(
            leads_with_payments=joined_export,
            leads_nonzero=joined_nonzero_export,
            email_summary=email_summary,
            owner_breakdown=owner_breakdown,
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
        )

    # UI
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

        st.pyplot(fig_owner, use_container_width=True)

    with tab_main:
        st.markdown("#### Leads with payments (includes all owners)")
        st.dataframe(joined_export, use_container_width=True)
        st.markdown("#### Leads with non-zero payments only")
        st.dataframe(joined_nonzero_export, use_container_width=True)

    with tab_summaries:
        st.markdown("#### Owner breakdown (excluded owner removed)")
        st.dataframe(owner_breakdown, use_container_width=True)
        st.markdown("#### Owner x Connected")
        st.dataframe(owner_x_connected, use_container_width=True)
        st.pyplot(fig_owner_conn, use_container_width=True)
        st.markdown("#### Connected breakdown")
        st.dataframe(connected_breakdown, use_container_width=True)
        st.markdown("#### Label breakdown")
        st.dataframe(label_breakdown, use_container_width=True)
        st.markdown("#### Email summary (excluded owner removed)")
        st.dataframe(email_summary, use_container_width=True)

    with tab_time:
        st.markdown("#### Daily breakdown (payments/refunds/net), excluded owner removed")
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
