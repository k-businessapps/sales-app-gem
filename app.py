import base64
import json
import re
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Font
from openpyxl.utils.dataframe import dataframe_to_rows

# =========================
# Branding
# =========================
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PRIMARY = "#B04EF0"
KC_ACCENT = "#E060F0"
KC_DEEP = "#8030F0"
KC_SOFT = "#F6F0FF"

EXCLUDED_OWNER_CANON = "pipedrive krispcall"
CREDIT_EXCLUDE_DESCS = {"purchased credit", "credit purchased", "amount recharged"}


# =========================
# Secrets + UI helpers
# =========================
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
          .block-container {{ padding-top: 1.1rem; padding-bottom: 1.2rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _logo_html(width_px: int = 220, top_pad_px: int = 10) -> str:
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


def _mixpanel_headers() -> Dict[str, str]:
    auth = _get_secret(["mixpanel", "authorization"])
    if not auth:
        raise RuntimeError("Missing mixpanel.authorization in Streamlit secrets.")
    return {"accept": "text/plain", "authorization": str(auth).strip()}


# =========================
# Core utilities
# =========================
def _excel_safe(v):
    """Convert values to Excel-safe primitives. Avoid openpyxl ValueError."""
    if v is None or v is pd.NA:
        return ""
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return ""
    if isinstance(v, (np.floating, np.integer)):
        return v.item()
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is not None:
            v = v.tz_convert(None)
        return v.to_pydatetime()
    if isinstance(v, (datetime, date)):
        return v
    if isinstance(v, (list, tuple, set)):
        return ", ".join([str(x) for x in v])
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return v


def _norm_text(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val).strip().lower()


def _extract_emails(value) -> List[str]:
    if value is None:
        return []
    try:
        if isinstance(value, float) and pd.isna(value):
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


def _pick_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Case-insensitive lookup first, then exact."""
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _parse_time_to_dt(series: pd.Series) -> pd.Series:
    t = pd.to_numeric(series, errors="coerce")
    if t.dropna().empty:
        return pd.to_datetime(series, errors="coerce", utc=True)
    if float(t.median()) > 1e11:  # ms epoch
        t = (t // 1000)
    return pd.to_datetime(t, unit="s", utc=True)


# =========================
# Mixpanel export + DEDUPE (your logic)
# =========================
def dedupe_mixpanel_export(df: pd.DataFrame) -> pd.DataFrame:
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

    before = len(df)
    df = df.drop_duplicates(
        subset=["event", "distinct_id", "_time_s", "$insert_id"],
        keep="last",
    )
    after = len(df)

    df = df.drop(columns=["_time_s"])
    df.attrs["dedupe_removed"] = before - after
    df.attrs["dedupe_before"] = before
    df.attrs["dedupe_after"] = after
    return df


@st.cache_data(show_spinner=False, ttl=600)
def fetch_mixpanel_event_export(project_id: int, base_url: str, from_date: date, to_date: date, event_name: str) -> pd.DataFrame:
    """
    Fetches Mixpanel Export API and returns a flattened dataframe with:
    - top-level fields (event, distinct_id, time, etc.)
    - properties flattened into columns
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

    objs = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            objs.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not objs:
        return pd.DataFrame()

    raw = pd.DataFrame(objs)
    if "properties" in raw.columns:
        props = pd.json_normalize(raw["properties"])
        raw = pd.concat([raw.drop(columns=["properties"]), props], axis=1)

    # dt
    if "time" in raw.columns and not raw.empty:
        raw["_dt"] = _parse_time_to_dt(raw["time"])
    return raw


# =========================
# Leads parsing (emails, labels, connected, created date)
# =========================
def _split_labels(value) -> List[str]:
    if value is None:
        return []
    try:
        if isinstance(value, float) and pd.isna(value):
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


def _expand_leads_for_multiple_emails(df: pd.DataFrame, email_cols_priority: List[str]) -> Tuple[pd.DataFrame, List[int]]:
    """
    Resolve emails from first non-empty email column (priority order).
    If that cell contains multiple emails, duplicate row for each email.
    Keeps rows with missing email (email=None). Returns those row numbers for logs.
    """
    missing_rows: List[int] = []
    expanded = []

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
            # Excel-style row numbering (header = row 1)
            missing_rows.append(i + 2)
            continue

        for e in emails:
            rec = row.to_dict()
            rec["email"] = e
            expanded.append(rec)

    return pd.DataFrame(expanded), missing_rows


# =========================
# Business logic
# =========================
def _filter_credit_excluded(df: pd.DataFrame, text_col: Optional[str]) -> pd.DataFrame:
    """Remove credit-related rows by exact (case-insensitive) match on the provided text column."""
    if df.empty or not text_col or text_col not in df.columns:
        return df.copy()
    mask = df[text_col].apply(_norm_text).isin(CREDIT_EXCLUDE_DESCS)
    return df[~mask].copy()


def _windowed_email_summary(
    payments_gross: pd.DataFrame,
    refunds_gross: pd.DataFrame,
    payments_ce: pd.DataFrame,
    refunds_ce: pd.DataFrame,
    amount_col: str,
    desc_col: Optional[str],
    refund_amount_col: str,
    days: int = 7,
) -> pd.DataFrame:
    """
    7-day window per email, start defined on GROSS payments:
    - earliest 'Workspace Subscription' (contains) if present, else first payment event
    Window applied to: gross payments, gross refunds, credit-excluded payments, credit-excluded refunds.
    """
    d = payments_gross.dropna(subset=["email"]).copy()
    if d.empty:
        cols = [
            "email",
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
            "Transactions",
            "Transactions_creditExcluded",
            "First_Subscription",
            "First_Payment_Date",
        ]
        return pd.DataFrame(columns=cols)

    d[amount_col] = pd.to_numeric(d[amount_col], errors="coerce").fillna(0.0)
    d = d.sort_values("_dt", kind="mergesort")

    ce_map = {e: g.sort_values("_dt", kind="mergesort") for e, g in payments_ce.dropna(subset=["email"]).groupby("email", sort=False)} if not payments_ce.empty else {}
    ref_map = {e: g.sort_values("_dt", kind="mergesort") for e, g in refunds_gross.dropna(subset=["email"]).groupby("email", sort=False)} if not refunds_gross.empty else {}
    ref_ce_map = {e: g.sort_values("_dt", kind="mergesort") for e, g in refunds_ce.dropna(subset=["email"]).groupby("email", sort=False)} if not refunds_ce.empty else {}

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

        g_ce = ce_map.get(email)
        ce_total = float(g_ce[(g_ce["_dt"] >= start) & (g_ce["_dt"] <= end)][amount_col].sum()) if g_ce is not None else 0.0


        gross_txn = int(g[(g["_dt"] >= start) & (g["_dt"] <= end)].shape[0])
        ce_txn = int(g_ce[(g_ce["_dt"] >= start) & (g_ce["_dt"] <= end)].shape[0]) if g_ce is not None else 0
        g_ref = ref_map.get(email)
        ref_total = float(g_ref[(g_ref["_dt"] >= start) & (g_ref["_dt"] <= end)][refund_amount_col].sum()) if g_ref is not None else 0.0

        g_ref_ce = ref_ce_map.get(email)
        ref_ce_total = float(g_ref_ce[(g_ref_ce["_dt"] >= start) & (g_ref_ce["_dt"] <= end)][refund_amount_col].sum()) if g_ref_ce is not None else 0.0

        out.append(
            {
                "email": email,
                "Net_Amount": gross_total - ref_total,
                "Net_Amount_creditExcluded": ce_total - ref_ce_total,
                "Total_Amount": gross_total,
                "Total_Amount_creditExcluded": ce_total,
                "Refund_Amount": ref_total,
                "Refund_Amount_creditExcluded": ref_ce_total,
                "Transactions": gross_txn,
                "Transactions_creditExcluded": ce_txn,
                "First_Subscription": "TRUE" if trigger else "FALSE",
                "First_Payment_Date": pd.to_datetime(start, utc=True).tz_convert(None) if pd.notna(start) else None,
            }
        )

    return pd.DataFrame(out)


def _add_totals_row(df: pd.DataFrame, label_col: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    num_cols = out.select_dtypes(include=[np.number]).columns.tolist()
    totals = {c: float(pd.to_numeric(out[c], errors="coerce").fillna(0).sum()) for c in num_cols}
    row = {c: "" for c in out.columns}
    if label_col and label_col in out.columns:
        row[label_col] = "TOTAL"
    else:
        row[out.columns[0]] = "TOTAL"
    row.update(totals)
    return pd.concat([out, pd.DataFrame([row])], ignore_index=True)



def _style_totals_row(df: pd.DataFrame):
    if df is None or df.empty:
        return df

    def _row_style(row):
        is_total = str(row.iloc[0]).strip().upper() == "TOTAL"
        return ["font-weight: bold" if is_total else "" for _ in row]

    return df.style.apply(_row_style, axis=1)



def _style_sheet(ws):
    # Header
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.freeze_panes = "A2"

    # Bold TOTAL rows (summations)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        first = row[0].value
        if first is None:
            continue
        if str(first).strip().upper() == "TOTAL":
            for cell in row:
                cell.font = Font(bold=True)

    # Best-effort column widths
    for col in ws.columns:
        letter = col[0].column_letter
        max_len = 0
        for cell in col[:250]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[letter].width = min(max(10, max_len + 2), 50)


def _build_excel(
    leads_with_payments: pd.DataFrame,
    leads_nonzero: pd.DataFrame,
    email_summary: pd.DataFrame,
    owner_summary: pd.DataFrame,
    owner_x_connected: pd.DataFrame,
    connected_summary: pd.DataFrame,
    label_summary: pd.DataFrame,
    time_summary: pd.DataFrame,
    duplicate_leads: pd.DataFrame,
    self_converted_emails: pd.DataFrame,
    logs_df: pd.DataFrame,
    fig_owner,
    fig_owner_conn,
    fig_time,
    from_date: date,
    to_date: date,
) -> Tuple[str, bytes]:
    wb = Workbook()

    def add_sheet(title: str, df: pd.DataFrame, label_col: Optional[str] = None, add_totals: bool = True):
        ws = wb.create_sheet(title)
        df2 = _add_totals_row(df, label_col=label_col) if add_totals and df is not None and not df.empty else df
        if df2 is None or df2.empty:
            ws.append(["No data"])
            _style_sheet(ws)
            return ws
        for r in dataframe_to_rows(df2, index=False, header=True):
            ws.append([_excel_safe(x) for x in r])
        _style_sheet(ws)
        return ws

    # Main sheet
    ws0 = wb.active
    ws0.title = "Leads_with_Payments"
    main_df = _add_totals_row(leads_with_payments, label_col=None)
    for r in dataframe_to_rows(main_df, index=False, header=True):
        ws0.append([_excel_safe(x) for x in r])
    _style_sheet(ws0)

    add_sheet("Leads_Payments_NonZero", leads_nonzero, label_col=None)
    add_sheet("Email_Summary", email_summary, label_col="email")
    ws_owner = add_sheet("Owner_Summary", owner_summary, label_col=owner_summary.columns[0] if not owner_summary.empty else None, add_totals=False)
    add_sheet("Owner_x_Connected", owner_x_connected, label_col=owner_x_connected.columns[0] if not owner_x_connected.empty else None)
    add_sheet("Connected_Summary", connected_summary, label_col="Connected" if "Connected" in connected_summary.columns else None)
    add_sheet("Label_Summary", label_summary, label_col="Label" if "Label" in label_summary.columns else None)
    add_sheet("Time_Summary", time_summary, label_col=time_summary.columns[0] if not time_summary.empty else None)
    add_sheet("Duplicate_Leads_By_Email", duplicate_leads, label_col="email" if "email" in duplicate_leads.columns else None)
    add_sheet("SelfConverted_Emails", self_converted_emails, label_col="email" if "email" in self_converted_emails.columns else None)
    add_sheet("Logs", logs_df, label_col=None)

    # Charts sheet
    ws_chart = wb.create_sheet("Charts")

    def add_fig(ws, fig, anchor, table_df: pd.DataFrame, table_anchor_col: int, table_anchor_row: int):
        img_bytes = BytesIO()
        fig.savefig(img_bytes, format="png", dpi=150, bbox_inches="tight")
        img_bytes.seek(0)
        img = XLImage(img_bytes)
        img.anchor = anchor
        ws.add_image(img)

        if table_df is not None and not table_df.empty:
            rounded = table_df.copy()
            for c in rounded.select_dtypes(include=[np.number]).columns:
                rounded[c] = pd.to_numeric(rounded[c], errors="coerce").fillna(0).round(0).astype(int)
            for r_idx, row in enumerate(dataframe_to_rows(rounded, index=False, header=True), table_anchor_row):
                for c_idx, value in enumerate(row, table_anchor_col):
                    ws.cell(row=r_idx, column=c_idx, value=_excel_safe(value))

    add_fig(ws_chart, fig_owner, "A1", owner_summary, 8, 1)
    add_fig(ws_chart, fig_owner_conn, "A26", owner_x_connected, 8, 26)
    add_fig(ws_chart, fig_time, "A51", time_summary, 8, 51)
    _style_sheet(ws_chart)

    # Remove default "Sheet" if present
    if "Sheet" in wb.sheetnames:
        del wb["Sheet"]

    out = BytesIO()
    wb.save(out)

    fname = f"payment_summary_{from_date.strftime('%b%d').lower()}_{to_date.strftime('%b%d').lower()}.xlsx"
    return fname, out.getvalue()


# =========================
# Main app
# =========================
def main():
    st.set_page_config(page_title="KrispCall Payment Summary", page_icon="📈", layout="wide")
    require_login()
    _inject_brand_css()

    # Header
    st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
    l, r = st.columns([1, 3], vertical_alignment="center")
    with l:
        st.markdown(_logo_html(width_px=240, top_pad_px=14), unsafe_allow_html=True)
    with r:
        st.markdown(
            '<div class="kc-hero"><h1>KrispCall Payment Summary</h1><p>Leads reconciliation with Mixpanel transactions</p></div>',
            unsafe_allow_html=True,
        )

    # Sidebar
    with st.sidebar:
        st.markdown("### Date Selection")
        today = date.today()
        first_of_month = today.replace(day=1)

        # Default: 4 days before first of current month (Jan -> Dec 28)
        default_start = first_of_month - timedelta(days=4)
        default_end = today - timedelta(days=1)

        from_date = st.date_input("Date from", value=default_start)
        to_date = st.date_input("Date to", value=default_end)

        st.markdown("---")
        st.markdown("### Rules")
        st.info("Summaries exclude owner: Pipedrive KrispCall. Main tables still include it.")
        st.info("Time summary groups by Lead - Lead created on date (after summaries dedupe).")
        st.info("7-day window per email based on earliest Workspace Subscription payment if present. Else first payment event.")

    # Inputs
    st.markdown('<div class="kc-card">', unsafe_allow_html=True)
    leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])
    run = st.button("Run Analysis", type="primary", disabled=(leads_file is None))
    st.markdown("</div>", unsafe_allow_html=True)

    if not run:
        st.stop()

    logs: List[str] = []

    with st.spinner("Running analysis..."):
        leads_raw = pd.read_csv(leads_file)

        # Owner + created + labels
        owner_col = _pick_first_existing_column(leads_raw, ["Lead - Owner", "Deal - Owner", "Owner", "owner"]) or "Owner"
        created_col = _pick_first_existing_column(leads_raw, ["Lead - Lead created on", "Lead created on", "Created on"])
        label_col = _pick_first_existing_column(leads_raw, ["Lead - Label", "Label", "Labels", "Lead - Labels"])

        # Email columns priority for THIS workflow
        email_cols_priority: List[str] = []
        for cand in ["Person - Email", "Lead - User Email"]:
            col = _pick_first_existing_column(leads_raw, [cand])
            if col and col not in email_cols_priority:
                email_cols_priority.append(col)

        # Fallback: if those columns are missing, fall back to any column containing "email"
        if not email_cols_priority:
            fallback = [c for c in leads_raw.columns if "email" in c.lower()]
            email_cols_priority = fallback[:5]

        expanded_leads, missing_rows = _expand_leads_for_multiple_emails(leads_raw, email_cols_priority)
        if missing_rows:
            logs.append(f"Missing email for {len(missing_rows)} lead row(s). Example rows: {missing_rows[:20]}")

        # labels_list + Connected
        if label_col and label_col in expanded_leads.columns:
            expanded_leads["labels_list"] = expanded_leads[label_col].apply(_split_labels)
        else:
            expanded_leads["labels_list"] = [[] for _ in range(len(expanded_leads))]

        expanded_leads["Connected"] = expanded_leads["labels_list"].apply(_connected_from_labels)

        # lead created dt
        if created_col and created_col in expanded_leads.columns:
            expanded_leads["_lead_created_dt"] = pd.to_datetime(expanded_leads[created_col], errors="coerce")
        else:
            expanded_leads["_lead_created_dt"] = pd.NaT

        # Mixpanel exports
        pid = int(_get_secret(["mixpanel", "project_id"]))
        base = _get_secret(["mixpanel", "base_url"], "https://data-eu.mixpanel.com")

        # Payments always on selected window
        payments_raw = fetch_mixpanel_event_export(pid, base, from_date, to_date, "New Payment Made")

        # Refund window rule: if selected window < 2 months, export last 2 months
        window_days = (to_date - from_date).days
        refund_from = (to_date - timedelta(days=60)) if window_days < 60 else from_date
        refunds_raw = fetch_mixpanel_event_export(pid, base, refund_from, to_date, "Refund Granted")

        # Dedupe (your logic)
        if not payments_raw.empty:
            payments = dedupe_mixpanel_export(payments_raw)
            logs.append(
                f"Payments raw rows: {payments.attrs.get('dedupe_before', len(payments_raw))}. Dedupe removed: {payments.attrs.get('dedupe_removed', 0)}."
            )
        else:
            payments = payments_raw.copy()
            logs.append("Payments raw rows: 0.")

        if not refunds_raw.empty:
            refunds = dedupe_mixpanel_export(refunds_raw)
            logs.append(
                f"Refunds raw rows: {refunds.attrs.get('dedupe_before', len(refunds_raw))}. Dedupe removed: {refunds.attrs.get('dedupe_removed', 0)}."
            )
        else:
            refunds = refunds_raw.copy()
            logs.append("Refunds raw rows: 0.")

        # Identify email columns in exports (case-insensitive)
        pay_email_col = _pick_first_existing_column(payments, ["$email", "email", "Email", "EMAIL", "User Email", "user.email"])
        ref_email_col = _pick_first_existing_column(refunds, ["User Email", "user.email", "$email", "email", "Email", "EMAIL"])

        payments["email"] = payments[pay_email_col].apply(lambda v: (_extract_emails(v)[0] if _extract_emails(v) else None)) if pay_email_col and pay_email_col in payments.columns else None
        refunds["email"] = refunds[ref_email_col].apply(lambda v: (_extract_emails(v)[0] if _extract_emails(v) else None)) if ref_email_col and ref_email_col in refunds.columns else None

        # Amount + description columns
        amount_col = _pick_first_existing_column(payments, ["Amount", "amount", "Amount Paid"])
        desc_col = _pick_first_existing_column(payments, ["Amount Description", "description", "Plan"])

        refund_amount_col = _pick_first_existing_column(refunds, ["Refund Amount", "refund_amount", "Amount", "amount"])
        refund_desc_col = _pick_first_existing_column(refunds, ["Refunded Transaction description", "Refunded Transaction Description", "Refunded Transaction"])

        if not amount_col:
            raise RuntimeError("Could not find payment amount column in Mixpanel export (expected 'Amount').")
        if not refund_amount_col:
            refunds["Refund Amount"] = 0.0
            refund_amount_col = "Refund Amount"

        # Numeric safety
        payments[amount_col] = pd.to_numeric(payments[amount_col], errors="coerce").fillna(0.0)
        refunds[refund_amount_col] = pd.to_numeric(refunds[refund_amount_col], errors="coerce").fillna(0.0)


        # Leads emails list from the uploaded export
        lead_emails = set(expanded_leads["email"].dropna().unique())

        # Keep full Mixpanel pulls for overall metrics and self-converted detection
        payments_all = payments.copy()
        refunds_all = refunds.copy()

        # Credit excluded variants (full)
        payments_all_ce = _filter_credit_excluded(payments_all, desc_col)
        refunds_all_ce = _filter_credit_excluded(refunds_all, refund_desc_col)

        # Overall metrics (selected date range only)
        overall_revenue = float(pd.to_numeric(payments_all[amount_col], errors="coerce").fillna(0).sum())
        overall_revenue_ce = float(pd.to_numeric(payments_all_ce[amount_col], errors="coerce").fillna(0).sum()) if not payments_all_ce.empty else 0.0
        overall_transactions = int(len(payments_all))
        overall_transactions_ce = int(len(payments_all_ce)) if not payments_all_ce.empty else 0

        # Overall refunds should only count events inside selected date range (even if refunds were fetched for a longer window)
        if "_dt" in refunds_all.columns and not refunds_all.empty:
            _ref_in_range = refunds_all["_dt"].dt.date.between(from_date, to_date)
            overall_refunds = float(pd.to_numeric(refunds_all.loc[_ref_in_range, refund_amount_col], errors="coerce").fillna(0).sum())
        else:
            overall_refunds = 0.0

        if "_dt" in refunds_all_ce.columns and not refunds_all_ce.empty:
            _ref_in_range_ce = refunds_all_ce["_dt"].dt.date.between(from_date, to_date)
            overall_refunds_ce = float(pd.to_numeric(refunds_all_ce.loc[_ref_in_range_ce, refund_amount_col], errors="coerce").fillna(0).sum())
        else:
            overall_refunds_ce = 0.0

        # Leads-only subsets for join tables and summary base
        payments = payments_all[payments_all["email"].isin(lead_emails)].copy()
        refunds = refunds_all[refunds_all["email"].isin(lead_emails)].copy()

        payments_ce = payments_all_ce[payments_all_ce["email"].isin(lead_emails)].copy() if not payments_all_ce.empty else payments_all_ce.copy()
        refunds_ce = refunds_all_ce[refunds_all_ce["email"].isin(lead_emails)].copy() if not refunds_all_ce.empty else refunds_all_ce.copy()

        # Windowed per-email summary (Net first columns)
        email_summary = _windowed_email_summary(
            payments_gross=payments,
            refunds_gross=refunds,
            payments_ce=payments_ce,
            refunds_ce=refunds_ce,
            amount_col=amount_col,
            desc_col=desc_col,
            refund_amount_col=refund_amount_col,
            days=7,
        )

        # Join back to leads (preserve all rows)
        joined = expanded_leads.merge(email_summary, on="email", how="left")
        metric_cols = [
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
            "Transactions",
            "Transactions_creditExcluded",
        ]
        for c in metric_cols:
            joined[c] = pd.to_numeric(joined[c], errors="coerce").fillna(0.0)
        joined["First_Subscription"] = joined.get("First_Subscription", "FALSE").fillna("FALSE")

        # Non-zero table
        joined_nonzero = joined[joined["Total_Amount"].fillna(0).ne(0)].copy()

        # =========================
        # Summaries base:
        # - Exclude owner Pipedrive KrispCall from ALL summaries
        # - If duplicates remain (by email), keep earliest Lead created on
        # - Time summary grouped by Lead created on date (not payment date)
        # =========================
        owner_series = joined[owner_col].astype(str).str.strip().str.lower()
        summ_base = joined[~owner_series.eq(EXCLUDED_OWNER_CANON)].copy()

        summ_base["_lead_created_dt"] = pd.to_datetime(summ_base["_lead_created_dt"], errors="coerce")

        dup_mask = summ_base["email"].notna() & summ_base["email"].duplicated(keep=False)
        duplicate_leads = summ_base.loc[dup_mask].sort_values(["email", "_lead_created_dt"], kind="mergesort").copy()

        summ_dedup = (
            summ_base.sort_values(["email", "_lead_created_dt"], kind="mergesort")
            .drop_duplicates(subset=["email"], keep="first")
            .copy()
        )

        logs.append(f"Summary dedupe kept 1 row per email after excluding '{EXCLUDED_OWNER_CANON}'.")
        logs.append(f"Duplicate leads (after excluding '{EXCLUDED_OWNER_CANON}') rows: {len(duplicate_leads)}.")

        # Non-zero users helper
        def nonzero_users_count(df_: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
            d = df_[df_["Total_Amount"].fillna(0).ne(0)].dropna(subset=["email"]).copy()
            if d.empty:
                return pd.DataFrame(columns=group_cols + ["NonZero_Users"])
            return d.groupby(group_cols, as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})

        # Owner summary
        owner_summary = summ_dedup.groupby(owner_col, as_index=False)[metric_cols].sum().sort_values("Net_Amount", ascending=False)
        owner_summary = owner_summary.merge(nonzero_users_count(summ_dedup, [owner_col]), on=owner_col, how="left").fillna({"NonZero_Users": 0})

        # Connected summary
        connected_summary = summ_dedup.groupby(["Connected"], as_index=False)[metric_cols].sum().sort_values("Net_Amount", ascending=False)
        connected_summary = connected_summary.merge(nonzero_users_count(summ_dedup, ["Connected"]), on="Connected", how="left").fillna({"NonZero_Users": 0})

        # Owner x Connected
        owner_x_connected = (
            summ_dedup.groupby([owner_col, "Connected"], as_index=False)[metric_cols].sum().sort_values("Net_Amount", ascending=False)
        )
        owner_x_connected = owner_x_connected.merge(nonzero_users_count(summ_dedup, [owner_col, "Connected"]), on=[owner_col, "Connected"], how="left").fillna({"NonZero_Users": 0})

        # Label summary
        labels_expanded = summ_dedup[["email", "labels_list"] + metric_cols].copy()
        labels_expanded = labels_expanded.explode("labels_list").rename(columns={"labels_list": "Label"})
        labels_expanded["Label"] = labels_expanded["Label"].fillna("").astype(str).str.strip()
        labels_expanded = labels_expanded[labels_expanded["Label"] != ""].copy()
        labels_expanded["Connected_Label"] = labels_expanded["Label"].str.lower().apply(
            lambda s: False if s.strip() == "not connected" else ("connected" in s)
        )
        label_summary = labels_expanded.groupby(["Label", "Connected_Label"], as_index=False)[metric_cols].sum().sort_values("Net_Amount", ascending=False)
        label_counts = (
            labels_expanded[labels_expanded["Total_Amount"].fillna(0).ne(0)]
            .dropna(subset=["email"])
            .groupby(["Label", "Connected_Label"], as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "NonZero_Users"})
        )
        label_summary = label_summary.merge(label_counts, on=["Label", "Connected_Label"], how="left").fillna({"NonZero_Users": 0})

        # Time summary grouped by Lead created date (from deduped lead rows)
        summ_dedup["Lead_Created_Date"] = pd.to_datetime(summ_dedup["_lead_created_dt"], errors="coerce").dt.date
        time_summary = summ_dedup.groupby(["Lead_Created_Date"], as_index=False)[metric_cols].sum().sort_values("Lead_Created_Date")
        time_summary = time_summary.merge(nonzero_users_count(summ_dedup, ["Lead_Created_Date"]), on="Lead_Created_Date", how="left").fillna({"NonZero_Users": 0})

        # =========================
        # Self-converted logic (your updated requirement)
        # 1) Subscription payer emails = New Payment Made events where Amount Description contains "Workspace Subscription"
        # 2) Sales leads email list excludes Pipedrive owner
        # 3) Self-converted = subscription_payers - sales_leads_emails
        # =========================
        sales_lead_emails = set(summ_base["email"].dropna().unique())

        sub_mask = payments_all["email"].notna()
        if desc_col and desc_col in payments_all.columns:
            sub_mask &= payments_all[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
        subscription_emails = set(payments_all.loc[sub_mask, "email"].dropna().unique())

        self_converted_emails_list = sorted(list(subscription_emails - sales_lead_emails))
        logs.append(f"Workspace Subscription payer emails: {len(subscription_emails)}.")
        logs.append(f"Self-converted emails (subscription payers not in sales lead list): {len(self_converted_emails_list)}.")

        # Compute self-converted revenue using same window rules
        pay_sc = payments_all[payments_all["email"].isin(self_converted_emails_list)].copy()
        ref_sc = refunds_all[refunds_all["email"].isin(self_converted_emails_list)].copy()
        pay_sc_ce = payments_all_ce[payments_all_ce["email"].isin(self_converted_emails_list)].copy()
        ref_sc_ce = refunds_all_ce[refunds_all_ce["email"].isin(self_converted_emails_list)].copy()

        self_converted_fact = _windowed_email_summary(
            payments_gross=pay_sc,
            refunds_gross=ref_sc,
            payments_ce=pay_sc_ce,
            refunds_ce=ref_sc_ce,
            amount_col=amount_col,
            desc_col=desc_col,
            refund_amount_col=refund_amount_col,
            days=7,
        ).sort_values("Net_Amount", ascending=False)

        # Owner Summary extra block values
        # Owner Summary extra block values (self-converted totals)
        if not self_converted_fact.empty:
            sc_total = float(pd.to_numeric(self_converted_fact.get("Total_Amount", 0), errors="coerce").fillna(0).sum())
            sc_total_ce = float(pd.to_numeric(self_converted_fact.get("Total_Amount_creditExcluded", 0), errors="coerce").fillna(0).sum())
            sc_ref = float(pd.to_numeric(self_converted_fact.get("Refund_Amount", 0), errors="coerce").fillna(0).sum())
            sc_ref_ce = float(pd.to_numeric(self_converted_fact.get("Refund_Amount_creditExcluded", 0), errors="coerce").fillna(0).sum())
            sc_net = float(pd.to_numeric(self_converted_fact.get("Net_Amount", 0), errors="coerce").fillna(0).sum())
            sc_net_ce = float(pd.to_numeric(self_converted_fact.get("Net_Amount_creditExcluded", 0), errors="coerce").fillna(0).sum())
            sc_txn = int(pd.to_numeric(self_converted_fact.get("Transactions", 0), errors="coerce").fillna(0).sum())
            sc_txn_ce = int(pd.to_numeric(self_converted_fact.get("Transactions_creditExcluded", 0), errors="coerce").fillna(0).sum())
            sc_users = int(self_converted_fact["email"].nunique()) if "email" in self_converted_fact.columns else int(len(self_converted_emails_list))
        else:
            sc_total = 0.0
            sc_total_ce = 0.0
            sc_ref = 0.0
            sc_ref_ce = 0.0
            sc_net = 0.0
            sc_net_ce = 0.0
            sc_txn = 0
            sc_txn_ce = 0
            sc_users = int(len(self_converted_emails_list))

        # =========================
        # Charts
        # =========================
        # Owner chart
        fig_owner, ax_owner = plt.subplots(figsize=(10, 6))
        if not owner_summary.empty:
            chart_df = owner_summary.set_index(owner_col)[["Net_Amount", "Net_Amount_creditExcluded"]]
            chart_df.plot(kind="bar", ax=ax_owner)
            for container in ax_owner.containers:
                ax_owner.bar_label(container, labels=[str(int(round(v))) for v in container.datavalues], padding=2, fontsize=8)
        ax_owner.set_title("Net Revenue by Owner (summaries exclude Pipedrive KrispCall)")
        ax_owner.set_xlabel("Owner")
        ax_owner.set_ylabel("Amount")
        plt.tight_layout()

        # Owner x Connected chart (stacked net)
        fig_owner_conn, ax_owner_conn = plt.subplots(figsize=(10, 6))
        if not owner_x_connected.empty:
            pivot = owner_x_connected.pivot_table(index=owner_col, columns="Connected", values="Net_Amount", aggfunc="sum").fillna(0.0)
            pivot = pivot.rename(columns={False: "FALSE", True: "TRUE"})
            cols = [c for c in ["FALSE", "TRUE"] if c in pivot.columns]
            if cols:
                pivot = pivot.loc[:, cols]
            pivot.plot(kind="bar", stacked=True, ax=ax_owner_conn)
            for container in ax_owner_conn.containers:
                ax_owner_conn.bar_label(container, labels=[str(int(round(v))) for v in container.datavalues], padding=2, fontsize=8)
        ax_owner_conn.set_title("Net Revenue by Owner and Connected")
        ax_owner_conn.set_xlabel("Owner")
        ax_owner_conn.set_ylabel("Net Amount")
        plt.tight_layout()

        # Time chart
        fig_time, ax_time = plt.subplots(figsize=(10, 5))
        if not time_summary.empty:
            ax_time.plot(pd.to_datetime(time_summary["Lead_Created_Date"]), time_summary["Net_Amount"])
        ax_time.set_title("Net Amount by Lead Created Date")
        ax_time.set_xlabel("Date")
        ax_time.set_ylabel("Net Amount")
        plt.tight_layout()

        # Prepare excel-friendly main tables
        joined_export = joined.copy()
        joined_export["labels_list"] = joined_export["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")

        joined_nonzero_export = joined_nonzero.copy()
        joined_nonzero_export["labels_list"] = joined_nonzero_export["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")

        # Owner summary with self-converted + sales attempt block inside the sheet
        owner_for_excel = _add_totals_row(owner_summary, label_col=owner_col)

        # Append a mini block rows as extra records (kept in the same sheet, below table)
        owner_block = pd.DataFrame(
            [
                {
                    owner_col: "Self Converted Revenue",
                    "Net_Amount": sc_net,
                    "Net_Amount_creditExcluded": sc_net_ce,
                    "Total_Amount": sc_total,
                    "Total_Amount_creditExcluded": sc_total_ce,
                    "Refund_Amount": sc_ref,
                    "Refund_Amount_creditExcluded": sc_ref_ce,
                    "Transactions": sc_txn,
                    "Transactions_creditExcluded": sc_txn_ce,
                    "NonZero_Users": sc_users,
                },
                {
                    owner_col: "Self Converted Number of Users",
                    "NonZero_Users": sc_users,
                },
                {
                    owner_col: "Overall Revenue (All Sources, Selected Range)",
                    "Total_Amount": overall_revenue,
                    "Total_Amount_creditExcluded": overall_revenue_ce,
                    "Transactions": overall_transactions,
                    "Transactions_creditExcluded": overall_transactions_ce,
                },
                {
                    owner_col: "Overall Refunds (All Sources, Selected Range)",
                    "Refund_Amount": overall_refunds,
                    "Refund_Amount_creditExcluded": overall_refunds_ce,
                },
                {
                    owner_col: "Number of Transactions (All Sources, Selected Range)",
                    "Transactions": overall_transactions,
                    "Transactions_creditExcluded": overall_transactions_ce,
                },
                {
                    owner_col: "Sales Attempt Revenue (Summaries Total)",
                    "Net_Amount": float(pd.to_numeric(owner_summary["Net_Amount"], errors="coerce").fillna(0).sum()),
                    "Net_Amount_creditExcluded": float(pd.to_numeric(owner_summary["Net_Amount_creditExcluded"], errors="coerce").fillna(0).sum()),
                    "Total_Amount": float(pd.to_numeric(owner_summary["Total_Amount"], errors="coerce").fillna(0).sum()) if "Total_Amount" in owner_summary.columns else "",
                    "Total_Amount_creditExcluded": float(pd.to_numeric(owner_summary["Total_Amount_creditExcluded"], errors="coerce").fillna(0).sum()) if "Total_Amount_creditExcluded" in owner_summary.columns else "",
                    "Refund_Amount": float(pd.to_numeric(owner_summary["Refund_Amount"], errors="coerce").fillna(0).sum()) if "Refund_Amount" in owner_summary.columns else "",
                    "Refund_Amount_creditExcluded": float(pd.to_numeric(owner_summary["Refund_Amount_creditExcluded"], errors="coerce").fillna(0).sum()) if "Refund_Amount_creditExcluded" in owner_summary.columns else "",
                    "Transactions": int(pd.to_numeric(owner_summary.get("Transactions", 0), errors="coerce").fillna(0).sum()),
                    "Transactions_creditExcluded": int(pd.to_numeric(owner_summary.get("Transactions_creditExcluded", 0), errors="coerce").fillna(0).sum()),
                    "NonZero_Users": int(pd.to_numeric(owner_summary.get("NonZero_Users", 0), errors="coerce").fillna(0).sum()) if "NonZero_Users" in owner_summary.columns else "",
                },
            ]
        )
        # Ensure all columns exist
        for c in owner_for_excel.columns:
            if c not in owner_block.columns:
                owner_block[c] = ""
        owner_for_excel = pd.concat([owner_for_excel, pd.DataFrame([{c: "" for c in owner_for_excel.columns}]), owner_block[owner_for_excel.columns]], ignore_index=True)

        # Logs df
        logs_df = pd.DataFrame({"log": logs})

        # Excel build
        excel_name, excel_bytes = _build_excel(
            leads_with_payments=joined_export,
            leads_nonzero=joined_nonzero_export,
            email_summary=email_summary,
            owner_summary=owner_for_excel,
            owner_x_connected=owner_x_connected,
            connected_summary=connected_summary,
            label_summary=label_summary,
            time_summary=time_summary,
            duplicate_leads=duplicate_leads,
            self_converted_emails=self_converted_fact,
            logs_df=logs_df,
            fig_owner=fig_owner,
            fig_owner_conn=fig_owner_conn,
            fig_time=fig_time,
            from_date=from_date,
            to_date=to_date,
        )

    # =========================
    # UI tabs
    # =========================
    tab_overview, tab_tables, tab_summaries, tab_time, tab_export, tab_logs = st.tabs(
        ["Overview", "Main Tables", "Summaries", "Time", "Export", "Logs"]
    )

    with tab_overview:
        # Summary totals come from owner_summary total (dedup + excluded)
        total_net = float(pd.to_numeric(owner_summary["Net_Amount"], errors="coerce").fillna(0).sum()) if not owner_summary.empty else 0.0
        total_net_ce = float(pd.to_numeric(owner_summary["Net_Amount_creditExcluded"], errors="coerce").fillna(0).sum()) if not owner_summary.empty else 0.0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Net Revenue (summaries)", f"{total_net:,.2f}")
        c2.metric("Net Revenue creditExcluded", f"{total_net_ce:,.2f}")
        c3.metric("Self-Converted Contacts", f"{len(self_converted_emails_list):,}")
        c4.metric("Self-Converted Net", f"{sc_net:,.2f}")

        st.pyplot(fig_owner, use_container_width=True)

    with tab_tables:
        st.markdown("#### Leads with payments (includes all owners, including Pipedrive KrispCall)")
        st.dataframe(joined_export, use_container_width=True)
        st.markdown("#### Leads with non-zero payments only")
        st.dataframe(joined_nonzero_export, use_container_width=True)

    with tab_summaries:
        st.markdown("#### Owner Summary (Pipedrive KrispCall excluded)")
        st.dataframe(_style_totals_row(_add_totals_row(owner_summary, label_col=owner_summary.columns[0] if not owner_summary.empty else None)), use_container_width=True)
        st.markdown("#### Owner x Connected (Pipedrive KrispCall excluded)")
        st.dataframe(_style_totals_row(_add_totals_row(owner_x_connected, label_col=owner_x_connected.columns[0] if not owner_x_connected.empty else None)), use_container_width=True)
        st.pyplot(fig_owner_conn, use_container_width=True)

        st.markdown("#### Connected Summary")
        st.dataframe(_style_totals_row(_add_totals_row(connected_summary, label_col=connected_summary.columns[0] if not connected_summary.empty else None)), use_container_width=True)

        st.markdown("#### Label Summary")
        st.dataframe(_style_totals_row(_add_totals_row(label_summary, label_col=label_summary.columns[0] if not label_summary.empty else None)), use_container_width=True)

        st.markdown("#### Self-Converted Emails and Revenue")
        st.dataframe(self_converted_fact, use_container_width=True)

        st.markdown("#### Duplicate Leads by Email (after excluding Pipedrive KrispCall)")
        if duplicate_leads.empty:
            st.write("No duplicate emails detected in summaries base.")
        else:
            st.dataframe(duplicate_leads, use_container_width=True)

    with tab_time:
        st.markdown("#### Time Summary (grouped by Lead - Lead created on date)")
        st.dataframe(_style_totals_row(_add_totals_row(time_summary, label_col=time_summary.columns[0] if not time_summary.empty else None)), use_container_width=True)
        st.pyplot(fig_time, use_container_width=True)

    with tab_export:
        st.download_button(
            "Download Excel report",
            data=excel_bytes,
            file_name=excel_name,
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
