
import base64
import json
import re
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

# ----------------------------
# Constants
# ----------------------------
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PRIMARY = "#B04EF0"
KC_ACCENT = "#E060F0"
KC_DEEP = "#8030F0"
KC_SOFT = "#F6F0FF"

EXCLUDED_OWNER_CANON = "pipedrive krispcall"  # summaries exclude this
CREDIT_EXCLUDE_DESCS = {"purchased credit", "credit purchased", "amount recharged"}


# ----------------------------
# Secrets and UI helpers
# ----------------------------

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


# ----------------------------
# Mixpanel export and parsing (matches your export + normalize approach)
# ----------------------------

def _mixpanel_headers() -> Dict[str, str]:
    auth = _get_secret(["mixpanel", "authorization"])
    if not auth:
        raise RuntimeError("Missing mixpanel.authorization in Streamlit secrets.")
    return {"accept": "text/plain", "authorization": str(auth).strip()}


def _parse_time_to_dt(series: pd.Series) -> pd.Series:
    t = pd.to_numeric(series, errors="coerce")
    if t.dropna().empty:
        return pd.to_datetime(series, errors="coerce", utc=True)
    if float(t.median()) > 1e11:
        t = (t // 1000)
    return pd.to_datetime(t, unit="s", utc=True)


def dedupe_mixpanel_export(df: pd.DataFrame) -> pd.DataFrame:
    """Same logic as your dedupe_mixpanel_export snippet."""
    required = ["event", "distinct_id", "time", "$insert_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns: {missing}. Available: {list(df.columns)}")

    d = df.copy()

    t = pd.to_numeric(d["time"], errors="coerce")
    if t.notna().all():
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


@st.cache_data(show_spinner=False, ttl=600)
def fetch_mixpanel_event_export(project_id: int, base_url: str, from_date: date, to_date: date, event_name: str) -> pd.DataFrame:
    """Exports ONE event at a time (prevents wide columns)."""
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

    # Match your export logic: each line is a JSON record with {event, properties, ...}
    rows: List[Dict] = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not rows:
        return pd.DataFrame()

    # Normalize like your code: concat(top-level minus properties) + json_normalize(properties)
    # We do it in-place by flattening properties into columns, while preserving top-level fields.
    flat: List[Dict] = []
    for rec in rows:
        r = dict(rec)
        props = r.pop("properties", {}) or {}
        # Some exports may include properties not dict
        if isinstance(props, dict):
            r.update(props)
        flat.append(r)

    df = pd.DataFrame(flat)
    if not df.empty and "time" in df.columns:
        df["_dt"] = _parse_time_to_dt(df["time"])
    return df


# ----------------------------
# Leads parsing
# ----------------------------

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


def _normalize_email(value) -> Optional[str]:
    ems = _extract_emails(value)
    return ems[0] if ems else None


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df.empty:
        return None
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    for c in candidates:
        if c in df.columns:
            return c
    return None


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


def expand_leads_for_multiple_emails(df: pd.DataFrame, email_cols_priority: List[str]) -> Tuple[pd.DataFrame, List[int]]:
    missing_rows: List[int] = []
    expanded: List[Dict] = []

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
            missing_rows.append(i + 2)  # header=1, first row=2
            continue

        for e in emails:
            rec = row.to_dict()
            rec["email"] = e
            expanded.append(rec)

    return pd.DataFrame(expanded), missing_rows


# ----------------------------
# Payment summarization
# ----------------------------

def _norm_text(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val).strip().lower()


def filter_credit_excluded(df: pd.DataFrame, text_col: Optional[str]) -> pd.DataFrame:
    if df.empty or not text_col or text_col not in df.columns:
        return df
    mask = df[text_col].apply(_norm_text).isin(CREDIT_EXCLUDE_DESCS)
    return df[~mask].copy()


def _resolve_event_email(df: pd.DataFrame) -> pd.Series:
    """Row-wise email resolution: scan email-like columns (case-insensitive) and take first match."""
    if df.empty:
        return pd.Series([], dtype="object")

    email_cols = [c for c in df.columns if "email" in c.lower()]
    # Keep stable order (original column order)
    def first_email_for_row(row) -> Optional[str]:
        for c in email_cols:
            ems = _extract_emails(row.get(c))
            if ems:
                return ems[0]
        return None

    return df.apply(first_email_for_row, axis=1)


def windowed_payments_dual(
    payments_gross: pd.DataFrame,
    payments_credit_excluded: pd.DataFrame,
    amount_col: str,
    desc_col: Optional[str],
    days: int = 7,
) -> pd.DataFrame:
    """7-day window per email; start = earliest Workspace Subscription if present else earliest payment."""
    d = payments_gross.dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(
            columns=[
                "email",
                "Total_Amount",
                "Total_Amount_creditExcluded",
                "First_Subscription",
                "First_Payment_Date",
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
        df["First_Payment_Date"] = pd.to_datetime(df["First_Payment_Date"], utc=True, errors="coerce").dt.tz_convert(None)
    return df


def _sum_refunds(df: pd.DataFrame, email_col: str, amount_col: str, out_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["email", out_name])
    d = df.dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["email", out_name])
    d[amount_col] = pd.to_numeric(d[amount_col], errors="coerce").fillna(0.0)
    return d.groupby("email", as_index=False)[amount_col].sum().rename(columns={amount_col: out_name})


def _earliest_event_date(payments: pd.DataFrame, refunds: pd.DataFrame) -> pd.DataFrame:
    # returns email -> earliest _dt across payments/refunds
    p = payments.dropna(subset=["email"]).copy()
    r = refunds.dropna(subset=["email"]).copy()
    out = []
    if not p.empty:
        out.append(p.groupby("email", as_index=False)["_dt"].min().rename(columns={"_dt": "_pmin"}))
    if not r.empty:
        out.append(r.groupby("email", as_index=False)["_dt"].min().rename(columns={"_dt": "_rmin"}))
    if not out:
        return pd.DataFrame(columns=["email", "Earliest_Event_Date"])

    m = out[0]
    for other in out[1:]:
        m = m.merge(other, on="email", how="outer")

    m["Earliest_Event_Date"] = pd.to_datetime(m[[c for c in m.columns if c.startswith("_")]].min(axis=1), utc=True, errors="coerce").dt.tz_convert(None)
    return m[["email", "Earliest_Event_Date"]]


# ----------------------------
# Summary logic (matches the accepted v3 Excel)
# ----------------------------

def _parse_lead_created(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    col = _pick_col(df, ["Lead - Lead created on"])
    if not col:
        return df, None
    out = df.copy()
    out[col] = pd.to_datetime(out[col], errors="coerce")
    return out, col


def _nonzero_users_count(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    d = df[df["Total_Amount"].fillna(0).ne(0)].dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(columns=list(group_cols) + ["NonZero_Users"])
    return d.groupby(group_cols, as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})


def _totals_row(label: str, df: pd.DataFrame, cols: List[str], nonzero_users: Optional[int] = None) -> Dict:
    row = {df.columns[0]: label}
    for c in cols:
        row[c] = float(pd.to_numeric(df[c], errors="coerce").fillna(0).sum()) if c in df.columns else 0.0
    if nonzero_users is not None:
        row["NonZero_Users"] = int(nonzero_users)
    return row


def compute_report_from_dfs(
    leads_raw: pd.DataFrame,
    payments_raw: pd.DataFrame,
    refunds_raw: pd.DataFrame,
    exclude_owner_value: str = "Pipedrive KrispCall",
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Pure logic. Returns dict of named DataFrames + logs."""
    logs: List[str] = []

    # Expand leads for multiple emails
    person_email_col = _pick_col(leads_raw, ["Person - Email"])
    user_email_col = _pick_col(leads_raw, ["Lead - User Email"])
    email_cols_priority = [c for c in [person_email_col, user_email_col] if c]

    expanded_leads, missing_rows = expand_leads_for_multiple_emails(leads_raw, email_cols_priority)
    if missing_rows:
        logs.append("Missing email rows (Excel row numbers):")
        logs.append(", ".join(map(str, missing_rows[:500])))
        logs.append(f"Total missing email rows: {len(missing_rows)}")

    # Labels + Connected
    label_col = _pick_col(expanded_leads, ["Lead - Label", "Label", "Labels"])
    expanded_leads["labels_list"] = expanded_leads[label_col].apply(_split_labels) if label_col else [[]] * len(expanded_leads)
    expanded_leads["Connected"] = expanded_leads["labels_list"].apply(_connected_from_labels)

    owner_col = _pick_col(expanded_leads, ["Lead - Owner", "Deal - Owner", "Owner", "owner"]) or "Lead - Owner"
    if owner_col not in expanded_leads.columns:
        expanded_leads[owner_col] = "Unknown"

    # Parse and dedupe exports
    payments = payments_raw.copy()
    refunds = refunds_raw.copy()

    if not payments.empty and "_dt" not in payments.columns and "time" in payments.columns:
        payments["_dt"] = _parse_time_to_dt(payments["time"])
    if not refunds.empty and "_dt" not in refunds.columns and "time" in refunds.columns:
        refunds["_dt"] = _parse_time_to_dt(refunds["time"])

    if not payments.empty:
        payments = dedupe_mixpanel_export(payments)
    if not refunds.empty:
        refunds = dedupe_mixpanel_export(refunds)

    # Resolve emails from mixpanel exports
    if not payments.empty:
        payments["email"] = _resolve_event_email(payments)
    else:
        payments["email"] = None

    if not refunds.empty:
        refunds["email"] = _resolve_event_email(refunds)
    else:
        refunds["email"] = None

    # For efficiency: only events for leads emails (including excluded owner, since main tables keep all)
    lead_emails_all = set(expanded_leads["email"].dropna().unique())
    payments = payments[payments["email"].isin(lead_emails_all)].copy() if not payments.empty else payments
    refunds = refunds[refunds["email"].isin(lead_emails_all)].copy() if not refunds.empty else refunds

    # Columns
    amount_col = _pick_col(payments, ["Amount", "amount", "Amount Paid"])
    desc_col = _pick_col(payments, ["Amount Description", "amount description", "description", "Plan"])

    refund_amount_col = _pick_col(refunds, ["Refund Amount", "refund_amount", "Amount", "amount"])
    refund_desc_col = _pick_col(refunds, ["Refunded Transaction description", "Refunded Transaction Description", "Refunded Transaction", "Refunded transaction description"])

    if not amount_col:
        raise RuntimeError("Could not find payment amount column in Mixpanel export (expected 'Amount').")
    if not refund_amount_col:
        refunds = refunds.copy()
        refunds["Refund Amount"] = 0.0
        refund_amount_col = "Refund Amount"

    payments[amount_col] = pd.to_numeric(payments[amount_col], errors="coerce").fillna(0.0) if not payments.empty else 0.0
    refunds[refund_amount_col] = pd.to_numeric(refunds[refund_amount_col], errors="coerce").fillna(0.0) if not refunds.empty else 0.0

    # Credit excluded frames
    payments_ce = filter_credit_excluded(payments, desc_col)
    refunds_ce = filter_credit_excluded(refunds, refund_desc_col)

    # Windowed payment summary (gross + credit excluded)
    pay_summary = windowed_payments_dual(payments, payments_ce, amount_col, desc_col, days=7)

    # Refund summaries (not windowed, by design)
    ref_summary = _sum_refunds(refunds, "email", refund_amount_col, "Refund_Amount")
    ref_summary_ce = _sum_refunds(refunds_ce, "email", refund_amount_col, "Refund_Amount_creditExcluded")

    # Earliest event date
    earliest_df = _earliest_event_date(payments, refunds)

    # Merge to email summary (for emails that appear in summaries)
    summary = pay_summary.merge(ref_summary, on="email", how="outer").merge(ref_summary_ce, on="email", how="outer").merge(earliest_df, on="email", how="outer")

    for c in ["Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded"]:
        if c not in summary.columns:
            summary[c] = 0.0
        summary[c] = pd.to_numeric(summary[c], errors="coerce").fillna(0.0)

    summary["First_Subscription"] = summary.get("First_Subscription", "FALSE").fillna("FALSE")
    summary["First_Payment_Date"] = summary.get("First_Payment_Date")

    summary["Net_Amount"] = summary["Total_Amount"] - summary["Refund_Amount"]
    summary["Net_Amount_creditExcluded"] = summary["Total_Amount_creditExcluded"] - summary["Refund_Amount_creditExcluded"]

    # Column order (Net first)
    email_summary_cols = [
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
    for c in email_summary_cols:
        if c not in summary.columns:
            summary[c] = None

    # Join back to leads for main tables (preserve all)
    joined = expanded_leads.merge(summary[email_summary_cols], on="email", how="left")
    for c in [
        "Net_Amount",
        "Net_Amount_creditExcluded",
        "Total_Amount",
        "Total_Amount_creditExcluded",
        "Refund_Amount",
        "Refund_Amount_creditExcluded",
    ]:
        joined[c] = pd.to_numeric(joined[c], errors="coerce").fillna(0.0)

    joined["First_Subscription"] = joined["First_Subscription"].fillna("FALSE")

    # Stringify labels_list for export sheet later
    joined["labels_list"] = joined["labels_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")

    # Main tables column order
    lead_cols_rest = [c for c in joined.columns if c not in email_summary_cols and c != "labels_list"]
    main_cols = [
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
    ] + lead_cols_rest + ["labels_list"]
    joined_export = joined.loc[:, [c for c in main_cols if c in joined.columns]].copy()

    joined_nonzero_export = joined_export[joined_export["Total_Amount"].fillna(0).ne(0)].copy()

    # ----------------------------
    # Summaries base: exclude owner for summaries (but do NOT remove from main tables)
    # ----------------------------
    exclude_owner_val = str(exclude_owner_value).strip().lower()
    leads_for_sum = joined_export.copy()
    leads_for_sum["__owner_norm"] = leads_for_sum[owner_col].astype(str).str.strip().str.lower()
    leads_for_sum = leads_for_sum[leads_for_sum["__owner_norm"].ne(exclude_owner_val)].copy()
    leads_for_sum = leads_for_sum.drop(columns=["__owner_norm"], errors="ignore")

    # Duplicates (post exclusion, before dedupe)
    dup_emails = leads_for_sum["email"].dropna().value_counts()
    dup_emails = dup_emails[dup_emails > 1].index.tolist()
    duplicates_df = leads_for_sum[leads_for_sum["email"].isin(dup_emails)].copy()

    # Dedup for summaries: keep lowest Lead - Lead created on
    leads_for_sum, created_col = _parse_lead_created(leads_for_sum)
    if created_col:
        leads_for_sum = leads_for_sum.sort_values(created_col, kind="mergesort")
        leads_unique = leads_for_sum.dropna(subset=["email"]).drop_duplicates(subset=["email"], keep="first").copy()
    else:
        # fallback: first occurrence
        leads_unique = leads_for_sum.dropna(subset=["email"]).drop_duplicates(subset=["email"], keep="first").copy()

    # Emails used in summaries
    emails_in_summaries = set(leads_unique["email"].dropna().unique())

    email_summary = summary[summary["email"].isin(emails_in_summaries)][email_summary_cols].copy()
    email_summary = email_summary.sort_values("Net_Amount", ascending=False)

    # Owner Summary
    owner_summary = (
        leads_unique.groupby(owner_col, as_index=False)[
            [
                "Net_Amount",
                "Net_Amount_creditExcluded",
                "Total_Amount",
                "Total_Amount_creditExcluded",
                "Refund_Amount",
                "Refund_Amount_creditExcluded",
            ]
        ]
        .sum()
        .rename(columns={owner_col: "Lead - Owner"})
    )
    owner_counts = _nonzero_users_count(leads_unique.rename(columns={owner_col: "Lead - Owner"}), ["Lead - Owner"])
    owner_summary = owner_summary.merge(owner_counts, on="Lead - Owner", how="left").fillna({"NonZero_Users": 0})
    owner_summary = owner_summary.sort_values("Net_Amount", ascending=False)

    # TOTAL row
    owner_totals = _totals_row(
        "TOTAL",
        owner_summary,
        [
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
        ],
        nonzero_users=int(owner_summary["NonZero_Users"].sum()) if "NonZero_Users" in owner_summary.columns else None,
    )
    owner_summary_with_total = pd.concat([owner_summary, pd.DataFrame([owner_totals])], ignore_index=True)

    # Owner x Connected
    owner_x_connected = (
        leads_unique.groupby([owner_col, "Connected"], as_index=False)[
            [
                "Net_Amount",
                "Net_Amount_creditExcluded",
                "Total_Amount",
                "Total_Amount_creditExcluded",
                "Refund_Amount",
                "Refund_Amount_creditExcluded",
            ]
        ]
        .sum()
        .rename(columns={owner_col: "Lead - Owner"})
    )
    owner_conn_counts = _nonzero_users_count(leads_unique.rename(columns={owner_col: "Lead - Owner"}), ["Lead - Owner", "Connected"])
    owner_x_connected = owner_x_connected.merge(owner_conn_counts, on=["Lead - Owner", "Connected"], how="left").fillna({"NonZero_Users": 0})
    owner_x_connected = owner_x_connected.sort_values("Net_Amount", ascending=False)

    owner_x_total = {
        "Lead - Owner": "TOTAL",
        "Connected": None,
        "Net_Amount": float(owner_x_connected["Net_Amount"].sum()),
        "Net_Amount_creditExcluded": float(owner_x_connected["Net_Amount_creditExcluded"].sum()),
        "Total_Amount": float(owner_x_connected["Total_Amount"].sum()),
        "Total_Amount_creditExcluded": float(owner_x_connected["Total_Amount_creditExcluded"].sum()),
        "Refund_Amount": float(owner_x_connected["Refund_Amount"].sum()),
        "Refund_Amount_creditExcluded": float(owner_x_connected["Refund_Amount_creditExcluded"].sum()),
        "NonZero_Users": int(owner_x_connected["NonZero_Users"].sum()),
    }
    owner_x_connected = pd.concat([owner_x_connected, pd.DataFrame([owner_x_total])], ignore_index=True)

    # Connected Summary
    connected_summary = (
        leads_unique.groupby("Connected", as_index=False)[
            [
                "Net_Amount",
                "Net_Amount_creditExcluded",
                "Total_Amount",
                "Total_Amount_creditExcluded",
                "Refund_Amount",
                "Refund_Amount_creditExcluded",
            ]
        ]
        .sum()
    )
    conn_counts = _nonzero_users_count(leads_unique, ["Connected"])
    connected_summary = connected_summary.merge(conn_counts, on="Connected", how="left").fillna({"NonZero_Users": 0})
    connected_summary = connected_summary.sort_values("Net_Amount", ascending=False)

    conn_total = {
        "Connected": "TOTAL",
        "Net_Amount": float(connected_summary["Net_Amount"].sum()),
        "Net_Amount_creditExcluded": float(connected_summary["Net_Amount_creditExcluded"].sum()),
        "Total_Amount": float(connected_summary["Total_Amount"].sum()),
        "Total_Amount_creditExcluded": float(connected_summary["Total_Amount_creditExcluded"].sum()),
        "Refund_Amount": float(connected_summary["Refund_Amount"].sum()),
        "Refund_Amount_creditExcluded": float(connected_summary["Refund_Amount_creditExcluded"].sum()),
        "NonZero_Users": int(connected_summary["NonZero_Users"].sum()),
    }
    connected_summary = pd.concat([connected_summary, pd.DataFrame([conn_total])], ignore_index=True)

    # Label Summary
    label_col_in_leads = _pick_col(leads_unique, ["Lead - Label", "Label", "Labels"])
    labs_base = leads_unique.copy()
    if label_col_in_leads and label_col_in_leads in labs_base.columns:
        labs_base["labels_list"] = labs_base[label_col_in_leads].apply(_split_labels)
    else:
        # fallback: use stringified labels_list column if present
        if "labels_list" in labs_base.columns:
            labs_base["labels_list"] = labs_base["labels_list"].apply(lambda s: _split_labels(s))
        else:
            labs_base["labels_list"] = [[]] * len(labs_base)

    labels_expanded = labs_base[[
        "email",
        "Net_Amount",
        "Net_Amount_creditExcluded",
        "Total_Amount",
        "Total_Amount_creditExcluded",
        "Refund_Amount",
        "Refund_Amount_creditExcluded",
        "labels_list",
    ]].explode("labels_list").rename(columns={"labels_list": "Label"})

    labels_expanded["Label"] = labels_expanded["Label"].fillna("").astype(str).str.strip()
    labels_expanded = labels_expanded[labels_expanded["Label"] != ""].copy()
    labels_expanded["Connected_Label"] = labels_expanded["Label"].str.lower().apply(lambda s: False if s.strip() == "not connected" else ("connected" in s))

    label_summary = labels_expanded.groupby(["Label", "Connected_Label"], as_index=False)[
        [
            "Net_Amount",
            "Net_Amount_creditExcluded",
            "Total_Amount",
            "Total_Amount_creditExcluded",
            "Refund_Amount",
            "Refund_Amount_creditExcluded",
        ]
    ].sum()

    label_counts = (
        labels_expanded[labels_expanded["Total_Amount"].fillna(0).ne(0)]
        .dropna(subset=["email"])
        .groupby(["Label", "Connected_Label"], as_index=False)["email"]
        .nunique()
        .rename(columns={"email": "NonZero_Users"})
    )
    label_summary = label_summary.merge(label_counts, on=["Label", "Connected_Label"], how="left").fillna({"NonZero_Users": 0})
    label_summary = label_summary.sort_values("Net_Amount", ascending=False)

    label_total = {
        "Label": "TOTAL",
        "Connected_Label": None,
        "Net_Amount": float(label_summary["Net_Amount"].sum()),
        "Net_Amount_creditExcluded": float(label_summary["Net_Amount_creditExcluded"].sum()),
        "Total_Amount": float(label_summary["Total_Amount"].sum()),
        "Total_Amount_creditExcluded": float(label_summary["Total_Amount_creditExcluded"].sum()),
        "Refund_Amount": float(label_summary["Refund_Amount"].sum()),
        "Refund_Amount_creditExcluded": float(label_summary["Refund_Amount_creditExcluded"].sum()),
        "NonZero_Users": int(label_summary["NonZero_Users"].sum()),
    }
    label_summary = pd.concat([label_summary, pd.DataFrame([label_total])], ignore_index=True)

    # Time Summary: group by Lead created date (NOT payment date)
    time_summary = pd.DataFrame(columns=[
        "Lead_Created_Date",
        "Net_Amount",
        "Net_Amount_creditExcluded",
        "Total_Amount",
        "Total_Amount_creditExcluded",
        "Refund_Amount",
        "Refund_Amount_creditExcluded",
        "NonZero_Users",
    ])

    if created_col and created_col in leads_unique.columns:
        tmp = leads_unique.copy()
        tmp["Lead_Created_Date"] = pd.to_datetime(tmp[created_col], errors="coerce").dt.normalize()
        time_summary = tmp.groupby("Lead_Created_Date", as_index=False)[
            [
                "Net_Amount",
                "Net_Amount_creditExcluded",
                "Total_Amount",
                "Total_Amount_creditExcluded",
                "Refund_Amount",
                "Refund_Amount_creditExcluded",
            ]
        ].sum()
        time_counts = _nonzero_users_count(tmp.rename(columns={"Lead_Created_Date": "Lead_Created_Date"}), ["Lead_Created_Date"])
        # _nonzero_users_count expects Total_Amount and email present; tmp has.
        time_counts = tmp[tmp["Total_Amount"].fillna(0).ne(0)].dropna(subset=["email"]).groupby("Lead_Created_Date", as_index=False)["email"].nunique().rename(columns={"email": "NonZero_Users"})
        time_summary = time_summary.merge(time_counts, on="Lead_Created_Date", how="left").fillna({"NonZero_Users": 0})
        time_summary = time_summary.sort_values("Lead_Created_Date")

        time_total = {
            "Lead_Created_Date": "TOTAL",
            "Net_Amount": float(time_summary["Net_Amount"].sum()),
            "Net_Amount_creditExcluded": float(time_summary["Net_Amount_creditExcluded"].sum()),
            "Total_Amount": float(time_summary["Total_Amount"].sum()),
            "Total_Amount_creditExcluded": float(time_summary["Total_Amount_creditExcluded"].sum()),
            "Refund_Amount": float(time_summary["Refund_Amount"].sum()),
            "Refund_Amount_creditExcluded": float(time_summary["Refund_Amount_creditExcluded"].sum()),
            "NonZero_Users": int(time_summary["NonZero_Users"].sum()),
        }
        time_summary = pd.concat([time_summary, pd.DataFrame([time_total])], ignore_index=True)

    # ----------------------------
    # Self-converted logic (NEW): Workspace Subscription emails NOT in leads after exclusion
    # ----------------------------
    lead_emails_excl_owner = set(leads_for_sum["email"].dropna().unique())

    self_emails: set = set()
    if desc_col and desc_col in payments.columns and not payments.empty:
        ws_mask = payments[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
        ws_emails = set(payments.loc[ws_mask, "email"].dropna().unique())
        self_emails = set([e for e in ws_emails if e not in lead_emails_excl_owner])

    payments_self = payments[payments["email"].isin(self_emails)].copy() if self_emails and not payments.empty else pd.DataFrame()
    refunds_self = refunds[refunds["email"].isin(self_emails)].copy() if self_emails and not refunds.empty else pd.DataFrame()
    payments_self_ce = filter_credit_excluded(payments_self, desc_col)
    refunds_self_ce = filter_credit_excluded(refunds_self, refund_desc_col)

    self_pay = windowed_payments_dual(payments_self, payments_self_ce, amount_col, desc_col, days=7)
    self_ref = _sum_refunds(refunds_self, "email", refund_amount_col, "Refund_Amount")
    self_ref_ce = _sum_refunds(refunds_self_ce, "email", refund_amount_col, "Refund_Amount_creditExcluded")
    self_earliest = _earliest_event_date(payments_self, refunds_self)

    self_fact = self_pay.merge(self_ref, on="email", how="outer").merge(self_ref_ce, on="email", how="outer").merge(self_earliest, on="email", how="outer")
    for c in ["Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded"]:
        if c not in self_fact.columns:
            self_fact[c] = 0.0
        self_fact[c] = pd.to_numeric(self_fact[c], errors="coerce").fillna(0.0)
    self_fact["First_Subscription"] = self_fact.get("First_Subscription", "FALSE").fillna("FALSE")
    self_fact["Net_Amount"] = self_fact["Total_Amount"] - self_fact["Refund_Amount"]
    self_fact["Net_Amount_creditExcluded"] = self_fact["Total_Amount_creditExcluded"] - self_fact["Refund_Amount_creditExcluded"]

    # align columns to match Email_Summary
    for c in email_summary_cols:
        if c not in self_fact.columns:
            self_fact[c] = None
    self_fact = self_fact[email_summary_cols].copy()
    self_fact = self_fact.sort_values("Net_Amount", ascending=False)

    self_converted_contacts = int(len(set(self_fact["email"].dropna().unique())))
    self_converted_net = float(pd.to_numeric(self_fact["Net_Amount"], errors="coerce").fillna(0).sum())
    self_converted_net_ce = float(pd.to_numeric(self_fact["Net_Amount_creditExcluded"], errors="coerce").fillna(0).sum())

    # Sales attempt revenue totals from owner totals (after exclusion + dedup)
    sales_net = float(owner_totals["Net_Amount"]) if "Net_Amount" in owner_totals else 0.0
    sales_net_ce = float(owner_totals["Net_Amount_creditExcluded"]) if "Net_Amount_creditExcluded" in owner_totals else 0.0

    # Totals row for main tables (unique emails totals from owner totals)
    totals_for_main = {
        "email": "TOTAL (unique emails)",
        "Net_Amount": sales_net,
        "Net_Amount_creditExcluded": sales_net_ce,
        "Total_Amount": float(owner_totals["Total_Amount"]),
        "Total_Amount_creditExcluded": float(owner_totals["Total_Amount_creditExcluded"]),
        "Refund_Amount": float(owner_totals["Refund_Amount"]),
        "Refund_Amount_creditExcluded": float(owner_totals["Refund_Amount_creditExcluded"]),
    }

    joined_export = pd.concat([joined_export, pd.DataFrame([totals_for_main])], ignore_index=True)
    joined_nonzero_export = pd.concat([joined_nonzero_export, pd.DataFrame([totals_for_main])], ignore_index=True)

    # Output dict matching v3 sheet names
    dfs = {
        "Leads_with_Payments": joined_export,
        "Leads_Payments_NonZero": joined_nonzero_export,
        "Email_Summary": email_summary,
        "Owner_Summary": owner_summary_with_total,
        "Owner_x_Connected": owner_x_connected,
        "Connected_Summary": connected_summary,
        "Label_Summary": label_summary,
        "Time_Summary": time_summary,
        "Duplicate_Leads_By_Email": duplicates_df,
        "SelfConverted_Emails": self_fact,
        "__owner_col": pd.DataFrame({"owner_col": [owner_col]}),
        "__sales_self": pd.DataFrame(
            {
                "k": [
                    "Self_Converted_Contacts",
                    "Self_Converted_Net_Amount",
                    "Self_Converted_Net_Amount_creditExcluded",
                    "Sales_Net_Amount",
                    "Sales_Net_Amount_creditExcluded",
                ],
                "v": [
                    self_converted_contacts,
                    self_converted_net,
                    self_converted_net_ce,
                    sales_net,
                    sales_net_ce,
                ],
            }
        ),
    }
    return dfs, logs


# ----------------------------
# Excel builder (replicates v3 workbook structure)
# ----------------------------

def _auto_fit(ws, max_width: int = 55):
    # naive auto-fit using string lengths
    widths = {}
    for row in ws.iter_rows(values_only=True):
        for i, v in enumerate(row, 1):
            if v is None:
                continue
            s = str(v)
            widths[i] = max(widths.get(i, 0), len(s))
    for col_idx, w in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(10, w + 2), max_width)


def _format_header(ws, freeze: bool = True):
    header_fill = PatternFill("solid", fgColor="EEE6FF")
    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # style first row
    for cell in ws[1]:
        cell.font = bold
        cell.fill = header_fill
        cell.alignment = center

    if freeze:
        ws.freeze_panes = "A2"


def _write_df(ws, df: pd.DataFrame):
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)


def build_excel(dfs: Dict[str, pd.DataFrame], logs: List[str], from_date: date, to_date: date) -> Tuple[str, bytes]:
    wb = Workbook()

    order = [
        "Leads_with_Payments",
        "Leads_Payments_NonZero",
        "Email_Summary",
        "Owner_Summary",
        "Owner_x_Connected",
        "Connected_Summary",
        "Label_Summary",
        "Time_Summary",
        "Duplicate_Leads_By_Email",
        "SelfConverted_Emails",
        "Logs",
    ]

    # Create sheets
    first = True
    for name in order:
        if name == "Logs":
            continue
        df = dfs.get(name, pd.DataFrame())
        ws = wb.active if first else wb.create_sheet(name)
        ws.title = name
        first = False

        _write_df(ws, df)
        _format_header(ws, freeze=True)
        _auto_fit(ws)

        # Special: Owner_Summary needs extra blocks below
        if name == "Owner_Summary":
            # find current end
            last_row = ws.max_row
            ws.append([])
            ws.append([])

            ws.append(["Self-Converted Revenue (workspace subscription emails not in leads after excluding Pipedrive KrispCall)"])
            ws.append(["Self_Converted_Contacts", int(dfs["__sales_self"].loc[dfs["__sales_self"]["k"] == "Self_Converted_Contacts", "v"].iloc[0])])
            ws.append(["Self_Converted_Net_Amount", float(dfs["__sales_self"].loc[dfs["__sales_self"]["k"] == "Self_Converted_Net_Amount", "v"].iloc[0])])
            ws.append(["Self_Converted_Net_Amount_creditExcluded", float(dfs["__sales_self"].loc[dfs["__sales_self"]["k"] == "Self_Converted_Net_Amount_creditExcluded", "v"].iloc[0])])
            ws.append([])
            ws.append(["Sales Attempt Revenue (leads excluding Pipedrive KrispCall)"])
            ws.append(["Sales_Net_Amount", float(dfs["__sales_self"].loc[dfs["__sales_self"]["k"] == "Sales_Net_Amount", "v"].iloc[0])])
            ws.append(["Sales_Net_Amount_creditExcluded", float(dfs["__sales_self"].loc[dfs["__sales_self"]["k"] == "Sales_Net_Amount_creditExcluded", "v"].iloc[0])])

            # format the labels
            for r in range(last_row + 3, ws.max_row + 1):
                ws.cell(row=r, column=1).font = Font(bold=True) if r in [last_row + 3, last_row + 8] else Font(bold=False)

            _auto_fit(ws)

    # Logs sheet
    ws_log = wb.create_sheet("Logs")
    if logs:
        for line in logs:
            ws_log.append([line])
    else:
        ws_log.append(["No issues logged."])

    # Freeze logs header not needed
    _auto_fit(ws_log)

    out = BytesIO()
    wb.save(out)

    fname = f"payment_summary_{from_date.strftime('%b%d').lower()}_{to_date.strftime('%b%d').lower()}.xlsx"
    return fname, out.getvalue()


# ----------------------------
# Streamlit main
# ----------------------------

def main():
    st.set_page_config(page_title="KrispCall Payment Summary", page_icon="📈", layout="wide")
    require_login()
    _inject_brand_css()

    st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
    l, r = st.columns([1, 3], vertical_alignment="center")
    with l:
        st.markdown(_logo_html(width_px=240, top_pad_px=12), unsafe_allow_html=True)
    with r:
        st.markdown('<div class="kc-hero"><h1>KrispCall Payment Summary</h1><p>Leads reconciliation with Mixpanel transactions</p></div>', unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Date Selection")
        today = date.today()
        first_of_month = today.replace(day=1)
        # Example requirement: Jan -> Dec 28
        default_start = first_of_month - timedelta(days=4)
        default_end = today - timedelta(days=1)

        from_date = st.date_input("Date from", value=default_start)
        to_date = st.date_input("Date to", value=default_end)

        st.markdown("---")
        exclude_owner_ui = st.text_input("Exclude owner from summaries", value="Pipedrive KrispCall")
        st.caption("Summaries exclude this owner. Main tables keep all rows.")

    st.markdown('<div class="kc-card">', unsafe_allow_html=True)
    leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])
    run = st.button("Run Analysis", type="primary", disabled=(leads_file is None))
    st.markdown("</div>", unsafe_allow_html=True)

    if not run:
        st.stop()

    with st.spinner("Running analysis..."):
        leads_raw = pd.read_csv(leads_file)

        pid = int(_get_secret(["mixpanel", "project_id"]))
        base = _get_secret(["mixpanel", "base_url"], "https://data-eu.mixpanel.com")

        payments_raw = fetch_mixpanel_event_export(pid, base, from_date, to_date, "New Payment Made")

        window_days = (to_date - from_date).days
        refund_from = (to_date - timedelta(days=60)) if window_days < 60 else from_date
        refunds_raw = fetch_mixpanel_event_export(pid, base, refund_from, to_date, "Refund Granted")

        dfs, logs = compute_report_from_dfs(
            leads_raw=leads_raw,
            payments_raw=payments_raw,
            refunds_raw=refunds_raw,
            exclude_owner_value=exclude_owner_ui,
        )

        fname, excel_bytes = build_excel(dfs, logs, from_date, to_date)

    # UI tabs
    tab_main, tab_summ, tab_time, tab_self, tab_dups, tab_export, tab_logs = st.tabs(
        ["Main Tables", "Summaries", "Time", "Self-Converted", "Duplicates", "Export", "Logs"]
    )

    with tab_main:
        st.markdown("#### Leads with payments")
        st.dataframe(dfs["Leads_with_Payments"], use_container_width=True)
        st.markdown("#### Leads with non-zero payments only")
        st.dataframe(dfs["Leads_Payments_NonZero"], use_container_width=True)

    with tab_summ:
        st.markdown("#### Owner Summary")
        st.dataframe(dfs["Owner_Summary"], use_container_width=True)
        st.markdown("#### Owner x Connected")
        st.dataframe(dfs["Owner_x_Connected"], use_container_width=True)
        st.markdown("#### Connected Summary")
        st.dataframe(dfs["Connected_Summary"], use_container_width=True)
        st.markdown("#### Label Summary")
        st.dataframe(dfs["Label_Summary"], use_container_width=True)
        st.markdown("#### Email Summary")
        st.dataframe(dfs["Email_Summary"], use_container_width=True)

    with tab_time:
        st.markdown("#### Time Summary (grouped by Lead . Lead created on date)")
        st.dataframe(dfs["Time_Summary"], use_container_width=True)

    with tab_self:
        st.markdown("#### SelfConverted_Emails")
        st.dataframe(dfs["SelfConverted_Emails"], use_container_width=True)

    with tab_dups:
        st.markdown("#### Duplicate leads by email (after excluding owner, before dedupe)")
        st.dataframe(dfs["Duplicate_Leads_By_Email"], use_container_width=True)

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
