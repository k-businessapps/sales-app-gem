import base64
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Font
from openpyxl.utils.dataframe import dataframe_to_rows


def _normalize_email(x: object) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().lower()
    if not s or s in {"nan", "none", "null"}:
        return None
    return s


def _pick_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _pick_any_column_containing(df: pd.DataFrame, needle: str) -> Optional[str]:
    if df is None or df.empty:
        return None
    needle = needle.lower()
    for c in df.columns:
        if needle in str(c).lower():
            return c
    return None


def _split_emails(cell: object) -> List[str]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = str(cell).strip()
    if not s:
        return []
    parts = re.split(r"[,\n;|\s]+", s)
    out = []
    for p in parts:
        e = _normalize_email(p)
        if e and "@" in e:
            out.append(e)
    seen = set()
    uniq = []
    for e in out:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq


def _ensure_dt_from_mixpanel_time(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "_dt" not in df.columns:
        if "time" in df.columns:
            df["_dt"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
        else:
            df["_dt"] = pd.NaT
    return df


def _date_in_range(dt_series: pd.Series, from_d: date, to_d: date) -> pd.Series:
    d = pd.to_datetime(dt_series, utc=True, errors="coerce")
    dd = d.dt.date
    return (dd >= from_d) & (dd <= to_d)


def _dedupe_mixpanel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if "time" in df.columns:
        df["_time_s"] = pd.to_numeric(df["time"], errors="coerce").fillna(0).astype(int)
    else:
        df["_time_s"] = 0

    insert_id_col = "$insert_id" if "$insert_id" in df.columns else None
    if insert_id_col is None:
        insert_id_col = "insert_id" if "insert_id" in df.columns else None

    key_cols = ["event", "distinct_id", "_time_s"]
    if insert_id_col:
        key_cols.append(insert_id_col)

    sort_cols = []
    if "mp_processing_time_ms" in df.columns:
        sort_cols.append("mp_processing_time_ms")
    sort_cols.append("_time_s")

    df2 = df.sort_values(sort_cols, kind="mergesort").copy()
    df2 = df2.drop_duplicates(subset=key_cols, keep="last").copy()
    df2 = df2.drop(columns=["_time_s"], errors="ignore")
    return df2


_CREDIT_EXCLUDED_EXACT = {"purchased credit", "credit purchased", "amount recharged"}


def _filter_credit_excluded(df: pd.DataFrame, desc_col: Optional[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if not desc_col or desc_col not in df.columns:
        return df.copy()

    def norm(s: object) -> str:
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return ""
        return str(s).strip().lower()

    keep_mask = ~df[desc_col].map(norm).isin(_CREDIT_EXCLUDED_EXACT)
    return df.loc[keep_mask].copy()


def _add_totals_row(df: pd.DataFrame, label_col: Optional[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df2 = df.copy()
    num_cols = [c for c in df2.columns if pd.api.types.is_numeric_dtype(df2[c])]
    totals = {c: pd.to_numeric(df2[c], errors="coerce").fillna(0).sum() for c in num_cols}
    row = {c: "" for c in df2.columns}
    if label_col and label_col in df2.columns:
        row[label_col] = "TOTAL"
    for k, v in totals.items():
        row[k] = float(v)
    return pd.concat([df2, pd.DataFrame([row])], ignore_index=True)


def _style_totals_row(df: pd.DataFrame, label_col: Optional[str]):
    if df is None or df.empty:
        return df
    if label_col and label_col in df.columns:
        def styler(row):
            if str(row[label_col]).strip().upper() == "TOTAL":
                return ["font-weight: 700"] * len(row)
            return [""] * len(row)
        return df.style.apply(styler, axis=1)
    return df


def _to_float(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _mixpanel_export(
    base_url: str,
    auth_header: str,
    project_id: str,
    events: List[str],
    from_date: date,
    to_date: date,
) -> pd.DataFrame:
    # Mixpanel Raw Export API endpoint.
    # US: https://data.mixpanel.com/api/2.0/export
    # EU: https://data-eu.mixpanel.com/api/2.0/export
    url = f"{base_url.rstrip('/')}/api/2.0/export"

    params = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps(events),
    }
    # project_id is optional for the Export API. Mixpanel typically infers project from the API Secret used for auth.
    if str(project_id).strip():
        params["project_id"] = str(project_id).strip()

    headers = {"Authorization": auth_header}

    r = requests.get(url, params=params, headers=headers, timeout=180)

    if not r.ok:
        # Provide a helpful error message instead of a raw HTTPError traceback.
        snippet = (r.text or "").strip().replace("\n", " ")
        snippet = snippet[:800]
        raise RuntimeError(
            "Mixpanel Export API request failed."
            f" HTTP {r.status_code}."
            f" Endpoint: {url}."
            f" Response: {snippet}"
        )

    rows = []
    for line in r.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        event_name = obj.get("event")
        props = obj.get("properties", {}) or {}
        flat = {"event": event_name}
        flat.update(props)
        if "distinct_id" not in flat and "distinct_id" in obj:
            flat["distinct_id"] = obj["distinct_id"]
        if "time" not in flat and "time" in obj:
            flat["time"] = obj["time"]
        rows.append(flat)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = _dedupe_mixpanel(df)
    df = _ensure_dt_from_mixpanel_time(df)

    email_col = (
        _pick_first_existing_column(df, ["User Email", "user_email", "email", "Email", "Person - Email", "Lead - User Email"])
        or _pick_any_column_containing(df, "email")
    )
    df["email"] = df[email_col].map(_normalize_email) if email_col else None
    return df

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
    d = payments_gross.dropna(subset=["email"]).copy()
    d = _ensure_dt_from_mixpanel_time(d)

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

    payments_ce2 = payments_ce.dropna(subset=["email"]).copy() if payments_ce is not None else pd.DataFrame()
    refunds_gross2 = refunds_gross.dropna(subset=["email"]).copy() if refunds_gross is not None else pd.DataFrame()
    refunds_ce2 = refunds_ce.dropna(subset=["email"]).copy() if refunds_ce is not None else pd.DataFrame()

    payments_ce2 = _ensure_dt_from_mixpanel_time(payments_ce2)
    refunds_gross2 = _ensure_dt_from_mixpanel_time(refunds_gross2)
    refunds_ce2 = _ensure_dt_from_mixpanel_time(refunds_ce2)

    ce_map = {e: g.sort_values("_dt", kind="mergesort") for e, g in payments_ce2.groupby("email", sort=False)} if not payments_ce2.empty else {}
    ref_map = {e: g.sort_values("_dt", kind="mergesort") for e, g in refunds_gross2.groupby("email", sort=False)} if not refunds_gross2.empty else {}
    ref_ce_map = {e: g.sort_values("_dt", kind="mergesort") for e, g in refunds_ce2.groupby("email", sort=False)} if not refunds_ce2.empty else {}

    out = []
    for email, g in d.groupby("email", sort=False):
        g = g.sort_values("_dt", kind="mergesort")

        start_dt = None
        first_subscription = False
        if desc_col and desc_col in g.columns:
            mask_sub = g[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
            if mask_sub.any():
                start_dt = g.loc[mask_sub, "_dt"].min()
                first_subscription = True
        if start_dt is None:
            start_dt = g["_dt"].min()
        if pd.isna(start_dt):
            continue

        end_dt = start_dt + timedelta(days=days)

        gross_mask = (g["_dt"] >= start_dt) & (g["_dt"] <= end_dt)
        gross_total = float(g.loc[gross_mask, amount_col].sum())
        gross_txn = int(g.loc[gross_mask, amount_col].shape[0])

        g_ce = ce_map.get(email)
        if g_ce is not None and not g_ce.empty:
            g_ce[amount_col] = pd.to_numeric(g_ce[amount_col], errors="coerce").fillna(0.0)
            ce_mask = (g_ce["_dt"] >= start_dt) & (g_ce["_dt"] <= end_dt)
            ce_total = float(g_ce.loc[ce_mask, amount_col].sum())
            ce_txn = int(g_ce.loc[ce_mask, amount_col].shape[0])
        else:
            ce_total = 0.0
            ce_txn = 0

        g_ref = ref_map.get(email)
        if g_ref is not None and not g_ref.empty:
            g_ref[refund_amount_col] = pd.to_numeric(g_ref[refund_amount_col], errors="coerce").fillna(0.0)
            ref_mask = (g_ref["_dt"] >= start_dt) & (g_ref["_dt"] <= end_dt)
            ref_total = float(g_ref.loc[ref_mask, refund_amount_col].sum())
        else:
            ref_total = 0.0

        g_ref_ce = ref_ce_map.get(email)
        if g_ref_ce is not None and not g_ref_ce.empty:
            g_ref_ce[refund_amount_col] = pd.to_numeric(g_ref_ce[refund_amount_col], errors="coerce").fillna(0.0)
            ref_ce_mask = (g_ref_ce["_dt"] >= start_dt) & (g_ref_ce["_dt"] <= end_dt)
            ref_ce_total = float(g_ref_ce.loc[ref_ce_mask, refund_amount_col].sum())
        else:
            ref_ce_total = 0.0

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
                "First_Subscription": bool(first_subscription),
                "First_Payment_Date": start_dt.date().isoformat(),
            }
        )

    return pd.DataFrame(out)


def _build_summaries(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if df is None or df.empty:
        return {
            "owner_summary": pd.DataFrame(),
            "connected_summary": pd.DataFrame(),
            "owner_x_connected": pd.DataFrame(),
            "label_summary": pd.DataFrame(),
            "time_summary": pd.DataFrame(),
        }

    metric_cols = [
        "Total_Amount",
        "Refund_Amount",
        "Net_Amount",
        "Total_Amount_creditExcluded",
        "Refund_Amount_creditExcluded",
        "Net_Amount_creditExcluded",
        "Transactions",
        "Transactions_creditExcluded",
    ]

    for c in metric_cols:
        if c not in df.columns:
            df[c] = 0.0 if "Transactions" not in c else 0

    owner_summary = (
        df.groupby("Owner", dropna=False, sort=False)
        .agg(
            Users=("email", "nunique"),
            NonZero_Users=("email", lambda s: df.loc[s.index, "Total_Amount"].fillna(0).ne(0).sum()),
            **{c: (c, "sum") for c in metric_cols},
        )
        .reset_index()
    )

    connected_summary = (
        df.groupby("Connected", dropna=False, sort=False)
        .agg(
            Users=("email", "nunique"),
            NonZero_Users=("email", lambda s: df.loc[s.index, "Total_Amount"].fillna(0).ne(0).sum()),
            **{c: (c, "sum") for c in metric_cols},
        )
        .reset_index()
    )

    owner_x_connected = (
        df.groupby(["Owner", "Connected"], dropna=False, sort=False)
        .agg(
            Users=("email", "nunique"),
            NonZero_Users=("email", lambda s: df.loc[s.index, "Total_Amount"].fillna(0).ne(0).sum()),
            **{c: (c, "sum") for c in metric_cols},
        )
        .reset_index()
    )

    label_df = df.copy()
    if "labels_list" not in label_df.columns:
        label_df["labels_list"] = [[] for _ in range(len(label_df))]
    label_df = label_df.explode("labels_list")
    label_df["Label"] = label_df["labels_list"].fillna("").astype(str).str.strip()
    label_df = label_df[label_df["Label"].ne("")]
    label_summary = (
        label_df.groupby(["Label", "Connected"], dropna=False, sort=False)
        .agg(
            Users=("email", "nunique"),
            NonZero_Users=("email", lambda s: label_df.loc[s.index, "Total_Amount"].fillna(0).ne(0).sum()),
            **{c: (c, "sum") for c in metric_cols},
        )
        .reset_index()
    )

    if "Lead_Created_Date" in df.columns:
        time_summary = (
            df.groupby("Lead_Created_Date", dropna=False, sort=False)
            .agg(
                Users=("email", "nunique"),
                NonZero_Users=("email", lambda s: df.loc[s.index, "Total_Amount"].fillna(0).ne(0).sum()),
                **{c: (c, "sum") for c in metric_cols},
            )
            .reset_index()
            .sort_values("Lead_Created_Date", kind="mergesort")
        )
    else:
        time_summary = pd.DataFrame()

    return {
        "owner_summary": owner_summary,
        "connected_summary": connected_summary,
        "owner_x_connected": owner_x_connected,
        "label_summary": label_summary,
        "time_summary": time_summary,
    }


def _plot_owner(owner_summary: pd.DataFrame):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if owner_summary is None or owner_summary.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.set_axis_off()
        return fig
    df = owner_summary.copy()
    df["Net_Amount"] = pd.to_numeric(df["Net_Amount"], errors="coerce").fillna(0.0)
    df = df.sort_values("Net_Amount", kind="mergesort", ascending=False)
    ax.bar(df["Owner"].astype(str), df["Net_Amount"].astype(float))
    ax.set_title("Net Revenue by Owner (Summaries Base)")
    ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.set_ylabel("Net Amount")
    fig.tight_layout()
    return fig


def _plot_owner_connected(owner_x_connected: pd.DataFrame):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if owner_x_connected is None or owner_x_connected.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.set_axis_off()
        return fig
    df = owner_x_connected.copy()
    df["Net_Amount"] = pd.to_numeric(df["Net_Amount"], errors="coerce").fillna(0.0)
    owners = list(df["Owner"].astype(str).unique())
    true_vals, false_vals = [], []
    for o in owners:
        sub = df[df["Owner"].astype(str) == o]
        t = sub[sub["Connected"] == True]["Net_Amount"].sum()
        f = sub[sub["Connected"] == False]["Net_Amount"].sum()
        true_vals.append(float(t))
        false_vals.append(float(f))
    x = range(len(owners))
    ax.bar(x, false_vals, label="Connected = False")
    ax.bar(x, true_vals, bottom=false_vals, label="Connected = True")
    ax.set_xticks(list(x))
    ax.set_xticklabels(owners, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Net Amount")
    ax.set_title("Net Revenue by Owner x Connected")
    ax.legend(fontsize=8)
    fig.tight_layout()
    return fig


def _plot_time(time_summary: pd.DataFrame):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if time_summary is None or time_summary.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.set_axis_off()
        return fig
    df = time_summary.copy()
    df["Net_Amount"] = pd.to_numeric(df["Net_Amount"], errors="coerce").fillna(0.0)
    ax.plot(df["Lead_Created_Date"].astype(str), df["Net_Amount"].astype(float), marker="o")
    ax.set_title("Net Revenue by Lead Created Date")
    ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.set_ylabel("Net Amount")
    fig.tight_layout()
    return fig


def _write_df(ws, df: pd.DataFrame, bold_total: bool = True, total_label: str = "TOTAL"):
    if df is None or df.empty:
        ws.append(["(empty)"])
        return
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")
    if not bold_total:
        return
    max_row = ws.max_row
    for row_idx in range(2, max_row + 1):
        row_vals = [ws.cell(row=row_idx, column=c).value for c in range(1, ws.max_column + 1)]
        if any(str(v).strip().upper() == total_label for v in row_vals if v is not None):
            for c in range(1, ws.max_column + 1):
                ws.cell(row=row_idx, column=c).font = Font(bold=True)


def _fig_to_png_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _build_excel(
    *,
    owner_summary: pd.DataFrame,
    owner_x_connected: pd.DataFrame,
    connected_summary: pd.DataFrame,
    label_summary: pd.DataFrame,
    time_summary: pd.DataFrame,
    joined_export: pd.DataFrame,
    joined_nonzero_export: pd.DataFrame,
    email_summary: pd.DataFrame,
    duplicate_leads: pd.DataFrame,
    self_converted_emails_df: pd.DataFrame,
    logs_df: pd.DataFrame,
    fig_owner,
    fig_owner_conn,
    fig_time,
    extra_rows: List[Tuple[str, object]],
) -> Tuple[str, bytes]:
    wb = Workbook()
    default = wb.active
    wb.remove(default)

    ws_owner = wb.create_sheet("Owner_Summary")
    owner_df = _add_totals_row(owner_summary, label_col="Owner") if owner_summary is not None else owner_summary
    _write_df(ws_owner, owner_df, bold_total=True)

    ws_owner.append([])
    ws_owner.append(["Extra Metrics", "Value"])
    ws_owner["A" + str(ws_owner.max_row)].font = Font(bold=True)
    ws_owner["B" + str(ws_owner.max_row)].font = Font(bold=True)
    for k, v in extra_rows:
        ws_owner.append([k, v])

    sheets = [
        ("Owner_x_Connected", _add_totals_row(owner_x_connected, label_col="Owner") if owner_x_connected is not None else owner_x_connected),
        ("Connected_Summary", _add_totals_row(connected_summary, label_col="Connected") if connected_summary is not None else connected_summary),
        ("Label_Summary", _add_totals_row(label_summary, label_col="Label") if label_summary is not None else label_summary),
        ("Time_Summary", _add_totals_row(time_summary, label_col="Lead_Created_Date") if time_summary is not None else time_summary),
        ("Leads_with_Payments", joined_export),
        ("Leads_Payments_NonZero", joined_nonzero_export),
        ("Email_Summary", email_summary),
        ("Duplicate_Leads_By_Email", duplicate_leads),
        ("SelfConverted_Emails", self_converted_emails_df),
        ("Logs", logs_df),
    ]
    for title, df in sheets:
        ws = wb.create_sheet(title)
        _write_df(ws, df, bold_total=True)

    ws_charts = wb.create_sheet("Charts")
    ws_charts.append(["Charts"])
    ws_charts["A1"].font = Font(bold=True)
    try:
        for name, fig in [("Owner", fig_owner), ("Owner_x_Connected", fig_owner_conn), ("Time", fig_time)]:
            ws_charts.append([])
            ws_charts.append([name])
            ws_charts["A" + str(ws_charts.max_row)].font = Font(bold=True)
            img_bytes = _fig_to_png_bytes(fig)
            tmp = io.BytesIO(img_bytes)
            img = XLImage(tmp)
            img.anchor = f"A{ws_charts.max_row + 1}"
            ws_charts.add_image(img)
            for _ in range(5):
                ws_charts.append([])
    except Exception:
        ws_charts.append(["(Chart embedding failed in this environment. Data tables are exported correctly.)"])

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    name = f"krispcall_revenue_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return name, out.getvalue()


@dataclass
class AnalysisResults:
    joined_export: pd.DataFrame
    joined_nonzero_export: pd.DataFrame
    owner_summary: pd.DataFrame
    owner_x_connected: pd.DataFrame
    connected_summary: pd.DataFrame
    label_summary: pd.DataFrame
    time_summary: pd.DataFrame
    duplicate_leads: pd.DataFrame
    self_converted_emails_list: List[str]
    self_converted_emails_df: pd.DataFrame
    overall_revenue: float
    overall_refunds_selected_window: float
    overall_transactions: int
    sales_attempt_totals: Dict[str, float]
    sales_effort_connected_totals: Dict[str, float]
    self_converted_totals: Dict[str, float]
    logs: List[str]
    fig_owner: object
    fig_owner_conn: object
    fig_time: object
    excel_name: str
    excel_bytes: bytes


def _run_analysis(leads_bytes: bytes, from_date: date, to_date: date) -> AnalysisResults:
    logs: List[str] = []
    leads_raw = pd.read_csv(io.BytesIO(leads_bytes))

    owner_col = _pick_first_existing_column(leads_raw, ["Lead - Owner", "Deal - Owner", "Owner", "owner"])
    created_col = _pick_first_existing_column(leads_raw, ["Lead - Lead created on", "Lead created on", "Lead Created On", "created"])
    labels_col = _pick_first_existing_column(leads_raw, ["Lead - Label", "Labels", "Label", "lead_label"])
    email_col = _pick_first_existing_column(leads_raw, ["Person - Email", "Lead - User Email", "Email", "email"]) or _pick_any_column_containing(leads_raw, "email")
    if not email_col:
        raise RuntimeError("Could not find an email column in the uploaded leads CSV.")

    expanded_rows = []
    for _, row in leads_raw.iterrows():
        emails = _split_emails(row.get(email_col))
        if not emails:
            continue
        for e in emails:
            rr = row.to_dict()
            rr["email"] = e
            expanded_rows.append(rr)
    expanded_leads = pd.DataFrame(expanded_rows)
    if expanded_leads.empty:
        raise RuntimeError("No valid emails found after expanding the leads CSV.")

    expanded_leads["Owner"] = expanded_leads[owner_col].astype(str).fillna("") if owner_col else ""
    if created_col and created_col in expanded_leads.columns:
        expanded_leads["_lead_created_dt"] = pd.to_datetime(expanded_leads[created_col], errors="coerce")
        expanded_leads["Lead_Created_Date"] = expanded_leads["_lead_created_dt"].dt.date
    else:
        expanded_leads["_lead_created_dt"] = pd.NaT
        expanded_leads["Lead_Created_Date"] = pd.NaT

    if labels_col and labels_col in expanded_leads.columns:
        expanded_leads["labels_list"] = expanded_leads[labels_col].fillna("").astype(str).map(lambda s: [x.strip() for x in s.split(",") if x.strip()])
    else:
        expanded_leads["labels_list"] = [[] for _ in range(len(expanded_leads))]

    def is_connected(labels: List[str]) -> bool:
        lab = [str(x).strip().lower() for x in (labels or [])]
        if any(x == "not connected" for x in lab):
            return False
        if any("connected" in x for x in lab):
            return True
        return False
    expanded_leads["Connected"] = expanded_leads["labels_list"].map(is_connected)

    base_url = st.secrets.get("mixpanel", {}).get("base_url", "https://data-eu.mixpanel.com")
    project_id = str(st.secrets.get("mixpanel", {}).get("project_id", "")).strip()
    auth = str(st.secrets.get("mixpanel", {}).get("authorization", "")).strip()
    if not project_id or not auth:
        raise RuntimeError("Missing Mixpanel secrets. Expected st.secrets['mixpanel']['project_id'] and ['authorization'].")

    payments_all = _mixpanel_export(base_url, auth, project_id, ["New Payment Made"], from_date, to_date)
    days_span = (to_date - from_date).days + 1
    refund_from = from_date if days_span >= 60 else (to_date - timedelta(days=60))
    refunds_all = _mixpanel_export(base_url, auth, project_id, ["Refund Granted"], refund_from, to_date)

    amount_col = _pick_first_existing_column(payments_all, ["Amount", "amount", "payment_amount", "Payment Amount"])
    desc_col = _pick_first_existing_column(payments_all, ["Transaction Description", "transaction_description", "Description", "description", "Plan", "plan"])
    if not amount_col:
        raise RuntimeError("Could not find payment amount column in Mixpanel export (expected 'Amount' or similar).")
    refund_amount_col = _pick_first_existing_column(refunds_all, ["Refund Amount", "refund_amount", "Amount", "amount"])
    refund_desc_col = _pick_first_existing_column(refunds_all, ["Refund Description", "refund_description", "Description", "description", "Refunded Transaction Description"])
    if not refund_amount_col:
        refunds_all["Refund Amount"] = 0.0
        refund_amount_col = "Refund Amount"

    payments_all[amount_col] = pd.to_numeric(payments_all[amount_col], errors="coerce").fillna(0.0)
    refunds_all[refund_amount_col] = pd.to_numeric(refunds_all[refund_amount_col], errors="coerce").fillna(0.0)

    lead_emails = set(expanded_leads["email"].dropna().unique())
    payments_leads = payments_all[payments_all["email"].isin(lead_emails)].copy()
    refunds_leads = refunds_all[refunds_all["email"].isin(lead_emails)].copy()

    payments_ce_all = _filter_credit_excluded(payments_all, desc_col)
    refunds_ce_all = _filter_credit_excluded(refunds_all, refund_desc_col)
    payments_ce_leads = _filter_credit_excluded(payments_leads, desc_col)
    refunds_ce_leads = _filter_credit_excluded(refunds_leads, refund_desc_col)

    email_summary_leads = _windowed_email_summary(payments_leads, refunds_leads, payments_ce_leads, refunds_ce_leads, amount_col, desc_col, refund_amount_col, 7)

    leads_joined = expanded_leads.merge(email_summary_leads, on="email", how="left")
    for c in ["Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded", "Transactions", "Transactions_creditExcluded"]:
        if c in leads_joined.columns:
            leads_joined[c] = pd.to_numeric(leads_joined[c], errors="coerce").fillna(0.0 if "Transactions" not in c else 0)

    joined_export = leads_joined.copy()
    joined_nonzero_export = joined_export[joined_export["Total_Amount"].fillna(0).ne(0)].copy()

    owner_norm = joined_export["Owner"].fillna("").astype(str).str.strip().str.lower()
    summaries_base_all = joined_export.loc[~owner_norm.eq("pipedrive krispcall")].copy()

    dup_mask = summaries_base_all["email"].duplicated(keep=False)
    duplicate_leads = summaries_base_all.loc[dup_mask].sort_values(["email", "_lead_created_dt"], kind="mergesort")[["email", "Owner", "Lead_Created_Date"]].copy()

    if "_lead_created_dt" in summaries_base_all.columns:
        summaries_base_all = summaries_base_all.sort_values("_lead_created_dt", kind="mergesort")
    summaries_base = summaries_base_all.drop_duplicates(subset=["email"], keep="first").copy()

    sums = _build_summaries(summaries_base)
    owner_summary = sums["owner_summary"]
    connected_summary = sums["connected_summary"]
    owner_x_connected = sums["owner_x_connected"]
    label_summary = sums["label_summary"]
    time_summary = sums["time_summary"]

    fig_owner = _plot_owner(owner_summary)
    fig_owner_conn = _plot_owner_connected(owner_x_connected)
    fig_time = _plot_time(time_summary)

    if desc_col and desc_col in payments_all.columns:
        sub_mask_all = payments_all[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
        subscription_emails_all = set(payments_all.loc[sub_mask_all, "email"].dropna().unique())
    else:
        subscription_emails_all = set()
    sales_attempt_emails = set(summaries_base["email"].dropna().unique())
    self_converted_emails = sorted(list(subscription_emails_all - sales_attempt_emails))
    self_converted_emails_df = pd.DataFrame({"email": self_converted_emails})

    payments_sc = payments_all[payments_all["email"].isin(self_converted_emails)].copy()
    refunds_sc = refunds_all[refunds_all["email"].isin(self_converted_emails)].copy()
    payments_sc_ce = payments_ce_all[payments_ce_all["email"].isin(self_converted_emails)].copy()
    refunds_sc_ce = refunds_ce_all[refunds_ce_all["email"].isin(self_converted_emails)].copy()
    self_converted_summary = _windowed_email_summary(payments_sc, refunds_sc, payments_sc_ce, refunds_sc_ce, amount_col, desc_col, refund_amount_col, 7)

    def totals_from_summary_df(df: pd.DataFrame) -> Dict[str, float]:
        if df is None or df.empty:
            return {"Total": 0.0, "Refund": 0.0, "Net": 0.0, "Transactions": 0, "Total_CE": 0.0, "Refund_CE": 0.0, "Net_CE": 0.0, "Transactions_CE": 0, "Users": 0}
        return {
            "Total": float(_to_float(df, "Total_Amount").sum()),
            "Refund": float(_to_float(df, "Refund_Amount").sum()),
            "Net": float(_to_float(df, "Net_Amount").sum()),
            "Transactions": int(pd.to_numeric(df["Transactions"], errors="coerce").fillna(0).sum()) if "Transactions" in df.columns else 0,
            "Total_CE": float(_to_float(df, "Total_Amount_creditExcluded").sum()),
            "Refund_CE": float(_to_float(df, "Refund_Amount_creditExcluded").sum()),
            "Net_CE": float(_to_float(df, "Net_Amount_creditExcluded").sum()),
            "Transactions_CE": int(pd.to_numeric(df["Transactions_creditExcluded"], errors="coerce").fillna(0).sum()) if "Transactions_creditExcluded" in df.columns else 0,
            "Users": int(df["email"].nunique()) if "email" in df.columns else 0,
        }

    sales_attempt_totals = totals_from_summary_df(summaries_base[["email", "Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded", "Transactions", "Transactions_creditExcluded"]].copy() if not summaries_base.empty else pd.DataFrame())
    sales_effort_df = summaries_base[summaries_base["Connected"] == True].copy()
    sales_effort_connected_totals = totals_from_summary_df(sales_effort_df[["email", "Total_Amount", "Refund_Amount", "Net_Amount", "Total_Amount_creditExcluded", "Refund_Amount_creditExcluded", "Net_Amount_creditExcluded", "Transactions", "Transactions_creditExcluded"]].copy() if not sales_effort_df.empty else pd.DataFrame())
    self_converted_totals = totals_from_summary_df(self_converted_summary)

    payments_all = _ensure_dt_from_mixpanel_time(payments_all)
    refunds_all = _ensure_dt_from_mixpanel_time(refunds_all)
    pay_sel = payments_all[_date_in_range(payments_all["_dt"], from_date, to_date)].copy() if not payments_all.empty else pd.DataFrame()
    ref_sel = refunds_all[_date_in_range(refunds_all["_dt"], from_date, to_date)].copy() if not refunds_all.empty else pd.DataFrame()

    overall_revenue = float(pd.to_numeric(pay_sel[amount_col], errors="coerce").fillna(0.0).sum()) if not pay_sel.empty else 0.0
    overall_transactions = int(len(pay_sel)) if not pay_sel.empty else 0
    overall_refunds_selected_window = float(pd.to_numeric(ref_sel[refund_amount_col], errors="coerce").fillna(0.0).sum()) if not ref_sel.empty else 0.0

    extra_rows = [
        ("Sales Attempt Revenue (Summaries Total) - Net", sales_attempt_totals["Net"]),
        ("Sales Attempt Revenue (Summaries Total) - Net creditExcluded", sales_attempt_totals["Net_CE"]),
        ("Sales Attempt Revenue (Summaries Total) - Transactions", sales_attempt_totals["Transactions"]),
        ("Sales Attempt Revenue (Summaries Total) - Transactions creditExcluded", sales_attempt_totals["Transactions_CE"]),
        ("Sales Effort Revenue (Connected = TRUE) - Net", sales_effort_connected_totals["Net"]),
        ("Sales Effort Revenue (Connected = TRUE) - Net creditExcluded", sales_effort_connected_totals["Net_CE"]),
        ("Sales Effort Revenue (Connected = TRUE) - Transactions", sales_effort_connected_totals["Transactions"]),
        ("Sales Effort Revenue (Connected = TRUE) - Transactions creditExcluded", sales_effort_connected_totals["Transactions_CE"]),
        ("Self Converted Revenue - Net", self_converted_totals["Net"]),
        ("Self Converted Revenue - Net creditExcluded", self_converted_totals["Net_CE"]),
        ("Self Converted Number of Users", len(self_converted_emails)),
        ("Number of Transactions (Selected Range)", overall_transactions),
        ("Overall Revenue (All Sources, Selected Range)", overall_revenue),
        ("Overall Refunds (Selected Range)", overall_refunds_selected_window),
    ]

    logs_df = pd.DataFrame({"log": logs})
    excel_name, excel_bytes = _build_excel(
        owner_summary=owner_summary,
        owner_x_connected=owner_x_connected,
        connected_summary=connected_summary,
        label_summary=label_summary,
        time_summary=time_summary,
        joined_export=joined_export,
        joined_nonzero_export=joined_nonzero_export,
        email_summary=email_summary_leads,
        duplicate_leads=duplicate_leads,
        self_converted_emails_df=self_converted_emails_df,
        logs_df=logs_df,
        fig_owner=fig_owner,
        fig_owner_conn=fig_owner_conn,
        fig_time=fig_time,
        extra_rows=extra_rows,
    )

    return AnalysisResults(
        joined_export=joined_export,
        joined_nonzero_export=joined_nonzero_export,
        owner_summary=owner_summary,
        owner_x_connected=owner_x_connected,
        connected_summary=connected_summary,
        label_summary=label_summary,
        time_summary=time_summary,
        duplicate_leads=duplicate_leads,
        self_converted_emails_list=self_converted_emails,
        self_converted_emails_df=self_converted_emails_df,
        overall_revenue=overall_revenue,
        overall_refunds_selected_window=overall_refunds_selected_window,
        overall_transactions=overall_transactions,
        sales_attempt_totals=sales_attempt_totals,
        sales_effort_connected_totals=sales_effort_connected_totals,
        self_converted_totals=self_converted_totals,
        logs=logs,
        fig_owner=fig_owner,
        fig_owner_conn=fig_owner_conn,
        fig_time=fig_time,
        excel_name=excel_name,
        excel_bytes=excel_bytes,
    )


def _require_login():
    st.sidebar.markdown("### Login")
    u = st.sidebar.text_input("Username")
    p = st.sidebar.text_input("Password", type="password")
    expected_u = str(st.secrets.get("auth", {}).get("username", ""))
    expected_p = str(st.secrets.get("auth", {}).get("password", ""))
    if expected_u and expected_p:
        if u == expected_u and p == expected_p:
            return True
        st.sidebar.warning("Please log in.")
        return False
    st.sidebar.info("Auth secrets not set. Access allowed.")
    return True


def main():
    st.set_page_config(page_title="KrispCall Revenue Analyzer", layout="wide")
    st.title("KrispCall Revenue Analyzer")

    if not _require_login():
        st.stop()

    if "uploaded_leads_bytes" not in st.session_state:
        st.session_state["uploaded_leads_bytes"] = None
    if "results" not in st.session_state:
        st.session_state["results"] = None
    if "last_from_date" not in st.session_state:
        st.session_state["last_from_date"] = None
    if "last_to_date" not in st.session_state:
        st.session_state["last_to_date"] = None

    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        from_date = st.date_input("From date", value=(date.today().replace(day=1) - timedelta(days=3)))
    with c2:
        to_date = st.date_input("To date", value=(date.today() - timedelta(days=1)))
    with c3:
        leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])

    if leads_file is not None:
        st.session_state["uploaded_leads_bytes"] = leads_file.getvalue()

    run = st.button("Run Analysis", type="primary", disabled=(st.session_state["uploaded_leads_bytes"] is None))
    dates_changed = (st.session_state["last_from_date"] != from_date) or (st.session_state["last_to_date"] != to_date)

    if run:
        with st.spinner("Running analysis..."):
            res = _run_analysis(st.session_state["uploaded_leads_bytes"], from_date, to_date)
            st.session_state["results"] = res
            st.session_state["last_from_date"] = from_date
            st.session_state["last_to_date"] = to_date

    if st.session_state["results"] is None:
        st.info("Upload a leads CSV, select dates, then click Run Analysis.")
        st.stop()

    if dates_changed and not run:
        st.warning("Dates changed. Click Run Analysis to refresh results for the new date range.")

    res: AnalysisResults = st.session_state["results"]

    tab_overview, tab_overall, tab_tables, tab_summaries, tab_time, tab_export, tab_logs = st.tabs(
        ["Overview", "Overall Metrics", "Main Tables", "Summaries", "Time", "Export", "Logs"]
    )

    with tab_overview:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Net Revenue (Sales Attempt, summaries)", f"{res.sales_attempt_totals['Net']:,.2f}")
        c2.metric("Net Revenue creditExcluded", f"{res.sales_attempt_totals['Net_CE']:,.2f}")
        c3.metric("Self Converted Users", f"{len(res.self_converted_emails_list):,}")
        c4.metric("Self Converted Net", f"{res.self_converted_totals['Net']:,.2f}")
        st.pyplot(res.fig_owner, use_container_width=True)

    with tab_overall:
        st.markdown("#### Key Totals (side-by-side)")
        rows = [
            {
                "Metric Group": "Sales Attempt (Summaries Total)",
                "Users": res.sales_attempt_totals["Users"],
                "Transactions": res.sales_attempt_totals["Transactions"],
                "Total": res.sales_attempt_totals["Total"],
                "Refund": res.sales_attempt_totals["Refund"],
                "Net": res.sales_attempt_totals["Net"],
                "Transactions (creditExcluded)": res.sales_attempt_totals["Transactions_CE"],
                "Total (creditExcluded)": res.sales_attempt_totals["Total_CE"],
                "Refund (creditExcluded)": res.sales_attempt_totals["Refund_CE"],
                "Net (creditExcluded)": res.sales_attempt_totals["Net_CE"],
            },
            {
                "Metric Group": "Sales Effort (Connected = TRUE)",
                "Users": res.sales_effort_connected_totals["Users"],
                "Transactions": res.sales_effort_connected_totals["Transactions"],
                "Total": res.sales_effort_connected_totals["Total"],
                "Refund": res.sales_effort_connected_totals["Refund"],
                "Net": res.sales_effort_connected_totals["Net"],
                "Transactions (creditExcluded)": res.sales_effort_connected_totals["Transactions_CE"],
                "Total (creditExcluded)": res.sales_effort_connected_totals["Total_CE"],
                "Refund (creditExcluded)": res.sales_effort_connected_totals["Refund_CE"],
                "Net (creditExcluded)": res.sales_effort_connected_totals["Net_CE"],
            },
            {
                "Metric Group": "Self Converted",
                "Users": len(res.self_converted_emails_list),
                "Transactions": res.self_converted_totals["Transactions"],
                "Total": res.self_converted_totals["Total"],
                "Refund": res.self_converted_totals["Refund"],
                "Net": res.self_converted_totals["Net"],
                "Transactions (creditExcluded)": res.self_converted_totals["Transactions_CE"],
                "Total (creditExcluded)": res.self_converted_totals["Total_CE"],
                "Refund (creditExcluded)": res.self_converted_totals["Refund_CE"],
                "Net (creditExcluded)": res.self_converted_totals["Net_CE"],
            },
        ]
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        st.markdown("#### Overall (All Sources, Selected Range)")
        c1, c2, c3 = st.columns(3)
        c1.metric("Overall Revenue", f"{res.overall_revenue:,.2f}")
        c2.metric("Overall Refunds", f"{res.overall_refunds_selected_window:,.2f}")
        c3.metric("Number of Transactions", f"{res.overall_transactions:,}")

        st.markdown("#### Self Converted Emails")
        if res.self_converted_emails_df.empty:
            st.write("No self converted emails found.")
        else:
            st.dataframe(res.self_converted_emails_df, use_container_width=True)

    with tab_tables:
        st.markdown("#### Leads with payments (includes all owners, including Pipedrive KrispCall)")
        st.dataframe(res.joined_export, use_container_width=True)
        st.markdown("#### Leads with non-zero payments only")
        st.dataframe(res.joined_nonzero_export, use_container_width=True)

    with tab_summaries:
        st.markdown("#### Owner Summary (Pipedrive KrispCall excluded)")
        df_owner = _add_totals_row(res.owner_summary, label_col="Owner") if res.owner_summary is not None else res.owner_summary
        st.dataframe(_style_totals_row(df_owner, label_col="Owner"), use_container_width=True)

        st.markdown("#### Owner x Connected (Pipedrive KrispCall excluded)")
        df_oxc = _add_totals_row(res.owner_x_connected, label_col="Owner") if res.owner_x_connected is not None else res.owner_x_connected
        st.dataframe(_style_totals_row(df_oxc, label_col="Owner"), use_container_width=True)
        st.pyplot(res.fig_owner_conn, use_container_width=True)

        st.markdown("#### Connected Summary")
        df_conn = _add_totals_row(res.connected_summary, label_col="Connected") if res.connected_summary is not None else res.connected_summary
        st.dataframe(_style_totals_row(df_conn, label_col="Connected"), use_container_width=True)

        st.markdown("#### Label Summary")
        df_label = _add_totals_row(res.label_summary, label_col="Label") if res.label_summary is not None else res.label_summary
        st.dataframe(_style_totals_row(df_label, label_col="Label"), use_container_width=True)

        st.markdown("#### Duplicate leads by email (in summaries base before dedupe)")
        if res.duplicate_leads is None or res.duplicate_leads.empty:
            st.write("No duplicate emails detected in summaries base.")
        else:
            st.dataframe(res.duplicate_leads, use_container_width=True)

    with tab_time:
        st.markdown("#### Time Summary (grouped by Lead created date)")
        df_time = _add_totals_row(res.time_summary, label_col="Lead_Created_Date") if res.time_summary is not None else res.time_summary
        st.dataframe(_style_totals_row(df_time, label_col="Lead_Created_Date"), use_container_width=True)
        st.pyplot(res.fig_time, use_container_width=True)

    with tab_export:
        st.download_button(
            "Download Excel report",
            data=res.excel_bytes,
            file_name=res.excel_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetsml.sheet",
        )
        st.caption("Downloading should not reset the UI. Results stay available until you change dates and re-run.")

    with tab_logs:
        if res.logs:
            for line in res.logs:
                st.info(line)
        else:
            st.write("No issues logged.")


if __name__ == "__main__":
    main()
