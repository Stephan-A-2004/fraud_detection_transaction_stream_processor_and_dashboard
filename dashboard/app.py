from __future__ import annotations

import time
import pandas as pd
import streamlit as st
import psycopg


# ---- Config ----
DB_DSN = "host=localhost port=5432 dbname=transactions user=app password=app"
REFRESH_SECONDS = 2


@st.cache_resource
def get_conn() -> psycopg.Connection:
    # autocommit avoids needing conn.commit() for reads
    return psycopg.connect(DB_DSN, autocommit=True)


def read_df(query: str, params: tuple | None = None) -> pd.DataFrame:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def build_where(*conditions: str) -> str:
    conds = [c for c in conditions if c]
    return ("WHERE " + " AND ".join(conds)) if conds else ""

def add_risk_band(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    risk_bands = []

    for i in range(len(out)):
        row = out.iloc[i]
        risk_bands.append(risk_band_from_score(row["risk_score"]))

    out["risk_band"] = risk_bands
    return out

def risk_band_from_score(score: int) -> str:
    if score >= 80:
        return "High"
    if score >= 50:
        return "Medium"
    return "Low"

def highlight_risk_band(row: pd.Series) -> list[str]:
    if row["risk_band"] == "High":
        return ["background-color: #ffcccc"] * len(row)
    if row["risk_band"] == "Medium":
        return ["background-color: #fff3cd"] * len(row)
    return [""] * len(row)

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud Detection Dashboard")

# Controls
colA, colB, colC = st.columns([1, 1, 2])
with colA:
    auto_refresh = st.checkbox("Auto-refresh", value=True)
with colB:
    refresh_s = st.number_input("Refresh (sec)", min_value=1, max_value=30, value=REFRESH_SECONDS)
with colC:
    st.caption("Reads from Postgres table `flags` produced by your stream processor.")

timeframe = st.selectbox(
    "Timeframe",
    ["Last 1 hour", "Last 24 hours", "Last 7 days", "Last 30 days", "Last 3 months", "Last 6 months", "Last 1 year", "All time"],
    index=1,  # default: Last 24 hours
)

timeframe_sql = {
    "Last 1 hour": "NOW() - INTERVAL '1 hour'",
    "Last 24 hours": "NOW() - INTERVAL '24 hours'",
    "Last 7 days": "NOW() - INTERVAL '7 days'",
    "Last 30 days": "NOW() - INTERVAL '30 days'",
    "Last 3 months": "NOW() - INTERVAL '3 months'",
    "Last 6 months": "NOW() - INTERVAL '6 months'",
    "Last 1 year": "NOW() - INTERVAL '1 year'",
    "All time": None,
}[timeframe]

# Choose chart bucket size based on timeframe (keeps chart readable + fast)
if timeframe == "Last 1 hour":
    bucket = "minute"
elif timeframe == "Last 24 hours":
    bucket = "hour"
elif timeframe == "Last 7 days":
    bucket = "hour"
elif timeframe == "Last 30 days":
    bucket = "day"
elif timeframe == "Last 3 months":
    bucket = "week"
elif timeframe == "Last 6 months":
    bucket = "week"
elif timeframe == "Last 1 year":
    bucket = "week"
else:  # All time
    bucket = "month"

# ---- KPI row ----
kpi1, kpi2, kpi3 = st.columns(3)

time_cond = f"created_at >= {timeframe_sql}" if timeframe_sql is not None else ""

total_flags = read_df(
    f"SELECT COUNT(*) AS total_flags FROM flags {build_where(time_cond)}"
)["total_flags"].iloc[0]

unique_users = read_df(
    f"SELECT COUNT(DISTINCT user_id) AS unique_users FROM flags {build_where(time_cond)}"
)["unique_users"].iloc[0]

last_5m = read_df(
    "SELECT COUNT(*) AS flags_last_5m FROM flags WHERE created_at >= NOW() - INTERVAL '5 minutes'"
)["flags_last_5m"].iloc[0]

kpi1.metric(f"Total flags ({timeframe})", int(total_flags))
kpi2.metric(f"Unique flagged users ({timeframe})", int(unique_users))
kpi3.metric(f"Flags (last 5 min)", int(last_5m))

st.divider()

# ---- Flags chart ----
st.subheader(f"Flags per {bucket}")

where_clause = ""
if timeframe_sql is not None:
    where_clause = f"WHERE created_at >= {timeframe_sql}"

per_series = read_df(
    f"""
    SELECT DATE_TRUNC('{bucket}', created_at) AS t, COUNT(*) AS flags
    FROM flags
    {where_clause}
    GROUP BY 1
    ORDER BY 1
    """
)

if per_series.empty:
    st.info("No flags yet. Start generator + processor to populate data.")
else:
    per_series["t"] = pd.to_datetime(per_series["t"])
    per_series = per_series.set_index("t")

    # fill missing time buckets with zero (makes chart smoother)
    freq_map = {
        "minute": "T",   # minute
        "hour": "H",
        "day": "D",
        "week": "W",
        "month": "MS",   # month start (cleaner for charts)
    }
    per_series = per_series.asfreq(freq_map[bucket]).fillna(0)

    # add rolling average
    per_series["Rolling Avg"] = per_series["flags"].rolling(window=5, min_periods=1).mean()

    # Rename flags column
    per_series = per_series.rename(columns={"flags": "Flags"})

    st.line_chart(per_series)

st.divider()

# ---- User filter ----
time_cond = f"created_at >= {timeframe_sql}" if timeframe_sql is not None else ""
users_df = read_df(f"SELECT DISTINCT user_id FROM flags {build_where(time_cond)} ORDER BY user_id")

user_options = ["All"] + users_df["user_id"].tolist()

selected_user = st.selectbox(
    "Filter by user",
    user_options
)

time_cond = f"created_at >= {timeframe_sql}" if timeframe_sql is not None else ""
user_cond = "" if selected_user == "All" else "user_id = %s"
where_clause = build_where(time_cond, user_cond)
params = () if selected_user == "All" else (selected_user,)

# ---- Top suspicious users ----
left, right = st.columns([1, 1])

with left:
    st.subheader("Top suspicious users")
    if timeframe_sql is None:
        top_users = read_df(
            """
            SELECT user_id,
                COUNT(*) AS flags,
                MAX(total_amount) AS max_total_amount,
                MAX(txn_count) AS max_txn_count
            FROM flags
            GROUP BY 1
            ORDER BY flags DESC
            LIMIT 10
            """
        )
    else:
        top_users = read_df(
            f"""
            SELECT user_id,
                COUNT(*) AS flags,
                MAX(total_amount) AS max_total_amount,
                MAX(txn_count) AS max_txn_count
            FROM flags
            WHERE created_at >= {timeframe_sql}
            GROUP BY 1
            ORDER BY flags DESC
            LIMIT 10
            """
        )
    st.dataframe(top_users, use_container_width=True, hide_index=True)

with right:
    st.subheader("Most recent alerts")

    recent = read_df(
        f"""
        SELECT created_at, user_id, txn_count, total_amount, window_start, window_end, reason, risk_score
        FROM flags
        {where_clause}
        ORDER BY created_at DESC
        LIMIT 25
        """,
        params,
    )

    st.dataframe(recent, use_container_width=True, hide_index=True)
# ---- Largest Alerts ----
st.subheader("Priority alert views")

largest_total_transactions = read_df(
    f"""
    SELECT created_at, user_id, txn_count, total_amount, reason, risk_score
    FROM flags
    {where_clause}
    ORDER BY total_amount DESC, risk_score DESC, created_at DESC
    LIMIT 10
    """,
    params
)

largest_risk_scores = read_df(
    f"""
    SELECT created_at, user_id, txn_count, total_amount, reason, risk_score
    FROM flags
    {where_clause}
    ORDER BY risk_score DESC, total_amount DESC, created_at DESC
    LIMIT 10
    """,
    params
)

left_alerts, right_alerts = st.columns(2)

with left_alerts:
    st.subheader("Largest total amount alerts")

    largest_total_transactions = add_risk_band(largest_total_transactions)

    if not largest_total_transactions.empty:
        styled_largest = largest_total_transactions.style.apply(highlight_risk_band, axis=1)
        st.dataframe(styled_largest, use_container_width=True, hide_index=True)
    else:
        st.info("No alerts found for the selected filters.")

with right_alerts:
    st.subheader("Highest risk alerts")

    largest_risk_scores = add_risk_band(largest_risk_scores)

    if not largest_risk_scores.empty:
        styled_risk = largest_risk_scores.style.apply(highlight_risk_band, axis=1)
        st.dataframe(styled_risk, use_container_width=True, hide_index=True)
    else:
        st.info("No alerts found for the selected filters.")

# ---- Alerts by rule ----
st.subheader("Alerts by rule")

rule_stats = read_df( # Special handling to aggregate all flags with reason "rapid_repeat_merchant" in one bar on the chart
f"""
    SELECT reason, COUNT(*) AS alerts
    FROM (
        SELECT
            CASE
                WHEN reason LIKE 'rapid_repeat_merchant:%%' THEN 'rapid_repeat_merchant'
                ELSE reason
            END AS reason
        FROM flags
        {where_clause}
    ) t
    GROUP BY reason
    ORDER BY alerts DESC
    """,
    params
)

st.bar_chart(rule_stats.set_index("reason"))
# ---- Auto refresh ----
if auto_refresh:
    time.sleep(int(refresh_s))
    st.rerun()