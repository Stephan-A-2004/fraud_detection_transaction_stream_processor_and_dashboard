from __future__ import annotations

import time
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from services.common.config import REFRESH_SECONDS
from dashboard.dashboard_filtering import (
    TIMEFRAME_OPTIONS,
    build_where,
    get_bucket_for_timeframe,
    get_time_condition,
    get_timeframe_sql,
)
from dashboard.risk_band_assignment_and_dashboard_styling import add_risk_band, highlight_risk_band
from dashboard.dashboard_queries import (
    get_flags_series,
    get_highest_risk_alerts,
    get_largest_total_alerts,
    get_last_5m_flags,
    get_recent_alerts,
    get_rule_stats,
    get_top_users,
    get_total_flags,
    get_unique_users,
    get_users,
    get_processor_stats,
    get_flagged_transactions,
)

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud Detection Dashboard")


if "last_refresh_change" not in st.session_state:
    st.session_state.last_refresh_change = 0.0

if "timeframe" not in st.session_state:
    st.session_state.timeframe = TIMEFRAME_OPTIONS[1]

if "selected_user" not in st.session_state:
    st.session_state.selected_user = "All"

if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True

if "refresh_s" not in st.session_state:
    st.session_state.refresh_s = int(REFRESH_SECONDS)

def on_refresh_change() -> None:
    st.session_state.last_refresh_change = time.time()

colA, colB, _spacing = st.columns([1, 1, 2])
with colA:
    st.checkbox("Auto-refresh", key="auto_refresh")
    auto_refresh = st.session_state.auto_refresh
with colB:
    st.number_input(
        "Refresh (sec)",
        min_value=1,
        max_value=30,
        step=1,
        key="refresh_s",
        on_change=on_refresh_change,
    )

timeframe = st.selectbox(
    "Timeframe",
    TIMEFRAME_OPTIONS,
    index=TIMEFRAME_OPTIONS.index(st.session_state.timeframe),
)
st.session_state.timeframe = timeframe

timeframe_sql = get_timeframe_sql(timeframe)
bucket = get_bucket_for_timeframe(timeframe)
time_cond = get_time_condition(timeframe_sql)

kpi1a, kpi1b, kpi2, kpi3 = st.columns(4)
kpi1a.metric(f"Total flagged transactions ({timeframe})", get_flagged_transactions(timeframe_sql))
kpi1b.metric(f"Total alerts ({timeframe})", get_total_flags(timeframe_sql))
kpi2.metric(f"Unique flagged users ({timeframe})", get_unique_users(timeframe_sql))
kpi3.metric("Flags (last 5 min)", get_last_5m_flags())

st.divider()

st.subheader(f"Flags per {bucket}")
per_series = get_flags_series(timeframe_sql, bucket)

if per_series.empty:
    st.info("No flags yet. Start generator + processor to populate data.")
else:
    per_series["t"] = pd.to_datetime(per_series["t"])
    per_series = per_series.set_index("t")

    freq_map = {
        "minute": "T",
        "hour": "H",
        "day": "D",
        "week": "W",
        "month": "MS",
    }
    per_series = per_series.asfreq(freq_map[bucket]).fillna(0)
    per_series["Rolling Avg"] = per_series["flags"].rolling(window=5, min_periods=1).mean()
    per_series = per_series.rename(columns={"flags": "Flags"})

    st.line_chart(per_series)

st.divider()

users_df = get_users(timeframe_sql)
# Extract numeric part and sort properly
users = users_df["user_id"].tolist()

users_sorted = sorted(users, key=lambda x: int(x.replace("u", "")))

user_options = ["All"] + users_sorted

if st.session_state.selected_user not in user_options:
    st.session_state.selected_user = "All"

selected_user = st.selectbox(
    "Filter by user",
    user_options,
    index=user_options.index(st.session_state.selected_user),
)
st.session_state.selected_user = selected_user

user_cond = "" if selected_user == "All" else "user_id = %s"
where_clause = build_where(time_cond, user_cond)
params = () if selected_user == "All" else (selected_user,)

left, right = st.columns([1, 1])

with left:
    st.subheader("Top suspicious users")
    top_users = get_top_users(timeframe_sql)
    st.dataframe(top_users, use_container_width=True, hide_index=True)

with right:
    st.subheader("Most recent alerts")
    recent = add_risk_band(get_recent_alerts(where_clause, params))
    if not recent.empty:
        st.dataframe(
            recent.style.apply(highlight_risk_band, axis=1),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No alerts found for the selected filters.")

st.subheader("Priority alert views")

largest_total_transactions = add_risk_band(get_largest_total_alerts(where_clause, params))
largest_risk_scores = add_risk_band(get_highest_risk_alerts(where_clause, params))

left_alerts, right_alerts = st.columns(2)

with left_alerts:
    st.subheader("Largest total amount alerts")
    if not largest_total_transactions.empty:
        st.dataframe(
            largest_total_transactions.style.apply(highlight_risk_band, axis=1),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No alerts found for the selected filters.")

with right_alerts:
    st.subheader("Highest risk alerts")
    if not largest_risk_scores.empty:
        st.dataframe(
            largest_risk_scores.style.apply(highlight_risk_band, axis=1),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No alerts found for the selected filters.")

st.subheader("Alerts by rule")
rule_stats = get_rule_stats(where_clause, params)
st.bar_chart(rule_stats.set_index("reason"))

st.divider()
st.subheader("System Health")

stats = get_processor_stats()

if stats.empty:
    st.info("No processor stats yet. Pipeline may not have processed 100 events.")
else:
    row = stats.iloc[0]
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Total events processed since system start", int(row["total_processed"]))
    h2.metric("Avg throughput (TPS)", f"{row['avg_tps']:.1f}")
    h3.metric("Current throughput (TPS)", f"{row['current_tps']:.1f}")
    h4.metric("Last recorded", pd.to_datetime(row["recorded_at"]).strftime("%H:%M:%S"))

if auto_refresh:
    seconds_since_change = time.time() - st.session_state.last_refresh_change

    if seconds_since_change < 2:
        interval_ms = 2000
    else:
        interval_ms = st.session_state.refresh_s * 1000

    st_autorefresh(
        interval=interval_ms,
        key="dashboard_autorefresh",
    )