from __future__ import annotations

import time
import pandas as pd
import streamlit as st

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
)

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud Detection Dashboard")

colA, colB, colC = st.columns([1, 1, 2])
with colA:
    auto_refresh = st.checkbox("Auto-refresh", value=True)
with colB:
    refresh_s = st.number_input("Refresh (sec)", min_value=1, max_value=30, value=REFRESH_SECONDS)

timeframe = st.selectbox("Timeframe", TIMEFRAME_OPTIONS, index=1)

timeframe_sql = get_timeframe_sql(timeframe)
bucket = get_bucket_for_timeframe(timeframe)
time_cond = get_time_condition(timeframe_sql)

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric(f"Total flags ({timeframe})", get_total_flags(timeframe_sql))
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
user_options = ["All"] + users_df["user_id"].tolist()

selected_user = st.selectbox("Filter by user", user_options)

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

if auto_refresh:
    time.sleep(int(refresh_s))
    st.rerun()