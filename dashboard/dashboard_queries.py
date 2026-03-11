from __future__ import annotations

import pandas as pd

from dashboard.dashboard_connection_db import read_df
from dashboard.dashboard_filtering import build_where, get_time_condition


def get_total_flags(timeframe_sql: str | None) -> int:
    time_cond = get_time_condition(timeframe_sql)
    df = read_df(f"SELECT COUNT(*) AS total_flags FROM flags {build_where(time_cond)}")
    return int(df["total_flags"].iloc[0])


def get_unique_users(timeframe_sql: str | None) -> int:
    time_cond = get_time_condition(timeframe_sql)
    df = read_df(
        f"SELECT COUNT(DISTINCT user_id) AS unique_users FROM flags {build_where(time_cond)}"
    )
    return int(df["unique_users"].iloc[0])


def get_last_5m_flags() -> int:
    df = read_df(
        "SELECT COUNT(*) AS flags_last_5m FROM flags WHERE created_at >= NOW() - INTERVAL '5 minutes'"
    )
    return int(df["flags_last_5m"].iloc[0])


def get_users(timeframe_sql: str | None) -> pd.DataFrame:
    time_cond = get_time_condition(timeframe_sql)
    return read_df(
        f"SELECT DISTINCT user_id FROM flags {build_where(time_cond)} ORDER BY user_id"
    )


def get_flags_series(timeframe_sql: str | None, bucket: str) -> pd.DataFrame:
    where_clause = ""
    if timeframe_sql is not None:
        where_clause = f"WHERE created_at >= {timeframe_sql}"

    return read_df(
        f"""
        SELECT DATE_TRUNC('{bucket}', created_at) AS t, COUNT(*) AS flags
        FROM flags
        {where_clause}
        GROUP BY 1
        ORDER BY 1
        """
    )


def get_top_users(timeframe_sql: str | None) -> pd.DataFrame:
    where_clause = ""
    if timeframe_sql is not None:
        where_clause = f"WHERE created_at >= {timeframe_sql}"

    return read_df(
        f"""
        SELECT user_id,
            COUNT(*) AS flags,
            MAX(total_amount) AS max_total_amount,
            MAX(txn_count) AS max_txn_count
        FROM flags
        {where_clause}
        GROUP BY 1
        ORDER BY flags DESC
        LIMIT 10
        """
    )


def get_recent_alerts(where_clause: str, params: tuple) -> pd.DataFrame:
    return read_df(
        f"""
        SELECT created_at, user_id, txn_count, total_amount, window_start, window_end, reason, risk_score
        FROM flags
        {where_clause}
        ORDER BY created_at DESC
        LIMIT 25
        """,
        params,
    )


def get_largest_total_alerts(where_clause: str, params: tuple) -> pd.DataFrame:
    return read_df(
        f"""
        SELECT created_at, user_id, txn_count, total_amount, reason, risk_score
        FROM flags
        {where_clause}
        ORDER BY total_amount DESC, risk_score DESC, created_at DESC
        LIMIT 10
        """,
        params,
    )


def get_highest_risk_alerts(where_clause: str, params: tuple) -> pd.DataFrame:
    return read_df(
        f"""
        SELECT created_at, user_id, txn_count, total_amount, reason, risk_score
        FROM flags
        {where_clause}
        ORDER BY risk_score DESC, total_amount DESC, created_at DESC
        LIMIT 10
        """,
        params,
    )


def get_rule_stats(where_clause: str, params: tuple) -> pd.DataFrame:
    return read_df(
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
        params,
    )