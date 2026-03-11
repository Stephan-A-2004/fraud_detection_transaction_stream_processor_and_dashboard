from __future__ import annotations

TIMEFRAME_OPTIONS = [
    "Last 1 hour",
    "Last 24 hours",
    "Last 7 days",
    "Last 30 days",
    "Last 3 months",
    "Last 6 months",
    "Last 1 year",
    "All time",
]

TIMEFRAME_TO_SQL = {
    "Last 1 hour": "NOW() - INTERVAL '1 hour'",
    "Last 24 hours": "NOW() - INTERVAL '24 hours'",
    "Last 7 days": "NOW() - INTERVAL '7 days'",
    "Last 30 days": "NOW() - INTERVAL '30 days'",
    "Last 3 months": "NOW() - INTERVAL '3 months'",
    "Last 6 months": "NOW() - INTERVAL '6 months'",
    "Last 1 year": "NOW() - INTERVAL '1 year'",
    "All time": None,
}


def build_where(*conditions: str) -> str:
    conds = [c for c in conditions if c]
    return ("WHERE " + " AND ".join(conds)) if conds else ""


def get_timeframe_sql(timeframe: str) -> str | None:
    return TIMEFRAME_TO_SQL[timeframe]


def get_bucket_for_timeframe(timeframe: str) -> str:
    if timeframe == "Last 1 hour":
        return "minute"
    if timeframe in {"Last 24 hours", "Last 7 days"}:
        return "hour"
    if timeframe == "Last 30 days":
        return "day"
    if timeframe in {"Last 3 months", "Last 6 months", "Last 1 year"}:
        return "week"
    return "month"


def get_time_condition(timeframe_sql: str | None) -> str:
    return f"created_at >= {timeframe_sql}" if timeframe_sql is not None else ""