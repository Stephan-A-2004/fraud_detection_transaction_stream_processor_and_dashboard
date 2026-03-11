import importlib
import sys
import types

import pandas as pd

sys.modules.setdefault("psycopg", types.SimpleNamespace(connect=lambda *args, **kwargs: None, Connection=object))


def cache_resource(func=None, **kwargs):
    if func is None:
        return lambda f: f
    return func


sys.modules.setdefault("streamlit", types.SimpleNamespace(cache_resource=cache_resource))

q = importlib.import_module("dashboard.dashboard_queries")


def _capture(monkeypatch):
    calls = []

    def fake_read_df(query, params=None):
        calls.append((query, params))
        normalized = " ".join(query.split())
        if "AS total_flags" in query:
            return pd.DataFrame({"total_flags": [7]})
        if "AS unique_users" in query:
            return pd.DataFrame({"unique_users": [3]})
        if "AS flags_last_5m" in query:
            return pd.DataFrame({"flags_last_5m": [2]})
        return pd.DataFrame({"query": [normalized], "params": [params]})

    monkeypatch.setattr(q, "read_df", fake_read_df)
    return calls


def test_count_queries_use_expected_where_clauses(monkeypatch) -> None:
    calls = _capture(monkeypatch)

    assert q.get_total_flags("NOW() - INTERVAL '1 hour'") == 7
    assert q.get_unique_users(None) == 3
    assert q.get_last_5m_flags() == 2

    assert "WHERE created_at >= NOW() - INTERVAL '1 hour'" in calls[0][0]
    assert "WHERE" not in calls[1][0].split("FROM flags", 1)[1]
    assert "INTERVAL '5 minutes'" in calls[2][0]


def test_dataframe_queries_include_expected_sql_and_params(monkeypatch) -> None:
    _capture(monkeypatch)

    users = q.get_users("NOW() - INTERVAL '24 hours'")
    series = q.get_flags_series(None, "hour")
    top_users = q.get_top_users("NOW() - INTERVAL '7 days'")
    recent = q.get_recent_alerts("WHERE user_id = %s", ("u1",))
    largest = q.get_largest_total_alerts("", ())
    highest = q.get_highest_risk_alerts("WHERE risk_score >= %s", (80,))
    rules = q.get_rule_stats("WHERE created_at >= %s", ("2024-01-01",))

    assert "SELECT DISTINCT user_id FROM flags WHERE created_at >= NOW() - INTERVAL '24 hours' ORDER BY user_id" in users.iloc[0]["query"]
    assert "DATE_TRUNC('hour', created_at)" in series.iloc[0]["query"]
    assert "GROUP BY 1 ORDER BY flags DESC LIMIT 10" in top_users.iloc[0]["query"]
    assert recent.iloc[0]["params"] == ("u1",)
    assert "ORDER BY total_amount DESC, risk_score DESC, created_at DESC" in largest.iloc[0]["query"]
    assert highest.iloc[0]["params"] == (80,)
    assert "rapid_repeat_merchant:%%" in rules.iloc[0]["query"]