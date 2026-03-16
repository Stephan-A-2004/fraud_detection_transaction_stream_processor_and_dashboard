import pandas as pd

from dashboard.dashboard_filtering import build_where, get_bucket_for_timeframe, get_time_condition, get_timeframe_sql
from dashboard.risk_band_assignment_and_dashboard_styling import add_risk_band, highlight_risk_band, risk_band_from_score
from services.generator.app.synth import generate_transaction


def test_generate_transaction_returns_valid_transaction_shape() -> None:
    tx = generate_transaction()

    assert tx.user_id.startswith("u")
    assert tx.amount > 0
    assert tx.currency in {"GBP"}
    assert tx.merchant
    assert tx.timestamp >= 0


def test_dashboard_filter_helpers_cover_expected_cases() -> None:
    assert build_where("a=1", "", "b=2") == "WHERE a=1 AND b=2"
    assert build_where() == ""
    assert get_timeframe_sql("All time") is None
    assert get_time_condition(None) == ""
    assert get_time_condition("NOW() - INTERVAL '1 hour'") == "created_at >= NOW() - INTERVAL '1 hour'"
    assert get_bucket_for_timeframe("Last 1 hour") == "minute"
    assert get_bucket_for_timeframe("Last 24 hours") == "hour"
    assert get_bucket_for_timeframe("Last 30 days") == "day"
    assert get_bucket_for_timeframe("Last 6 months") == "week"
    assert get_bucket_for_timeframe("All time") == "month"


def test_risk_band_helpers_add_labels_and_styles() -> None:
    df = pd.DataFrame({"risk_score": [30, 50, 80]})

    out = add_risk_band(df)

    assert list(out["risk_band"]) == ["Low", "Medium", "High"]
    assert risk_band_from_score(49) == "Low"
    assert risk_band_from_score(50) == "Medium"
    assert risk_band_from_score(80) == "High"
    assert highlight_risk_band(out.iloc[0]) == [""] * len(out.columns)
    assert highlight_risk_band(out.iloc[1]) == ["background-color: #d97706"] * len(out.columns)
    assert highlight_risk_band(out.iloc[2]) == ["background-color: #b91c1c"] * len(out.columns)