import pytest
from pydantic import ValidationError

from services.processor.app.txn_parsing import parse_transaction
from services.processor.app.txn_risk_score_calculation import compute_risk_score


def test_parse_transaction_converts_string_fields_to_expected_types() -> None:
    fields = {
        "transaction_id": "00000000-0000-0000-0000-000000000001",
        "user_id": "u1",
        "amount": "123.45",
        "currency": "GBP",
        "merchant": "Tesco",
        "timestamp": "1710000000",
    }

    tx = parse_transaction(fields)

    assert tx.user_id == "u1"
    assert tx.amount == 123.45
    assert tx.timestamp == 1710000000
    assert str(tx.transaction_id) == fields["transaction_id"]


@pytest.mark.parametrize(
    ("total_amount", "txn_count", "reason", "expected"),
    [
        (2500, 2, "velocity_amount", 40),
        (6000, 3, "velocity_amount", 80),
        (12000, 5, "large_transaction", 100),
        (100, 6, "high_velocity", 50),
        (100, 1, "rapid_repeat_merchant:Amazon", 30),
    ],
)
def test_compute_risk_score_matches_threshold_rules(total_amount: float, txn_count: int, reason: str, expected: int) -> None:
    assert compute_risk_score(total_amount, txn_count, reason) == expected


def test_compute_risk_score_caps_at_100() -> None:
    assert compute_risk_score(999999, 999, "velocity_amount") == 100


def test_parse_transaction_raises_key_error_for_missing_field() -> None:
    with pytest.raises(KeyError):
        parse_transaction({
            "transaction_id": "00000000-0000-0000-0000-000000000001",
            "user_id": "u1",
            "amount": "1",
            "currency": "GBP",
            "merchant": "Tesco",
        })


def test_parse_transaction_raises_validation_error_for_invalid_amount() -> None:
    with pytest.raises((ValidationError, ValueError)):
        parse_transaction({
            "transaction_id": "00000000-0000-0000-0000-000000000001",
            "user_id": "u1",
            "amount": "-1",
            "currency": "GBP",
            "merchant": "Tesco",
            "timestamp": "1710000000",
        })