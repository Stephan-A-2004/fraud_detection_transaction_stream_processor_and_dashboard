from uuid import uuid4

from services.common.schemas import Transaction
from services.processor.app.detector import SlidingWindowDetector


def make_tx(*, user_id="u1", amount=1000.0, timestamp=100, merchant="Tesco") -> Transaction:
    return Transaction(
        transaction_id=uuid4(),
        user_id=user_id,
        amount=amount,
        currency="GBP",
        merchant=merchant,
        timestamp=timestamp,
    )


def test_velocity_amount_rule_triggers_when_count_and_sum_threshold_met() -> None:
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)

    flags = []
    for tx in [
        make_tx(amount=2000, timestamp=100),
        make_tx(amount=2000, timestamp=120),
        make_tx(amount=1501, timestamp=140),
    ]:
        flags = detector.on_transaction(tx)

    reasons = {flag.reason for flag in flags}
    assert "velocity_amount" in reasons
    flagged = next(flag for flag in flags if flag.reason == "velocity_amount")
    assert flagged.txn_count == 3
    assert flagged.total_amount == 5501
    assert flagged.window_start == 100
    assert flagged.window_end == 140


def test_half_open_window_excludes_event_exactly_at_60_seconds() -> None:
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)

    detector.on_transaction(make_tx(amount=2500, timestamp=100))
    detector.on_transaction(make_tx(amount=2500, timestamp=140))
    flags = detector.on_transaction(make_tx(amount=2500, timestamp=160))

    reasons = {flag.reason for flag in flags}
    assert "velocity_amount" not in reasons
    assert detector._windows["u1"][0][0] == 140
    assert len(detector._windows["u1"]) == 2


def test_high_velocity_large_transaction_and_repeat_merchant_can_all_fire() -> None:
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)

    txs = [
        make_tx(amount=2500, timestamp=100, merchant="Amazon"),
        make_tx(amount=2500, timestamp=105, merchant="Amazon"),
        make_tx(amount=2500, timestamp=110, merchant="Amazon"),
        make_tx(amount=2500, timestamp=115, merchant="Tesco"),
        make_tx(amount=2500, timestamp=120, merchant="Uber"),
    ]

    flags = []
    for tx in txs:
        flags = detector.on_transaction(tx)

    reasons = {flag.reason for flag in flags}
    assert reasons == {
        "velocity_amount",
        "high_velocity",
        "large_transaction",
        "rapid_repeat_merchant:Amazon",
    }


def test_windows_are_isolated_per_user() -> None:
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)

    detector.on_transaction(make_tx(user_id="u1", amount=4000, timestamp=100))
    detector.on_transaction(make_tx(user_id="u2", amount=4000, timestamp=100))
    detector.on_transaction(make_tx(user_id="u1", amount=1500, timestamp=120))
    flags = detector.on_transaction(make_tx(user_id="u2", amount=1500, timestamp=120))

    assert flags == []
    assert len(detector._windows["u1"]) == 2
    assert len(detector._windows["u2"]) == 2