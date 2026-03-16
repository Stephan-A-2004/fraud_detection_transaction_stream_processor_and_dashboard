import pytest
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from services.common.schemas import Transaction
from services.processor.app.detector import FlaggedWindow, SlidingWindowDetector
from services.processor.app.main import make_dedupe_key
from services.processor.app.txn_risk_score_calculation import compute_risk_score


Payload = dict[str, str]
RawEntryFields = Mapping[str, str]
StreamEntry = tuple[str, RawEntryFields]


def make_fields(
    *,
    transaction_id: str = "00000000-0000-0000-0000-000000000001",
    user_id: str = "u1",
    amount: str = "2500",
    currency: str = "GBP",
    merchant: str = "Tesco",
    timestamp: str = "1710000000",
) -> Payload:
    return {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "amount": amount,
        "currency": currency,
        "merchant": merchant,
        "timestamp": timestamp,
    }


def make_tx(
    *,
    user_id: str = "u1",
    amount: float = 2500.0,
    merchant: str = "Tesco",
    timestamp: int = 100,
) -> Transaction:
    return Transaction(
        transaction_id=uuid4(),
        user_id=user_id,
        amount=amount,
        currency="GBP",
        merchant=merchant,
        timestamp=timestamp,
    )


class FakeConsumer:
    def __init__(self, batches: list[object]) -> None:
        self._batches = list(batches)
        self.calls: list[tuple[int, int]] = []

    def read(self, block_ms: int = 5000, count: int = 100) -> object:
        self.calls.append((block_ms, count))
        if not self._batches:
            raise KeyboardInterrupt
        batch = self._batches.pop(0)
        if isinstance(batch, BaseException):
            raise batch
        return batch


class FakeStore:
    def __init__(self, fail_on_insert: bool = False) -> None:
        self.fail_on_insert = fail_on_insert
        self.insert_calls: list[dict[str, object]] = []
        self.closed = False

    def insert_flag(self, **kwargs: object) -> None:
        if self.fail_on_insert:
            raise RuntimeError("db insert failed")
        self.insert_calls.append(kwargs)

    def close(self) -> None:
        self.closed = True


def run_processor_once_with_fakes(
    monkeypatch: Any,
    *,
    entries: list[StreamEntry],
    flagged_windows: list[FlaggedWindow],
    risk_score: int = 80,
    fail_on_insert: bool = False,
) -> tuple[FakeConsumer, FakeStore]:
    """
    Runs services.processor.app.main.main() using fake consumer/store/detector.
    Stops the infinite loop by raising KeyboardInterrupt after fake work is done.
    """
    import services.processor.app.main as main_mod

    fake_consumer = FakeConsumer([entries, KeyboardInterrupt()])
    fake_store = FakeStore(fail_on_insert=fail_on_insert)

    class FakeDetector:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.calls: list[Transaction] = []

        def on_transaction(self, tx: Transaction) -> list[FlaggedWindow]:
            self.calls.append(tx)
            return list(flagged_windows)

    class FakeStatsStore:
        def insert_stats(self, **kwargs: object) -> None:
            pass
        def close(self) -> None:
            pass
    
    def fake_parse_transaction(fields: RawEntryFields) -> Transaction:
        return Transaction(
            transaction_id=uuid4(),
            user_id=str(fields["user_id"]),
            amount=float(fields["amount"]),
            currency=str(fields["currency"]),
            merchant=str(fields["merchant"]),
            timestamp=int(fields["timestamp"]),
        )

    monkeypatch.setattr(main_mod, "RedisStreamConsumer", lambda: fake_consumer)
    monkeypatch.setattr(main_mod, "SlidingWindowDetector", lambda **kwargs: FakeDetector())
    monkeypatch.setattr(main_mod, "FlagStore", lambda cfg: fake_store)
    monkeypatch.setattr(main_mod, "parse_transaction", fake_parse_transaction)
    monkeypatch.setattr(
        main_mod,
        "compute_risk_score",
        lambda total_amount, txn_count, reason: risk_score,
    )

    monkeypatch.setattr(main_mod, "StatsStore", lambda cfg: FakeStatsStore())

    main_mod.main()
    return fake_consumer, fake_store


def test_main_end_to_end_writes_flagged_transaction_once(monkeypatch: Any) -> None:
    flagged = FlaggedWindow(
        user_id="u1",
        window_start=1710000000,
        window_end=1710000030,
        txn_count=3,
        total_amount=7500.0,
        reason="velocity_amount",
        txn_ids=["t2", "t1", "t3"],
    )

    _, fake_store = run_processor_once_with_fakes(
        monkeypatch,
        entries=[("1-0", make_fields())],
        flagged_windows=[flagged],
        risk_score=80,
    )

    assert fake_store.closed is True
    assert len(fake_store.insert_calls) == 1

    call = fake_store.insert_calls[0]
    assert call["user_id"] == "u1"
    assert call["window_start"] == 1710000000
    assert call["window_end"] == 1710000030
    assert call["txn_count"] == 3
    assert call["total_amount"] == 7500.0
    assert call["reason"] == "velocity_amount"
    assert call["risk_score"] == 80
    assert call["txn_ids"] == ["t2", "t1", "t3"]

    expected_dedupe = make_dedupe_key(
        "u1",
        "velocity_amount",
        1710000000,
        1710000030,
        ["t2", "t1", "t3"],
    )
    assert call["dedupe_key"] == expected_dedupe


def test_main_retries_after_redis_read_failure_then_processes_message(monkeypatch: Any) -> None:
    import services.processor.app.main as main_mod

    flagged = FlaggedWindow(
        user_id="u1",
        window_start=1710000000,
        window_end=1710000030,
        txn_count=3,
        total_amount=8000.0,
        reason="velocity_amount",
        txn_ids=["a", "b", "c"],
    )

    fake_consumer = FakeConsumer([
        RuntimeError("redis temporarily unavailable"),
        [("1-0", make_fields())],
        KeyboardInterrupt(),
    ])
    fake_store = FakeStore()

    class FakeDetector:
        def on_transaction(self, tx: Transaction) -> list[FlaggedWindow]:
            return [flagged]
        
    class FakeStatsStore:
        def insert_stats(self, **kwargs: object) -> None:
            pass
        def close(self) -> None:
            pass

    def fake_parse_transaction(fields: RawEntryFields) -> Transaction:
        return Transaction(
            transaction_id=uuid4(),
            user_id=str(fields["user_id"]),
            amount=float(fields["amount"]),
            currency=str(fields["currency"]),
            merchant=str(fields["merchant"]),
            timestamp=int(fields["timestamp"]),
        )

    monkeypatch.setattr(main_mod, "RedisStreamConsumer", lambda: fake_consumer)
    monkeypatch.setattr(main_mod, "SlidingWindowDetector", lambda **kwargs: FakeDetector())
    monkeypatch.setattr(main_mod, "FlagStore", lambda cfg: fake_store)
    monkeypatch.setattr(main_mod, "parse_transaction", fake_parse_transaction)
    monkeypatch.setattr(main_mod, "compute_risk_score", lambda total_amount, txn_count, reason: 80)
    monkeypatch.setattr(main_mod, "StatsStore", lambda cfg: FakeStatsStore())

    sleep_calls: list[int] = []
    monkeypatch.setattr("time.sleep", lambda secs: sleep_calls.append(secs))

    main_mod.main()

    assert sleep_calls == [2]
    assert len(fake_store.insert_calls) == 1
    assert fake_store.closed is True


def test_main_skips_bad_message_cleanly_and_continues(monkeypatch: Any) -> None:
    import services.processor.app.main as main_mod

    good_flag = FlaggedWindow(
        user_id="u1",
        window_start=1710000000,
        window_end=1710000030,
        txn_count=3,
        total_amount=7500.0,
        reason="velocity_amount",
        txn_ids=["a", "b", "c"],
    )

    fake_consumer = FakeConsumer([
        [
            ("1-0", {"bad": "payload"}),
            ("2-0", make_fields()),
        ],
        KeyboardInterrupt(),
    ])
    fake_store = FakeStore()

    class FakeDetector:
        def on_transaction(self, tx: Transaction) -> list[FlaggedWindow]:
            return [good_flag]
        
    class FakeStatsStore:
        def insert_stats(self, **kwargs: object) -> None:
            pass
        def close(self) -> None:
            pass

    def fake_parse_transaction(fields: RawEntryFields) -> Transaction:
        if "user_id" not in fields:
            raise KeyError("user_id")
        return Transaction(
            transaction_id=uuid4(),
            user_id=str(fields["user_id"]),
            amount=float(fields["amount"]),
            currency=str(fields["currency"]),
            merchant=str(fields["merchant"]),
            timestamp=int(fields["timestamp"]),
        )

    monkeypatch.setattr(main_mod, "RedisStreamConsumer", lambda: fake_consumer)
    monkeypatch.setattr(main_mod, "SlidingWindowDetector", lambda **kwargs: FakeDetector())
    monkeypatch.setattr(main_mod, "FlagStore", lambda cfg: fake_store)
    monkeypatch.setattr(main_mod, "parse_transaction", fake_parse_transaction)
    monkeypatch.setattr(main_mod, "compute_risk_score", lambda total_amount, txn_count, reason: 80)
    monkeypatch.setattr(main_mod, "StatsStore", lambda cfg: FakeStatsStore())

    main_mod.main()

    assert len(fake_store.insert_calls) == 1


def test_main_handles_store_insert_failure_without_crashing(monkeypatch: Any) -> None:
    flagged = FlaggedWindow(
        user_id="u1",
        window_start=1710000000,
        window_end=1710000030,
        txn_count=3,
        total_amount=7500.0,
        reason="velocity_amount",
        txn_ids=["a", "b", "c"],
    )

    _, fake_store = run_processor_once_with_fakes(
        monkeypatch,
        entries=[("1-0", make_fields())],
        flagged_windows=[flagged],
        risk_score=80,
        fail_on_insert=True,
    )

    assert fake_store.closed is True


def test_make_dedupe_key_is_stable_for_same_logical_flag() -> None:
    key1 = make_dedupe_key("u1", "velocity_amount", 100, 150, ["b", "a", "c"])
    key2 = make_dedupe_key("u1", "velocity_amount", 100, 150, ["c", "b", "a"])
    key3 = make_dedupe_key("u1", "velocity_amount", 100, 151, ["a", "b", "c"])

    assert key1 == key2
    assert key1 != key3


def test_detector_threshold_edges_for_window_logic() -> None:
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)

    flags: list[FlaggedWindow] = []
    for tx in [
        make_tx(amount=2000, timestamp=100),
        make_tx(amount=1500, timestamp=120),
        make_tx(amount=1500, timestamp=140),
    ]:
        flags = detector.on_transaction(tx)

    reasons = {f.reason for f in flags}
    assert "velocity_amount" not in reasons

    flags = detector.on_transaction(make_tx(amount=1, timestamp=159))
    reasons = {f.reason for f in flags}
    assert "velocity_amount" in reasons


def test_detector_repeat_merchant_just_below_trigger_does_not_flag() -> None:
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)

    flags: list[FlaggedWindow] = []
    for tx in [
        make_tx(amount=1000, merchant="Amazon", timestamp=100),
        make_tx(amount=1000, merchant="Amazon", timestamp=110),
        make_tx(amount=1000, merchant="Tesco", timestamp=120),
    ]:
        flags = detector.on_transaction(tx)

    reasons = {f.reason for f in flags}
    assert all(not reason.startswith("rapid_repeat_merchant:") for reason in reasons)


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
def test_risk_score_thresholds(
    total_amount: float,
    txn_count: int,
    reason: str,
    expected: int,
) -> None:
    assert compute_risk_score(total_amount, txn_count, reason) == expected