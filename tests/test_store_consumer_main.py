import importlib
import sys
import types
from typing import Any, Literal

fake_redis_module = types.ModuleType("redis")
setattr(fake_redis_module, "Redis", object)

fake_psycopg_module = types.ModuleType("psycopg")
setattr(fake_psycopg_module, "connect", lambda **kwargs: None)

sys.modules.setdefault("redis", fake_redis_module)
sys.modules.setdefault("psycopg", fake_psycopg_module)

consumer_mod = importlib.import_module("services.processor.app.consumer")
main_mod = importlib.import_module("services.processor.app.main")
store_mod = importlib.import_module("services.processor.app.store")

RedisStreamConsumer = consumer_mod.RedisStreamConsumer
make_dedupe_key = main_mod.make_dedupe_key
DbConfig = store_mod.DbConfig
FlagStore = store_mod.FlagStore


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any]] = []

    def execute(self, query: Any, params: Any) -> None:
        self.calls.append((query, params))


class CursorContext:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_obj = cursor

    def __enter__(self) -> FakeCursor:
        return self.cursor_obj

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
        return False


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.closed = False

    def cursor(self) -> CursorContext:
        return CursorContext(self.cursor_instance)

    def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[Any, int, int]] = []

    def xread(self, streams: Any, count: int, block: int) -> Any:
        self.calls.append((streams, count, block))
        return self.responses.pop(0)


class FakeRedisFactory:
    def __init__(self, responses: list[Any]) -> None:
        self.instance = FakeRedis(responses)

    def __call__(self, *args: Any, **kwargs: Any) -> FakeRedis:
        return self.instance


def test_flag_store_insert_flag_executes_expected_insert(monkeypatch: Any) -> None:
    fake_conn = FakeConnection()
    monkeypatch.setattr(store_mod.psycopg, "connect", lambda **kwargs: fake_conn)

    store = FlagStore(DbConfig(host="h", port=1, dbname="db", user="u", password="p"))
    store.insert_flag(
        user_id="u1",
        window_start=100,
        window_end=150,
        txn_count=3,
        total_amount=5500.0,
        reason="velocity_amount",
        risk_score=80,
        txn_ids=("a", "b", "c"),
        dedupe_key="dedupe",
    )
    store.close()

    assert fake_conn.closed is True
    query, params = fake_conn.cursor_instance.calls[0]
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in query
    assert params == ("u1", 100, 150, 3, 5500.0, "velocity_amount", 80, ["a", "b", "c"], "dedupe")


def test_redis_stream_consumer_returns_entries_and_advances_cursor(monkeypatch: Any) -> None:
    factory = FakeRedisFactory([
        [("transactions", [("1-0", {"a": "1"}), ("2-0", {"a": "2"})])],
        [],
    ])
    monkeypatch.setattr(consumer_mod.redis, "Redis", factory)

    consumer = RedisStreamConsumer(host="localhost", port=6379)
    first = consumer.read(block_ms=10, count=2)
    second = consumer.read(block_ms=10, count=2)

    assert first == [("1-0", {"a": "1"}), ("2-0", {"a": "2"})]
    assert second == []
    assert consumer._last_id == "2-0"
    assert factory.instance.calls[0][0] == {"transactions": "$"}
    assert factory.instance.calls[1][0] == {"transactions": "2-0"}


def test_make_dedupe_key_is_independent_of_txn_id_order() -> None:
    key1 = make_dedupe_key("u1", "velocity_amount", 100, 150, ["b", "a", "c"])
    key2 = make_dedupe_key("u1", "velocity_amount", 100, 150, ["c", "b", "a"])
    key3 = make_dedupe_key("u1", "velocity_amount", 100, 151, ["a", "b", "c"])

    assert key1 == key2
    assert key1 != key3