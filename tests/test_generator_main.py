import importlib
import json
import sys
import types

sys.modules.setdefault("redis", types.SimpleNamespace(Redis=object))

generator_main = importlib.import_module("services.generator.app.main")


class FakeRedisClient:
    def __init__(self):
        self.calls = []

    def xadd(self, stream, payload, maxlen, approximate):
        self.calls.append((stream, payload, maxlen, approximate))
        raise KeyboardInterrupt


class FakeTransaction:
    def model_dump_json(self):
        return json.dumps({
            "transaction_id": "00000000-0000-0000-0000-000000000001",
            "user_id": "u1",
            "amount": 100.0,
            "currency": "GBP",
            "merchant": "Tesco",
            "timestamp": 1710000000,
        })


def test_get_redis_client_uses_decode_responses(monkeypatch) -> None:
    captured = {}

    class DummyRedis:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(generator_main.redis, "Redis", DummyRedis)
    generator_main.get_redis_client()

    assert captured["decode_responses"] is True


def test_main_publishes_generated_transaction_once(monkeypatch) -> None:
    fake_redis = FakeRedisClient()
    monkeypatch.setattr(generator_main, "get_redis_client", lambda: fake_redis)
    monkeypatch.setattr(generator_main, "generate_transaction", lambda: FakeTransaction())
    monkeypatch.setattr(generator_main.time, "sleep", lambda _secs: None)

    try:
        generator_main.main()
    except KeyboardInterrupt:
        pass

    assert len(fake_redis.calls) == 1
    stream, payload, maxlen, approximate = fake_redis.calls[0]
    assert stream == "transactions"
    assert payload["user_id"] == "u1"
    assert maxlen == 10000
    assert approximate is True