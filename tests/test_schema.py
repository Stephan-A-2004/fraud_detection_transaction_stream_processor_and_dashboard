from services.common.schemas import Transaction


def test_transaction_serialization():
    t = Transaction.now(user_id="u1", amount=12.5, currency="GBP", merchant="Tesco")
    payload = t.model_dump_json()
    t2 = Transaction.model_validate_json(payload)

    assert t2.transaction_id == t.transaction_id