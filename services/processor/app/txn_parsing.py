from services.common.schemas import Transaction

def parse_transaction(fields: dict[str, str]) -> Transaction:
    """
    Redis stores values as strings, so convert types before validating.
    """
    payload = {
        "transaction_id": fields["transaction_id"],
        "user_id": fields["user_id"],
        "amount": float(fields["amount"]),
        "currency": fields["currency"],
        "merchant": fields["merchant"],
        "timestamp": int(fields["timestamp"]),
    }
    return Transaction.model_validate(payload)