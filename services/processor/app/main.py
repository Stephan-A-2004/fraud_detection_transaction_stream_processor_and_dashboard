from __future__ import annotations

from services.common.schemas import Transaction
from services.processor.app.consumer import RedisStreamConsumer
from services.processor.app.detector import SlidingWindowDetector
from services.processor.app.store import DbConfig, FlagStore
import hashlib


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

def compute_risk_score(total_amount: float, txn_count: int, reason: str) -> int: # If formula changes, can reclaculate all risk scores of all rows with new formula. The alternative is calculating risk scores for all rows as soon as the dashbord refreshes, which will be very inefficient for large tables.
    score = 0

    if total_amount >= 10000:
        score += 60
    elif total_amount >= 6000:
        score += 45
    elif total_amount >= 3000:
        score += 30
    else:
        score += 15

    if txn_count >= 5:
        score += 25
    elif txn_count >= 3:
        score += 15
    else:
        score += 5

    if reason == "large_transaction":
        score += 15
    elif reason == "velocity_amount":
        score += 20
    elif reason == "high_velocity":
        score += 10
    elif reason.startswith("rapid_repeat_merchant:"):
        score += 10

    return min(score, 100)

def main() -> None:
    consumer = RedisStreamConsumer()
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)
    store = FlagStore(DbConfig())

    print("Starting processor... (listening to Redis stream 'transactions')")

    try:
        processed_transactions = 0
        while True:
            try:
                entries = consumer.read(block_ms=5000, count=100)
            except Exception as e:
                print(f"Redis read failed: {e}. Retrying in 2 seconds...")
                import time
                time.sleep(2)
                continue

            if not entries:
                continue
            
            for redis_id, fields in entries:
                
                processed_transactions += 1

                if processed_transactions % 100 == 0:
                    print(f"Processed {processed_transactions} events")

                tx = parse_transaction(fields)
                flagged_list = detector.on_transaction(tx)

                for flagged in flagged_list:

                    dedupe_key = make_dedupe_key(
                        flagged.user_id,
                        flagged.reason,
                        flagged.window_start,
                        flagged.window_end,
                        flagged.txn_ids,
                    )

                    risk_score = compute_risk_score(
                        flagged.total_amount,
                        flagged.txn_count,
                        flagged.reason,
                    )

                    print(
                        f"FLAG user={flagged.user_id} count={flagged.txn_count} "
                        f"sum={flagged.total_amount:.2f} reason={flagged.reason} "
                        f"risk={risk_score} "
                        f"window=[{flagged.window_start},{flagged.window_end}]"
                    )


                    store.insert_flag(
                        user_id=flagged.user_id,
                        window_start=flagged.window_start,
                        window_end=flagged.window_end,
                        txn_count=flagged.txn_count,
                        total_amount=flagged.total_amount,
                        reason=flagged.reason,
                        risk_score = risk_score,
                        txn_ids=flagged.txn_ids,
                        dedupe_key=dedupe_key
                    )
    except KeyboardInterrupt:
        print("\nProcessor stopped.")
    finally:
        store.close()

def make_dedupe_key(user_id: str, reason: str, window_start: int, window_end: int, txn_ids: list[str]) -> str:
    payload = f"{user_id}|{reason}|{window_start}|{window_end}|{','.join(sorted(txn_ids))}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

if __name__ == "__main__":
    main()