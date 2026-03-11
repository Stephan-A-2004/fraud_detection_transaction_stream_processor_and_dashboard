from __future__ import annotations

from services.common.schemas import Transaction
from services.processor.app.consumer import RedisStreamConsumer
from services.processor.app.detector import SlidingWindowDetector
from services.processor.app.store import DbConfig, FlagStore
from services.common.config import TRANSACTION_STREAM
from services.processor.app.txn_risk_score_calculation import compute_risk_score
from services.processor.app.txn_parsing import parse_transaction

import hashlib

def main() -> None:
    consumer = RedisStreamConsumer()
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)
    store = FlagStore(DbConfig())

    print("Starting processor... (listening to Redis stream '{TRANSACTION_STREAM}')")

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

                try:
                    tx = parse_transaction(fields)
                except Exception as e:
                    print(f"Skipping bad transaction: {e}")
                    continue
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


                    try:
                        store.insert_flag(
                            user_id=flagged.user_id,
                            window_start=flagged.window_start,
                            window_end=flagged.window_end,
                            txn_count=flagged.txn_count,
                            total_amount=flagged.total_amount,
                            reason=flagged.reason,
                            risk_score=risk_score,
                            txn_ids=flagged.txn_ids,
                            dedupe_key=dedupe_key,
                        )
                    except Exception as e:
                        print(f"Failed to store flag: {e}")
    except KeyboardInterrupt:
        print("\nProcessor stopped.")
    finally:
        store.close()

def make_dedupe_key(user_id: str, reason: str, window_start: int, window_end: int, txn_ids: list[str]) -> str:
    payload = f"{user_id}|{reason}|{window_start}|{window_end}|{','.join(sorted(txn_ids))}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

if __name__ == "__main__":
    main()