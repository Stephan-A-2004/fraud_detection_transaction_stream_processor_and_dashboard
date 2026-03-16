from __future__ import annotations

from services.common.schemas import Transaction
from services.processor.app.consumer import RedisStreamConsumer
from services.processor.app.detector import SlidingWindowDetector
from services.processor.app.store import DbConfig, FlagStore, StatsStore
from services.common.config import TRANSACTION_STREAM
from services.processor.app.txn_risk_score_calculation import compute_risk_score
from services.processor.app.txn_parsing import parse_transaction

import hashlib
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("processor")

def main() -> None:
    consumer = RedisStreamConsumer()
    detector = SlidingWindowDetector(window_seconds=60, min_count=3, min_total=5000.0)
    store = FlagStore(DbConfig())
    stats_store = StatsStore(DbConfig())

    logger.info("Starting processor... (listening to Redis stream '%s')", TRANSACTION_STREAM)

    try:
        processed_transactions = 0
        start_time = time.time() 
        window_start_time = time.time()
        window_count = 0
        while True:
            try:
                entries = consumer.read(block_ms=5000, count=100)
            except Exception as e:
                logger.error("Redis read failed: %s. Retrying in 2 seconds...", e)
                time.sleep(2)
                continue

            if not entries:
                continue
            
            for redis_id, fields in entries:
                
                try:
                    tx = parse_transaction(fields)
                except Exception as e:
                    logger.warning("Skipping bad transaction: %s", e)
                    continue

                processed_transactions += 1

                window_count += 1

                if processed_transactions % 100 == 0:
                    now = time.time()
                    avg_tps = processed_transactions / (now - start_time)
                    current_tps = window_count / (now - window_start_time)
                    logger.info(
                        "Processed %s events | Avg txn per second: %.1f | Current txn per second: %.1f",
                        processed_transactions,
                        avg_tps,
                        current_tps,
                    )

                    try:
                        stats_store.insert_stats(
                            total_processed=processed_transactions,
                            avg_tps=avg_tps,
                            current_tps=current_tps,
                        )
                    except Exception as e:
                        logger.error("Failed to store processor stats: %s", e)

                    window_start_time = now
                    window_count = 0

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

                    logger.info(
                        "FLAG user=%s count=%s sum=%.2f reason=%s risk=%s window=[%s,%s]",
                        flagged.user_id,
                        flagged.txn_count,
                        flagged.total_amount,
                        flagged.reason,
                        risk_score,
                        flagged.window_start,
                        flagged.window_end,
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
                        logger.error("Failed to store flag: %s", e)
    except KeyboardInterrupt:
        logger.info("Processor stopped.")
    finally:
        store.close()
        stats_store.close()

def make_dedupe_key(user_id: str, reason: str, window_start: int, window_end: int, txn_ids: list[str]) -> str:
    payload = f"{user_id}|{reason}|{window_start}|{window_end}|{','.join(sorted(txn_ids))}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

if __name__ == "__main__":
    main()