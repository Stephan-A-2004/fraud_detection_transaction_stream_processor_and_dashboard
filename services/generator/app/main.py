import json
import time

import redis

from services.generator.app.synth import generate_transaction

from services.common.config import TRANSACTION_STREAM, REDIS_HOST, REDIS_PORT

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("generator")

def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )


def main() -> None:

    r = get_redis_client()

    logger.info("Starting transaction generator...")

    while True:

        txn = generate_transaction()

        payload = json.loads(txn.model_dump_json())

        r.xadd(TRANSACTION_STREAM, payload, maxlen=10000, approximate=True)

        logger.info("Produced transaction: %s", payload)

        time.sleep(0)


if __name__ == "__main__":
    main()