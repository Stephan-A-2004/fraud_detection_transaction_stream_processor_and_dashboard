import json
import time

import redis

from services.generator.app.synth import generate_transaction

from services.common.config import TRANSACTION_STREAM, REDIS_HOST, REDIS_PORT


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )


def main() -> None:

    r = get_redis_client()

    print("Starting transaction generator...")

    while True:

        txn = generate_transaction()

        payload = json.loads(txn.model_dump_json())

        r.xadd(TRANSACTION_STREAM, payload, maxlen=10000, approximate=True)

        print(f"Produced transaction: {payload}")

        time.sleep(1)


if __name__ == "__main__":
    main()