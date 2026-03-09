import json
import time

import redis

from services.generator.app.synth import generate_transaction

STREAM_NAME = "transactions"


def get_redis_client():
    return redis.Redis(
        host="localhost",
        port=6379,
        decode_responses=True
    )


def main():

    r = get_redis_client()

    print("Starting transaction generator...")

    while True:

        txn = generate_transaction()

        payload = json.loads(txn.model_dump_json())

        r.xadd(STREAM_NAME, payload, maxlen=10000, approximate=True)

        print(f"Produced transaction: {payload}")

        time.sleep(1)


if __name__ == "__main__":
    main()