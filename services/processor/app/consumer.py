from __future__ import annotations

from typing import Dict, List, Tuple

import redis

STREAM_NAME = "transactions"


class RedisStreamConsumer:
    def __init__(self, *, host: str = "localhost", port: int = 6379) -> None:
        self._r = redis.Redis(host=host, port=port, decode_responses=True)
        self._last_id = "$"  # Start from new to avoid reprocessing old transactions, therefore faster startup.

    def read(self, block_ms: int = 5000, count: int = 100) -> List[Tuple[str, Dict[str, str]]]:
        """
        Returns list of (redis_id, fields) entries.
        """
        resp = self._r.xread({STREAM_NAME: self._last_id}, count=count, block=block_ms)
        if not resp:
            return []

        # resp format: [(stream_name, [(id, {field: value}), ...])]
        _stream, entries = resp[0]
        out: List[Tuple[str, Dict[str, str]]] = []
        for entry_id, fields in entries:
            out.append((entry_id, fields))
            self._last_id = entry_id  # advance cursor
        return out