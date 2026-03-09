from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import psycopg


@dataclass(frozen=True)
class DbConfig:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "transactions"
    user: str = "app"
    password: str = "app"


class FlagStore:
    def __init__(self, cfg: DbConfig) -> None:
        self._cfg = cfg
        self._conn = psycopg.connect(
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.dbname,
            user=cfg.user,
            password=cfg.password,
            autocommit=True,
        )

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def insert_flag(
        self,
        *,
        user_id: str,
        window_start: int,
        window_end: int,
        txn_count: int,
        total_amount: float,
        reason: str,
        risk_score: int,
        txn_ids: Iterable[str],
        dedupe_key: str
    ) -> None:
        txn_ids_list = list(txn_ids)
        with self._conn.cursor() as cur: # Designed so only one connnection is needed in each session.
                cur.execute(
                    """
                    INSERT INTO flags (user_id, window_start, window_end, txn_count, total_amount, reason, risk_score, txn_ids, dedupe_key)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (dedupe_key) DO NOTHING;
                    """,
                    (user_id, window_start, window_end, txn_count, total_amount, reason, risk_score, txn_ids_list, dedupe_key),
                )