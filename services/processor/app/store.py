from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import psycopg

from services.common.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


@dataclass(frozen=True)
class DbConfig:
    host: str = POSTGRES_HOST
    port: int = POSTGRES_PORT
    dbname: str = POSTGRES_DB
    user: str = POSTGRES_USER
    password: str = POSTGRES_PASSWORD


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

class StatsStore:
    def __init__(self, cfg: DbConfig) -> None:
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

    def insert_stats(
        self,
        *,
        total_processed: int,
        avg_tps: float,
        current_tps: float,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processor_stats (total_processed, avg_tps, current_tps)
                VALUES (%s, %s, %s)
                """,
                (total_processed, avg_tps, current_tps),
            )