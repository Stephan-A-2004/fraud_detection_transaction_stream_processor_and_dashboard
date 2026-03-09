from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from services.common.schemas import Transaction


@dataclass
class FlaggedWindow:
    user_id: str
    window_start: int
    window_end: int
    txn_count: int
    total_amount: float
    reason: str
    txn_ids: List[str]


class SlidingWindowDetector:
    """
    Rule: for each user, flag if there exists a window of transactions where:
      - txn_count >= 3
      - total_amount > 5000
      - timestamps satisfy: newest_ts - oldest_ts < 60   (half-open window)
    """

    def __init__(self, *, window_seconds: int = 60, min_count: int = 3, min_total: float = 5000.0) -> None:
        self.window_seconds = window_seconds
        self.min_count = min_count
        self.min_total = min_total

        # per-user window: deque of (timestamp, amount, transaction_id, merchant)
        self._windows: Dict[str, Deque[Tuple[int, float, str, str]]] = {}
        self._sums: Dict[str, float] = {}
        # optional: simple cooldown so you don't spam flags every event
        self._last_flag_end: Dict[str, int] = {}

    def on_transaction(self, tx: Transaction) -> List[FlaggedWindow]:
        q = self._windows.setdefault(tx.user_id, deque())
        s = self._sums.get(tx.user_id, 0.0)
        flags: List[FlaggedWindow] = []

        # Add new event
        q.append((tx.timestamp, float(tx.amount), str(tx.transaction_id), tx.merchant))
        s += float(tx.amount)

        # Shrink until window condition holds: newest - oldest < window_seconds
        newest_ts = q[-1][0]
        while q and (newest_ts - q[0][0]) >= self.window_seconds:
            _old_ts, old_amt, _old_id, _old_merchant = q.popleft()
            s -= old_amt

        self._sums[tx.user_id] = s

        # Check rule
        window_start = q[0][0]
        window_end = newest_ts
        txn_count = len(q)
        txn_ids = [tid for (_t, _a, tid, _m) in q]

        # Count merchants inside the current window
        merchant_counts = Counter(merchant for (_t, _a, _tid, merchant) in q)

        # Rule 1: velocity + total amount
        if txn_count >= self.min_count and s > self.min_total:
            flags.append(
                FlaggedWindow(
                    user_id=tx.user_id,
                    window_start=window_start,
                    window_end=window_end,
                    txn_count=txn_count,
                    total_amount=s,
                    reason="velocity_amount",
                    txn_ids=txn_ids,
                )
            )

        # Rule 2: high transaction velocity
        if txn_count >= 5:
            flags.append(
                FlaggedWindow(
                    user_id=tx.user_id,
                    window_start=window_start,
                    window_end=window_end,
                    txn_count=txn_count,
                    total_amount=s,
                    reason="high_velocity",
                    txn_ids=txn_ids,
                )
            )

        # Rule 3: large total amount in a short window
        if s >= 10000:
            flags.append(
                FlaggedWindow(
                    user_id=tx.user_id,
                    window_start=window_start,
                    window_end=window_end,
                    txn_count=txn_count,
                    total_amount=s,
                    reason="large_transaction",
                    txn_ids=txn_ids,
                )
            )

        # Rule 4: rapid repeat merchant
        repeat_merchant = None
        for merchant, count in merchant_counts.items():
            if count >= 3:
                repeat_merchant = merchant
                break

        if repeat_merchant is not None:
            flags.append(
                FlaggedWindow(
                    user_id=tx.user_id,
                    window_start=window_start,
                    window_end=window_end,
                    txn_count=txn_count,
                    total_amount=s,
                    reason=f"rapid_repeat_merchant:{repeat_merchant}",
                    txn_ids=txn_ids,
                )
            )

        return flags