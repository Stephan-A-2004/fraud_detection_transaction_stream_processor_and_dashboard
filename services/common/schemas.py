from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID, uuid4
import time


class Transaction(BaseModel):
    """
    A single financial transaction event.

    Assumptions:
      - timestamp is UNIX epoch seconds (int), not "seconds within the minute"
      - transaction_id is a UUID represented as a string in JSON
    """

    model_config = ConfigDict(extra="forbid")  # reject unexpected fields

    transaction_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3, description="ISO 4217 code (e.g., GBP, EUR)")
    merchant: str = Field(min_length=1)
    timestamp: int = Field(ge=0, description="UNIX epoch seconds")

    @classmethod
    def now(
        cls,
        *,
        user_id: str,
        amount: float,
        currency: str,
        merchant: str,
    ) -> "Transaction":
        """Convenience constructor using current time as epoch seconds."""
        return cls(
            user_id=user_id,
            amount=amount,
            currency=currency.upper(),
            merchant=merchant,
            timestamp=int(time.time()),
        )