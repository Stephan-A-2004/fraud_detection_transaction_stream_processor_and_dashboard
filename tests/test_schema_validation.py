import pytest
from services.common.schemas import Transaction
from uuid import UUID, uuid4


def test_invalid_amount() -> None:
    with pytest.raises(ValueError):
        Transaction(
            transaction_id=UUID("00000000-0000-0000-0000-000000000000"),
            user_id="u1",
            amount=-5,
            currency="GBP",
            merchant="Tesco",
            timestamp=1710000000,
        )