import pytest
from services.common.schemas import Transaction


def test_invalid_amount():
    with pytest.raises(ValueError):
        Transaction(
            transaction_id="00000000-0000-0000-0000-000000000000",
            user_id="u1",
            amount=-5,
            currency="GBP",
            merchant="Tesco",
            timestamp=1710000000,
        )