import random
from services.common.schemas import Transaction

USERS = [f"u{i}" for i in range(1, 1001)]

MERCHANTS = [
    "Amazon",
    "Tesco",
    "Starbucks",
    "Apple",
    "Uber",
    "Netflix",
]

CURRENCIES = ["GBP"]


def generate_transaction() -> Transaction:
    """
    Generate a synthetic transaction event.
    """

    return Transaction.now(
        user_id=random.choice(USERS),
        amount=round(random.uniform(1, 2000), 2),
        currency=random.choice(CURRENCIES),
        merchant=random.choice(MERCHANTS),
    )