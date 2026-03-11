def compute_risk_score(total_amount: float, txn_count: int, reason: str) -> int: # If formula changes, can reclaculate all risk scores of all rows with new formula. The alternative is calculating risk scores for all rows as soon as the dashbord refreshes, which will be very inefficient for large tables.
    score = 0

    if total_amount >= 10000:
        score += 60
    elif total_amount >= 6000:
        score += 45
    elif total_amount >= 3000:
        score += 30
    else:
        score += 15

    if txn_count >= 5:
        score += 25
    elif txn_count >= 3:
        score += 15
    else:
        score += 5

    if reason == "large_transaction":
        score += 15
    elif reason == "velocity_amount":
        score += 20
    elif reason == "high_velocity":
        score += 10
    elif reason.startswith("rapid_repeat_merchant:"):
        score += 10

    return min(score, 100)
