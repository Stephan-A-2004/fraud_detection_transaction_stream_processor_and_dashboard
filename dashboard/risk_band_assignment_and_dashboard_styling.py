from __future__ import annotations

import pandas as pd


def risk_band_from_score(score: int) -> str:
    if score >= 80:
        return "High"
    if score >= 50:
        return "Medium"
    return "Low"


def add_risk_band(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["risk_band"] = out["risk_score"].apply(risk_band_from_score)
    return out


def highlight_risk_band(row: pd.Series) -> list[str]:
    if row["risk_band"] == "High":
        return ["background-color: #b91c1c"] * len(row)
    if row["risk_band"] == "Medium":
        return ["background-color: #d97706"] * len(row)
    return [""] * len(row)