from __future__ import annotations

import pandas as pd
import psycopg
import streamlit as st

from services.common.config import POSTGRES_DSN

@st.cache_resource
def get_conn() -> psycopg.Connection:
    # autocommit avoids needing conn.commit() for reads
    return psycopg.connect(POSTGRES_DSN, autocommit=True)


def read_df(query: str, params: tuple | None = None) -> pd.DataFrame:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)