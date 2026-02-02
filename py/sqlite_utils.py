from contextlib import nullcontext
from pathlib import Path
import sqlite3

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "housepower.sqlite"


def connect_sqlite():
    """Return a sqlite3 connection to the project database."""
    return sqlite3.connect(DB_PATH)


def execute_sql_query(query, conn=None) -> pd.DataFrame:
    """Execute a SELECT query and return results as a pandas DataFrame."""
    ctx = connect_sqlite() if conn is None else nullcontext(conn)
    with ctx as c:
        cur = c.execute(query)
        ret = cur.fetchall()
        col_names = [desc[0] for desc in (cur.description or [])]
    return pd.DataFrame(ret, columns=col_names)


def execute_sql_script(script, conn=None):
    """Execute a multi-statement SQL script against SQLite."""
    ctx = connect_sqlite() if conn is None else nullcontext(conn)
    with ctx as c:
        c.executescript(script)


def insert_predictions(pred_df, model_name, conn=None):
    """
    Insert model predictions into the `model_predictions` table.

    Arguments:
    pred_df: DataFrame, must include columns: `hour` (YYYY-MM-DD HH:00:00 strings) and `pred_value`.
    model_name: str, model identifier used to version prediction batches.
    conn: sqlite3.Connection (optional), existing database connection; if None, a new one is created.
    """
    model_version = execute_sql_query(f"""
    SELECT COALESCE(
        (SELECT MAX(model_ver)
         FROM model_predictions
         WHERE model_name = '{model_name}'),
        -1
    ) 
    """, conn=conn).values[0][0] + 1
    print(f'Model version is {model_version}')
    df = pred_df.copy()
    df['model_name'] = model_name
    df['model_ver'] = model_version

    ctx = connect_sqlite() if conn is None else nullcontext(conn)
    with ctx as c:
        n_rows_inserted = df.to_sql(
            'model_predictions',
            c,
            index=False,
            if_exists='append',
            method='multi',
            chunksize=30_000
        )
    print(f'Inserted {n_rows_inserted} rows')
