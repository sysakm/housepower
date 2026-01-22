from contextlib import nullcontext
from pathlib import Path
import sqlite3

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "housepower.sqlite"


def connect_sqlite():
    return sqlite3.connect(DB_PATH)


def execute_sql_query(query, conn=None) -> pd.DataFrame:
    ctx = connect_sqlite() if conn is None else nullcontext(conn)
    with ctx as c:
        cur = c.execute(query)
        ret = cur.fetchall()
        col_names = [desc[0] for desc in (cur.description or [])]
    return pd.DataFrame(ret, columns=col_names)


def execute_sql_script(script, conn=None):
    ctx = connect_sqlite() if conn is None else nullcontext(conn)
    with ctx as c:
        c.executescript(script)


def insert_predictions(pred_df, model_name, conn=None):
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
