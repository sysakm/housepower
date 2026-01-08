import sqlite3

import pandas as pd


DB_PATH = 'data/housepower.sqlite'


def connect_sqlite():
    return sqlite3.connect(DB_PATH)


def execute_sql_query(query) -> pd.DataFrame:
    with connect_sqlite() as conn:
        cur = conn.execute(query)
        ret = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
    return pd.DataFrame(ret, columns=col_names)


def execute_sql_script(script):
    with connect_sqlite() as conn:
        conn.executescript(script)
