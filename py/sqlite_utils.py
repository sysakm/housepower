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
