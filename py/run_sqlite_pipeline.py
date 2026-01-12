from pathlib import Path

from py import sqlite_utils as utils


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_LIST = [
    ROOT / 'sql' / '01_hourly.sql',
    ROOT / 'sql' / '02_features.sql',
    # Following scripts
]


def main():
    with utils.connect_sqlite() as conn:
        for src_path in SCRIPT_LIST:
            utils.execute_sql_script(src_path.read_text(), conn=conn)
            print(f'Executed script {src_path}')


if __name__ == '__main__':
    main()
