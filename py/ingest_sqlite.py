# Expects to be executed from the parent folder
from pathlib import Path
import sqlite3

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
TXT_PATH = ROOT / 'data' / 'household_power_consumption.txt'
DB_PATH  = ROOT / 'data' / 'housepower.sqlite'
SCHEMA_PATH = ROOT / 'sql' / '00_schema.sql'


def main():
    df = pd.read_csv(TXT_PATH, sep=';', na_values='?')
    n_rows_read = len(df)
    time_index = pd.to_datetime(
        df['Date'].astype(str) + ' ' + df['Time'].astype(str),
        format='%d/%m/%Y %H:%M:%S',
        errors='coerce'
    )
    df = df.loc[time_index.notna()].copy()
    time_index = time_index.loc[time_index.notna()]
    n_empty_ts = time_index.isna().sum()

    df.drop(['Date', 'Time'], axis=1, inplace=True)
    df.insert(0, 'ts', time_index.dt.strftime('%Y-%m-%d %H:%M:%S'))
    df.rename({
        'Global_active_power': 'global_active_power',
        'Global_reactive_power': 'global_reactive_power',
        'Voltage': 'voltage',
        'Global_intensity': 'global_intensity',
        'Sub_metering_1': 'sub_1',
        'Sub_metering_2': 'sub_2',
        'Sub_metering_3': 'sub_3'
    }, axis=1, inplace=True)

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA_PATH.read_text())

    n_rows_inserted = df.to_sql('raw_power', conn, index=False, if_exists='append', method='multi', chunksize=30_000)
    print(f'Read {n_rows_read} rows, found {n_empty_ts} empty timestamps, inserted {n_rows_inserted} rows.')

    conn.close()


if __name__ == '__main__':
    main()
