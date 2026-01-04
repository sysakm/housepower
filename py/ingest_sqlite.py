# Expects to be executed from the parent folder
import pandas as pd
import sqlite3


TXT_PATH = 'data/household_power_consumption.txt'
DB_PATH = 'data/housepower.sqlite'


def main():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_csv(TXT_PATH, sep=';', na_values='?')
    n_rows_read = len(df)
    time_index = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str), format='%d/%m/%Y %H:%M:%S')
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

    with open('sql/00_schema.sql', mode='r') as inf:
        conn.executescript(inf.read())

    n_rows_inserted = df.to_sql('raw_power', conn, index=False, if_exists='append', method='multi', chunksize=30_000)
    print(f'Read {n_rows_read}, inserted {n_rows_inserted} rows')

    conn.close()


if __name__ == '__main__':
    main()