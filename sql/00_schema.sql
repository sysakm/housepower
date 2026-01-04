DROP TABLE IF EXISTS raw_power;
CREATE TABLE raw_power (
    ts TEXT NOT NULL,
    global_active_power REAL,
    global_reactive_power REAL,
    voltage REAL,
    global_intensity REAL,
    sub_1 REAL,
    sub_2 REAL,
    sub_3 REAL
);
CREATE INDEX IF NOT EXISTS idx_raw_power ON raw_power(ts);
