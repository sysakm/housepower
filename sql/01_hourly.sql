DROP TABLE IF EXISTS power_hourly;
CREATE TABLE power_hourly AS
SELECT
    STRFTIME('%Y-%m-%d %H:00:00', ts) AS hour,

    -- Power: hourly mean (kilowatt)
    AVG(global_active_power) AS y_kw_mean, 
    AVG(global_reactive_power) AS react_power_kw_mean,

    -- Voltage (volt) and intensity (ampere): hourly mean
    AVG(voltage) AS voltage_v_mean,
    AVG(global_intensity) AS intensity_a_mean,

    -- Sub-metering: energy (watt-hour), sum of observed (non-NULL)
    SUM(sub_1) AS sub_1_wh_obs, 
    SUM(sub_2) AS sub_2_wh_obs,
    SUM(sub_3) AS sub_3_wh_obs,

    -- Minute coverage
    COUNT(*) AS n_minute_rows,
    COUNT(global_active_power) AS n_nonempty_rows,
    CASE WHEN COUNT(*) = 60 THEN 1 ELSE 0 END AS is_full_hour,
    CASE WHEN COUNT(global_active_power) = 60
    THEN 1 ELSE 0 END AS is_full_data_hour
FROM raw_power
GROUP BY hour;
CREATE INDEX IF NOT EXISTS idx_power_hourly_hour ON power_hourly(hour)
