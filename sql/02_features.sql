-- Build feature table for forecasting y_kw_mean
DROP TABLE IF EXISTS power_features;
CREATE TABLE power_features AS
WITH base AS (
	SELECT
	-- Source hourly aggregates
		hour,
		y_kw_mean,
		react_power_kw_mean,
		voltage_v_mean,
		intensity_a_mean,
		sub_1_wh_obs,
		sub_2_wh_obs,
		sub_3_wh_obs,
		sub_1_wh_est,
		sub_2_wh_est,
		sub_3_wh_est,
		n_minute_rows,
		n_nonempty_rows,
		is_full_hour,
		is_full_data_hour
	FROM power_hourly
),
enriched AS (
	SELECT
		*,

		-- Calendar/time signals (current hour only)
		CAST(strftime('%H', hour) AS INTEGER) AS hour_of_day,
		CAST(strftime('%w', hour) AS INTEGER) AS day_of_week,
		CAST(strftime('%m', hour) AS INTEGER) AS month_of_year,
		CASE WHEN CAST(strftime('%w', hour) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,

		-- Data quality / missingness
		60 - n_minute_rows AS minutes_missing,
		60 - n_nonempty_rows AS minutes_missing_power,

		-- Electrical relationships
		react_power_kw_mean / NULLIF(y_kw_mean, 0.0) AS react_ratio,
		voltage_v_mean * intensity_a_mean / 1000.0 AS kw_from_vi,

		-- Sub-meter composition
		sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs AS sub_total_wh_obs,
		CASE WHEN (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) = 0 THEN NULL
		ELSE sub_1_wh_obs / (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) END AS sub_1_share,
		CASE WHEN (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) = 0 THEN NULL
		ELSE sub_2_wh_obs / (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) END AS sub_2_share,
		CASE WHEN (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) = 0 THEN NULL
		ELSE sub_3_wh_obs / (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) END AS sub_3_share,
		(y_kw_mean * 1000.0) - (sub_1_wh_obs + sub_2_wh_obs + sub_3_wh_obs) AS unmetered_wh_est,

		-- Sub-meter composition (estimated)
		sub_1_wh_est + sub_2_wh_est + sub_3_wh_est AS sub_total_wh_est,
		CASE WHEN (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) = 0 THEN NULL
		ELSE sub_1_wh_est / (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) END AS sub_1_share_est,
		CASE WHEN (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) = 0 THEN NULL
		ELSE sub_2_wh_est / (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) END AS sub_2_share_est,
		CASE WHEN (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) = 0 THEN NULL
		ELSE sub_3_wh_est / (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) END AS sub_3_share_est,
		(y_kw_mean * 1000.0) - (sub_1_wh_est + sub_2_wh_est + sub_3_wh_est) AS unmetered_wh_est_from_est
	FROM base
),
lags AS (
	SELECT
		-- Timestamp and target
		hour,
		y_kw_mean,

		-- Time signals (current hour)
		hour_of_day,
		day_of_week,
		month_of_year,
		is_weekend,

		-- Lags of target
		LAG(y_kw_mean, 1) OVER (ORDER BY hour) AS y_kw_mean_lag1,
		LAG(y_kw_mean, 2) OVER (ORDER BY hour) AS y_kw_mean_lag2,
		LAG(y_kw_mean, 24) OVER (ORDER BY hour) AS y_kw_mean_lag24,
		LAG(y_kw_mean, 168) OVER (ORDER BY hour) AS y_kw_mean_lag168,

		-- Rolling means of target
		AVG(y_kw_mean) OVER (ORDER BY hour ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS y_kw_mean_roll3_lag1,
		AVG(y_kw_mean) OVER (ORDER BY hour ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS y_kw_mean_roll6_lag1,
		AVG(y_kw_mean) OVER (ORDER BY hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS y_kw_mean_roll24_lag1,
		AVG(y_kw_mean) OVER (ORDER BY hour ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING) AS y_kw_mean_roll168_lag1,

		-- Lags of hourly predictors
		LAG(react_power_kw_mean, 1) OVER (ORDER BY hour) AS react_power_kw_mean_lag1,
		LAG(voltage_v_mean, 1) OVER (ORDER BY hour) AS voltage_v_mean_lag1,
		LAG(intensity_a_mean, 1) OVER (ORDER BY hour) AS intensity_a_mean_lag1,
		LAG(sub_1_wh_obs, 1) OVER (ORDER BY hour) AS sub_1_wh_obs_lag1,
		LAG(sub_2_wh_obs, 1) OVER (ORDER BY hour) AS sub_2_wh_obs_lag1,
		LAG(sub_3_wh_obs, 1) OVER (ORDER BY hour) AS sub_3_wh_obs_lag1,
		LAG(sub_1_wh_est, 1) OVER (ORDER BY hour) AS sub_1_wh_est_lag1,
		LAG(sub_2_wh_est, 1) OVER (ORDER BY hour) AS sub_2_wh_est_lag1,
		LAG(sub_3_wh_est, 1) OVER (ORDER BY hour) AS sub_3_wh_est_lag1,

		-- Missing minutes data
		LAG(minutes_missing, 1) OVER (ORDER BY hour) AS minutes_missing_lag1,
		LAG(minutes_missing_power, 1) OVER (ORDER BY hour) AS minutes_missing_power_lag1,

		-- Electrical relationships
		LAG(react_ratio, 1) OVER (ORDER BY hour) AS react_ratio_lag1,
		LAG(kw_from_vi, 1) OVER (ORDER BY hour) AS kw_from_vi_lag1,

		-- Submetering derived values
		LAG(sub_total_wh_obs, 1) OVER (ORDER BY hour) AS sub_total_wh_obs_lag1,
		LAG(sub_1_share, 1) OVER (ORDER BY hour) AS sub_1_share_lag1,
		LAG(sub_2_share, 1) OVER (ORDER BY hour) AS sub_2_share_lag1,
		LAG(sub_3_share, 1) OVER (ORDER BY hour) AS sub_3_share_lag1,
		LAG(unmetered_wh_est, 1) OVER (ORDER BY hour) AS unmetered_wh_est_lag1,
		LAG(sub_total_wh_est, 1) OVER (ORDER BY hour) AS sub_total_wh_est_lag1,
		LAG(sub_1_share_est, 1) OVER (ORDER BY hour) AS sub_1_share_est_lag1,
		LAG(sub_2_share_est, 1) OVER (ORDER BY hour) AS sub_2_share_est_lag1,
		LAG(sub_3_share_est, 1) OVER (ORDER BY hour) AS sub_3_share_est_lag1,
		LAG(unmetered_wh_est_from_est, 1) OVER (ORDER BY hour) AS unmetered_wh_est_from_est_lag1
	FROM enriched
)
SELECT * FROM lags
WHERE y_kw_mean IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_power_features_hour ON power_features(hour);
