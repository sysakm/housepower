# Household Power Forecasting (SQLite + Python)

Minimal end-to-end analytics pipeline on the UCI Household Power Consumption dataset:

1. ingest minute-level data into SQLite
2. aggregate hourly
3. build leakage-safe features in SQL
4. train baselines + CatBoost
5. evaluate errors
6. simulate offline "would-alert" monitoring

Intended as a compact reference pipeline (not a production system) and for portfolio demonstration.

## Overview

- SQLite-first workflow (no Postgres/Docker).
- Raw ingestion from text file into `raw_power` (minute-level).
- Hourly target: mean active power `y_kw_mean` (kW).
- Feature table built in SQL: time features + lagged and rolling predictors, leakage-safe.
- Baselines:
  - lag (`y_{t-1}`)
  - forward-filled lag
- Model: CatBoost regressor.
- Evaluation:
  - MAE / RMSE / average bias on train and test splits
  - error slices by hour-of-day, day-of-week, month
- Offline "would-alert":
  - threshold calibrated on **train** residuals (95%-quantile of absolute error)
  - alert events and grouped consecutive alert incidents on **test**

## Scope and Non-Goals

This project is not a production forecasting service or a monitoring system.

Intentionally out of scope:

- streaming ingestion, real-time alerting
- model deployment
- hyperparameter-heavy modeling, deep learning
- external features
- "perfect" forecasting (emphasis on pipeline clarity and evaluation discipline)

## Dataset

Individual Household Electric Power Consumption Dataset (UCI ML Repository):
https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption

Place the downloaded text file at:
`data/household_power_consumption.txt`

End-to-end run order:

1. ingest raw data into SQLite
2. build hourly/features/views via SQL pipeline
3. train in notebooks and write predictions back to SQLite
4. evaluate + would-alert report (notebook)

## Quick Start

### Install

```cmd
pip install -r requirements.txt
```

Tested with Python 3.12 and `catboost==1.2.7`.

### 1) Create the SQLite database and ingest raw data

```cmd
python -m py.ingest_sqlite
```

Creates `data/housepower.sqlite` and ingests data into `raw_power`.

Ingestion notes:

- `?` values are treated as NULL
- invalid timestamps are coerced and dropped

### 2) Build hourly + features + train/test views

```cmd
python -m py.run_sqlite_pipeline
```

Runs SQL scripts to create:

- `power_hourly` - hourly aggregates and quality flags
- `power_features` - leakage-safe features for modeling
- `power_features_train`, `power_features_test` - time-based split views
- `model_predictions` - table for storing model outputs

### 3) Train models and write predictions

Training is done in notebooks. Predictions are inserted into SQLite via helper:
`py.sqlite_utils.insert_predictions(pred_df, model_name)`

Expected DataFrame format (hourly timestamps as `YYYY-MM-DD HH:00:00`):

```text
{
  'hour': hour timestamp strings,
  'pred_value': float predictions
}
```

## Repo Structure

```
data/
  household_power_consumption.txt        # local only (not committed)
  housepower.sqlite                      # generated (not committed)
  catboost_model*.cbm                    # optional model dumps (not committed)
py/
  ingest_sqlite.py
  run_sqlite_pipeline.py
  sqlite_utils.py
sql/
  00_schema.sql
  01_hourly.sql
  02_features.sql
  03_train_test_split.sql
  04_predictions.sql
EDA.ipynb
predictions.ipynb
predictions_mae.ipynb
report.ipynb
requirements.txt
README.md
```

## What's inside

### Notebooks

Training and reporting are done in notebooks:

- `EDA.ipynb` - sanity checks, data validation, target and missing values distribution
- `predictions.ipynb` / `predictions_mae.ipynb` - baselines + CatBoost training, writes predictions to SQLite
- `report.ipynb` - metrics, error slices, would-alert simulation

### Tables / views

- `raw_power`  
  Minute-level ingested data (numeric columns + timestamp `ts`).

- `power_hourly`  
  Hourly aggregation. Target is `y_kw_mean = AVG(global_active_power)` per hour.  
  Also includes hourly sums for sub-metering and completeness flags.

- `power_features`  
  Feature table built in SQL (lags/rollings/time features). NULLs are allowed (tree models handle missing values).

- `power_features_train`, `power_features_test`  
  Time-based split views.

- `model_predictions`  
  Long format predictions (`model_name`, `model_ver`, `hour`, `pred_value`).

### Would-alert simulation (offline)

Alerts are simulated on the **test** subset:

- Calibrate threshold on train residuals:  
  `threshold = q95(|y - y_pred|)` on train
- Alert: `|error| > threshold` on labeled test hours
- Missing ground truth hours are excluded from scoring and tracked separately as data availability gaps.
- Alert hours are grouped into incidents by consecutive timestamps.
