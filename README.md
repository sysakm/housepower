# Household Power Prediction

### Dataset

Individual Household Electric Power Consumption Dataset, [link](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption).
Place the downloaded text file at `data/household_power_consumption.txt`.

### Database Initialization (SQLite)

```cmd
python -m py.ingest_sqlite
python -m py.run_sqlite_pipeline
```

SQLite database file is created at `data/housepower.sqlite`.
