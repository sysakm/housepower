DROP TABLE IF EXISTS model_predictions;
CREATE TABLE model_predictions (
    model_name TEXT NOT NULL,
    model_ver INTEGER NOT NULL,
    hour TEXT NOT NULL,
    pred_value REAL,
    UNIQUE(model_name, model_ver, hour)
);
CREATE INDEX IF NOT EXISTS idx_model_predictions
    ON model_predictions(model_name, model_ver, hour);
