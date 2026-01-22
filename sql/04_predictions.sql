DROP TABLE IF EXISTS model_predictions;
CREATE TABLE model_predictions (
    model_name TEXT NOT NULL,
    model_ver INTEGER NOT NULL,
    ts TEXT NOT NULL,
    pred_value REAL NOT NULL,
    UNIQUE(model_name, model_ver, ts)
);
CREATE INDEX IF NOT EXISTS idx_model_predictions
    ON model_predictions(model_name, model_ver, ts);
