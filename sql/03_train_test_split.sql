DROP VIEW IF EXISTS power_features_train;
CREATE VIEW power_features_train AS
WITH ordered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY hour) AS row_num,
        COUNT(*) OVER () AS total_rows
    FROM power_features
)
SELECT *
FROM ordered
WHERE row_num <= CAST((total_rows * 0.8) AS INTEGER)
AND y_kw_mean IS NOT NULL;  -- remove NULLs from training view

DROP VIEW IF EXISTS power_features_test;
CREATE VIEW power_features_test AS
WITH ordered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY hour) AS row_num,
        COUNT(*) OVER () AS total_rows
    FROM power_features
)
SELECT *
FROM ordered
WHERE row_num > CAST((total_rows * 0.8) AS INTEGER);
