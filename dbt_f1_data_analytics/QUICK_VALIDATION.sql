-- Quick Validation Query for dbt Silver and Gold Tables
-- Run this in Databricks SQL Editor

-- IMPORTANT: Tables are created in default_silver_clean and default_gold_analytics schemas
-- (dbt combines the base schema "default" with the +schema config)

SELECT
    'SILVER' AS layer,
    'silver_sessions' AS table_name,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_sessions) AS record_count
UNION ALL
SELECT 'SILVER', 'silver_drivers',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_drivers)
UNION ALL
SELECT 'SILVER', 'silver_laps',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_laps)
UNION ALL
SELECT 'SILVER', 'silver_locations',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_locations)
UNION ALL
SELECT 'GOLD', 'dim_sessions',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions)
UNION ALL
SELECT 'GOLD', 'dim_drivers',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_drivers)
UNION ALL
SELECT 'GOLD', 'fact_laps',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.fact_laps)
ORDER BY layer, table_name;

-- Expected output:
-- layer  | table_name        | record_count
-- GOLD   | dim_drivers       | ~20
-- GOLD   | dim_sessions      | 15
-- GOLD   | fact_laps         | 7,747
-- SILVER | silver_drivers    | 300
-- SILVER | silver_laps       | 7,747
-- SILVER | silver_locations  | 6,097,599
-- SILVER | silver_sessions   | 15

-- Sample data from silver_sessions
SELECT *
FROM dev_f1_data_analytics.default_silver_clean.silver_sessions
LIMIT 5;

-- Sample data from gold dim_sessions
SELECT session_key, session_name, session_category, session_date, session_year
FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions
ORDER BY session_date DESC
LIMIT 5;

-- Sample data from gold fact_laps
SELECT session_key, driver_number, lap_number, lap_duration, is_valid_lap
FROM dev_f1_data_analytics.default_gold_analytics.fact_laps
WHERE is_valid_lap = TRUE
ORDER BY lap_duration ASC
LIMIT 10;
