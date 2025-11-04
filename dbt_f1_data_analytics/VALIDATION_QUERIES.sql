-- ============================================================================
-- Silver and Gold Layer Validation Queries
-- F1 Data Analytics - OpenF1 Data Platform
-- ============================================================================

-- ============================================================================
-- SECTION 1: SILVER LAYER VALIDATION
-- ============================================================================

-- 1.1 Check Silver Tables Exist and Record Counts
-- ============================================================================
SELECT 'silver_sessions' AS table_name, COUNT(*) AS record_count
FROM dev_f1_data_analytics.silver_clean.silver_sessions
UNION ALL
SELECT 'silver_drivers', COUNT(*)
FROM dev_f1_data_analytics.silver_clean.silver_drivers
UNION ALL
SELECT 'silver_laps', COUNT(*)
FROM dev_f1_data_analytics.silver_clean.silver_laps
UNION ALL
SELECT 'silver_locations', COUNT(*)
FROM dev_f1_data_analytics.silver_clean.silver_locations
ORDER BY table_name;


-- 1.2 Verify Silver Sessions Data Quality
-- ============================================================================
-- Check for nulls in key fields
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT session_key) AS unique_sessions,
    SUM(CASE WHEN session_key IS NULL THEN 1 ELSE 0 END) AS null_session_keys,
    SUM(CASE WHEN session_name IS NULL THEN 1 ELSE 0 END) AS null_session_names,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_locations,
    MIN(date_start) AS earliest_session,
    MAX(date_start) AS latest_session
FROM dev_f1_data_analytics.silver_clean.silver_sessions;

-- Check session categories
SELECT
    session_category,
    COUNT(*) AS count,
    COUNT(DISTINCT session_key) AS unique_sessions
FROM dev_f1_data_analytics.silver_clean.silver_sessions
GROUP BY session_category
ORDER BY count DESC;


-- 1.3 Verify Silver Drivers Data Quality
-- ============================================================================
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT driver_number) AS unique_drivers,
    COUNT(DISTINCT full_name) AS unique_driver_names,
    COUNT(DISTINCT team_name) AS unique_teams,
    SUM(CASE WHEN driver_number IS NULL THEN 1 ELSE 0 END) AS null_driver_numbers,
    SUM(CASE WHEN full_name IS NULL THEN 1 ELSE 0 END) AS null_full_names
FROM dev_f1_data_analytics.silver_clean.silver_drivers;

-- Top drivers by number of sessions
SELECT
    full_name,
    driver_number,
    team_name,
    COUNT(DISTINCT session_key) AS sessions_participated
FROM dev_f1_data_analytics.silver_clean.silver_drivers
WHERE full_name IS NOT NULL
GROUP BY full_name, driver_number, team_name
ORDER BY sessions_participated DESC
LIMIT 10;


-- 1.4 Verify Silver Laps Data Quality
-- ============================================================================
SELECT
    COUNT(*) AS total_laps,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT driver_number) AS unique_drivers,
    SUM(CASE WHEN lap_duration IS NULL THEN 1 ELSE 0 END) AS null_lap_durations,
    SUM(CASE WHEN is_pit_out_lap = TRUE THEN 1 ELSE 0 END) AS pit_out_laps,
    ROUND(AVG(lap_duration), 2) AS avg_lap_duration_seconds,
    ROUND(MIN(lap_duration), 2) AS fastest_lap_seconds,
    ROUND(MAX(lap_duration), 2) AS slowest_lap_seconds
FROM dev_f1_data_analytics.silver_clean.silver_laps
WHERE lap_duration IS NOT NULL;

-- Lap count distribution by session
SELECT
    session_key,
    COUNT(*) AS total_laps,
    COUNT(DISTINCT driver_number) AS drivers_count,
    COUNT(DISTINCT lap_number) AS max_lap_number
FROM dev_f1_data_analytics.silver_clean.silver_laps
GROUP BY session_key
ORDER BY total_laps DESC
LIMIT 10;


-- 1.5 Verify Silver Locations Data Quality
-- ============================================================================
SELECT
    COUNT(*) AS total_location_points,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT driver_number) AS unique_drivers,
    SUM(CASE WHEN x IS NULL THEN 1 ELSE 0 END) AS null_x_coords,
    SUM(CASE WHEN y IS NULL THEN 1 ELSE 0 END) AS null_y_coords,
    SUM(CASE WHEN z IS NULL THEN 1 ELSE 0 END) AS null_z_coords
FROM dev_f1_data_analytics.silver_clean.silver_locations;

-- Average location points per session
SELECT
    session_key,
    COUNT(*) AS location_points,
    COUNT(DISTINCT driver_number) AS drivers_count,
    ROUND(COUNT(*) / COUNT(DISTINCT driver_number), 0) AS avg_points_per_driver
FROM dev_f1_data_analytics.silver_clean.silver_locations
GROUP BY session_key
ORDER BY location_points DESC
LIMIT 10;


-- ============================================================================
-- SECTION 2: GOLD LAYER VALIDATION - DIMENSIONS
-- ============================================================================

-- 2.1 Verify dim_sessions
-- ============================================================================
SELECT
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT session_key) AS unique_session_keys,
    COUNT(DISTINCT meeting_key) AS unique_meetings,
    COUNT(DISTINCT circuit_key) AS unique_circuits,
    COUNT(DISTINCT year) AS unique_years,
    MIN(date_start) AS earliest_session,
    MAX(date_start) AS latest_session
FROM dev_f1_data_analytics.gold_analytics.dim_sessions;

-- Sessions by category
SELECT
    session_category,
    COUNT(*) AS session_count,
    COUNT(DISTINCT circuit_key) AS unique_circuits
FROM dev_f1_data_analytics.gold_analytics.dim_sessions
GROUP BY session_category
ORDER BY session_count DESC;

-- Sessions by year
SELECT
    year,
    COUNT(*) AS session_count,
    COUNT(DISTINCT meeting_key) AS meetings,
    COUNT(DISTINCT circuit_key) AS circuits
FROM dev_f1_data_analytics.gold_analytics.dim_sessions
GROUP BY year
ORDER BY year DESC;


-- 2.2 Verify dim_drivers
-- ============================================================================
SELECT
    COUNT(*) AS total_driver_records,
    COUNT(DISTINCT driver_key) AS unique_drivers,
    COUNT(DISTINCT team_name) AS unique_teams,
    SUM(CASE WHEN is_current_record = TRUE THEN 1 ELSE 0 END) AS current_records,
    SUM(CASE WHEN is_current_record = FALSE THEN 1 ELSE 0 END) AS historical_records
FROM dev_f1_data_analytics.gold_analytics.dim_drivers;

-- Current drivers by team
SELECT
    team_name,
    COUNT(*) AS driver_count,
    COLLECT_SET(full_name) AS drivers
FROM dev_f1_data_analytics.gold_analytics.dim_drivers
WHERE is_current_record = TRUE
GROUP BY team_name
ORDER BY driver_count DESC;

-- Driver history (SCD Type 2 validation)
SELECT
    full_name,
    driver_number,
    team_name,
    valid_from,
    valid_to,
    is_current_record
FROM dev_f1_data_analytics.gold_analytics.dim_drivers
WHERE full_name IN (
    SELECT full_name
    FROM dev_f1_data_analytics.gold_analytics.dim_drivers
    GROUP BY full_name
    HAVING COUNT(*) > 1
)
ORDER BY full_name, valid_from
LIMIT 50;


-- ============================================================================
-- SECTION 3: GOLD LAYER VALIDATION - FACTS
-- ============================================================================

-- 3.1 Verify fact_laps
-- ============================================================================
SELECT
    COUNT(*) AS total_laps,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT driver_key) AS unique_drivers,
    SUM(CASE WHEN is_valid_lap = TRUE THEN 1 ELSE 0 END) AS valid_laps,
    SUM(CASE WHEN is_valid_lap = FALSE THEN 1 ELSE 0 END) AS invalid_laps,
    ROUND(AVG(lap_duration_seconds), 2) AS avg_lap_duration,
    ROUND(MIN(lap_duration_seconds), 2) AS fastest_lap,
    ROUND(MAX(lap_duration_seconds), 2) AS slowest_lap
FROM dev_f1_data_analytics.gold_analytics.fact_laps
WHERE lap_duration_seconds IS NOT NULL;

-- Lap statistics by session
SELECT
    f.session_key,
    d.session_name,
    d.location,
    COUNT(*) AS total_laps,
    COUNT(DISTINCT f.driver_key) AS drivers,
    ROUND(AVG(f.lap_duration_seconds), 2) AS avg_lap_time,
    ROUND(MIN(f.lap_duration_seconds), 2) AS fastest_lap
FROM dev_f1_data_analytics.gold_analytics.fact_laps f
JOIN dev_f1_data_analytics.gold_analytics.dim_sessions d ON f.session_key = d.session_key
WHERE f.is_valid_lap = TRUE
GROUP BY f.session_key, d.session_name, d.location
ORDER BY total_laps DESC
LIMIT 10;


-- ============================================================================
-- SECTION 4: GOLD LAYER VALIDATION - MARTS
-- ============================================================================

-- 4.1 Verify session_leaderboard
-- ============================================================================
SELECT
    COUNT(*) AS total_leaderboard_entries,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT driver_number) AS unique_drivers,
    MIN(position) AS min_position,
    MAX(position) AS max_position
FROM dev_f1_data_analytics.gold_analytics.session_leaderboard;

-- Sample leaderboard from latest session
WITH latest_session AS (
    SELECT session_key, session_name, location
    FROM dev_f1_data_analytics.gold_analytics.dim_sessions
    ORDER BY date_start DESC
    LIMIT 1
)
SELECT
    l.position,
    l.driver_number,
    l.driver_name,
    l.team_name,
    l.total_laps,
    l.fastest_lap_seconds,
    l.avg_lap_seconds,
    l.total_pit_stops
FROM dev_f1_data_analytics.gold_analytics.session_leaderboard l
JOIN latest_session s ON l.session_key = s.session_key
ORDER BY l.position
LIMIT 20;


-- 4.2 Verify driver_session_summary
-- ============================================================================
SELECT
    COUNT(*) AS total_summaries,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT driver_number) AS unique_drivers,
    SUM(total_laps) AS all_laps_combined,
    ROUND(AVG(avg_lap_time), 2) AS overall_avg_lap_time
FROM dev_f1_data_analytics.gold_analytics.driver_session_summary;

-- Top performers across all sessions
SELECT
    driver_name,
    COUNT(DISTINCT session_key) AS sessions_participated,
    SUM(total_laps) AS total_laps,
    ROUND(MIN(fastest_lap), 2) AS personal_best_lap,
    ROUND(AVG(avg_lap_time), 2) AS career_avg_lap_time,
    SUM(total_pit_stops) AS total_pit_stops
FROM dev_f1_data_analytics.gold_analytics.driver_session_summary
GROUP BY driver_name
ORDER BY sessions_participated DESC, personal_best_lap ASC
LIMIT 10;


-- 4.3 Verify team_performance
-- ============================================================================
SELECT
    COUNT(*) AS total_team_session_records,
    COUNT(DISTINCT session_key) AS unique_sessions,
    COUNT(DISTINCT team_name) AS unique_teams,
    SUM(total_laps) AS all_laps_combined,
    ROUND(AVG(avg_lap_time), 2) AS overall_avg_lap_time
FROM dev_f1_data_analytics.gold_analytics.team_performance;

-- Team performance leaderboard
SELECT
    team_name,
    COUNT(DISTINCT session_key) AS sessions,
    SUM(total_drivers) AS total_driver_entries,
    SUM(total_laps) AS total_laps,
    ROUND(MIN(team_fastest_lap), 2) AS team_best_lap,
    ROUND(AVG(avg_lap_time), 2) AS team_avg_lap_time,
    SUM(total_pit_stops) AS total_pit_stops
FROM dev_f1_data_analytics.gold_analytics.team_performance
GROUP BY team_name
ORDER BY team_best_lap ASC
LIMIT 10;


-- ============================================================================
-- SECTION 5: DATA LINEAGE VALIDATION
-- ============================================================================

-- 5.1 Bronze to Silver Record Preservation
-- ============================================================================
-- Verify no data loss from bronze to silver

SELECT
    'sessions' AS entity,
    b.bronze_count,
    s.silver_count,
    s.silver_count - b.bronze_count AS difference,
    ROUND((s.silver_count::FLOAT / b.bronze_count) * 100, 2) AS preservation_pct
FROM
    (SELECT COUNT(*) AS bronze_count FROM dev_f1_data_analytics.bronze_raw.bronze_sessions) b,
    (SELECT COUNT(DISTINCT session_key) AS silver_count FROM dev_f1_data_analytics.silver_clean.silver_sessions) s

UNION ALL

SELECT
    'drivers',
    b.bronze_count,
    s.silver_count,
    s.silver_count - b.bronze_count,
    ROUND((s.silver_count::FLOAT / b.bronze_count) * 100, 2)
FROM
    (SELECT COUNT(*) AS bronze_count FROM dev_f1_data_analytics.bronze_raw.bronze_drivers) b,
    (SELECT COUNT(*) AS silver_count FROM dev_f1_data_analytics.silver_clean.silver_drivers) s

UNION ALL

SELECT
    'laps',
    b.bronze_count,
    s.silver_count,
    s.silver_count - b.bronze_count,
    ROUND((s.silver_count::FLOAT / b.bronze_count) * 100, 2)
FROM
    (SELECT COUNT(*) AS bronze_count FROM dev_f1_data_analytics.bronze_raw.bronze_laps) b,
    (SELECT COUNT(*) AS silver_count FROM dev_f1_data_analytics.silver_clean.silver_laps) s

UNION ALL

SELECT
    'locations',
    b.bronze_count,
    s.silver_count,
    s.silver_count - b.bronze_count,
    ROUND((s.silver_count::FLOAT / b.bronze_count) * 100, 2)
FROM
    (SELECT COUNT(*) AS bronze_count FROM dev_f1_data_analytics.bronze_raw.bronze_locations) b,
    (SELECT COUNT(*) AS silver_count FROM dev_f1_data_analytics.silver_clean.silver_locations) s;


-- 5.2 Silver to Gold Dimension Preservation
-- ============================================================================
SELECT
    'dim_sessions' AS dimension,
    s.silver_count,
    g.gold_count,
    g.gold_count - s.silver_count AS difference
FROM
    (SELECT COUNT(DISTINCT session_key) AS silver_count FROM dev_f1_data_analytics.silver_clean.silver_sessions) s,
    (SELECT COUNT(DISTINCT session_key) AS gold_count FROM dev_f1_data_analytics.gold_analytics.dim_sessions) g

UNION ALL

SELECT
    'dim_drivers',
    s.silver_count,
    g.gold_count,
    g.gold_count - s.silver_count
FROM
    (SELECT COUNT(DISTINCT driver_number) AS silver_count FROM dev_f1_data_analytics.silver_clean.silver_drivers) s,
    (SELECT COUNT(DISTINCT driver_number) AS gold_count FROM dev_f1_data_analytics.gold_analytics.dim_drivers WHERE is_current_record = TRUE) g;


-- ============================================================================
-- SECTION 6: BUSINESS LOGIC VALIDATION
-- ============================================================================

-- 6.1 Session Duration Validation
-- ============================================================================
SELECT
    session_key,
    session_name,
    location,
    date_start,
    date_end,
    session_duration_minutes,
    CASE
        WHEN session_category = 'Race' AND session_duration_minutes > 120 THEN 'OK'
        WHEN session_category = 'Qualifying' AND session_duration_minutes > 60 THEN 'OK'
        WHEN session_category = 'Practice' AND session_duration_minutes > 60 THEN 'OK'
        ELSE 'CHECK'
    END AS duration_check
FROM dev_f1_data_analytics.gold_analytics.dim_sessions
WHERE session_duration_minutes IS NOT NULL
ORDER BY date_start DESC
LIMIT 20;


-- 6.2 Lap Time Reasonableness
-- ============================================================================
-- Check for suspiciously fast or slow laps
SELECT
    f.session_key,
    d.session_name,
    f.driver_number,
    f.lap_number,
    f.lap_duration_seconds,
    CASE
        WHEN f.lap_duration_seconds < 60 THEN 'VERY FAST (check if valid)'
        WHEN f.lap_duration_seconds > 300 THEN 'VERY SLOW (likely invalid)'
        ELSE 'NORMAL'
    END AS lap_classification
FROM dev_f1_data_analytics.gold_analytics.fact_laps f
JOIN dev_f1_data_analytics.gold_analytics.dim_sessions d ON f.session_key = d.session_key
WHERE f.lap_duration_seconds IS NOT NULL
AND (f.lap_duration_seconds < 60 OR f.lap_duration_seconds > 300)
ORDER BY f.lap_duration_seconds
LIMIT 50;


-- ============================================================================
-- SECTION 7: INCREMENTAL LOAD VALIDATION
-- ============================================================================

-- 7.1 Check Latest Ingestion Timestamps
-- ============================================================================
SELECT
    'silver_sessions' AS table_name,
    MAX(_ingestion_timestamp) AS latest_ingestion,
    COUNT(*) AS records_count
FROM dev_f1_data_analytics.silver_clean.silver_sessions

UNION ALL

SELECT
    'silver_drivers',
    MAX(_ingestion_timestamp),
    COUNT(*)
FROM dev_f1_data_analytics.silver_clean.silver_drivers

UNION ALL

SELECT
    'silver_laps',
    MAX(_ingestion_timestamp),
    COUNT(*)
FROM dev_f1_data_analytics.silver_clean.silver_laps

UNION ALL

SELECT
    'silver_locations',
    MAX(_ingestion_timestamp),
    COUNT(*)
FROM dev_f1_data_analytics.silver_clean.silver_locations

ORDER BY table_name;


-- 7.2 Check for Duplicate Records
-- ============================================================================
-- Check for duplicates in silver_sessions (should have unique session_keys)
SELECT
    session_key,
    COUNT(*) AS duplicate_count
FROM dev_f1_data_analytics.silver_clean.silver_sessions
GROUP BY session_key
HAVING COUNT(*) > 1
LIMIT 10;

-- Check for duplicates in silver_drivers
SELECT
    session_key,
    driver_number,
    COUNT(*) AS duplicate_count
FROM dev_f1_data_analytics.silver_clean.silver_drivers
GROUP BY session_key, driver_number
HAVING COUNT(*) > 1
LIMIT 10;


-- ============================================================================
-- SECTION 8: QUICK HEALTH CHECK (RUN THIS FIRST!)
-- ============================================================================

-- Comprehensive health check summary
SELECT
    'BRONZE LAYER' AS layer,
    'bronze_sessions' AS table_name,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions) AS record_count
UNION ALL
SELECT 'BRONZE LAYER', 'bronze_drivers', (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_drivers)
UNION ALL
SELECT 'BRONZE LAYER', 'bronze_laps', (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_laps)
UNION ALL
SELECT 'BRONZE LAYER', 'bronze_locations', (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_locations)

UNION ALL

SELECT 'SILVER LAYER', 'silver_sessions', (SELECT COUNT(*) FROM dev_f1_data_analytics.silver_clean.silver_sessions)
UNION ALL
SELECT 'SILVER LAYER', 'silver_drivers', (SELECT COUNT(*) FROM dev_f1_data_analytics.silver_clean.silver_drivers)
UNION ALL
SELECT 'SILVER LAYER', 'silver_laps', (SELECT COUNT(*) FROM dev_f1_data_analytics.silver_clean.silver_laps)
UNION ALL
SELECT 'SILVER LAYER', 'silver_locations', (SELECT COUNT(*) FROM dev_f1_data_analytics.silver_clean.silver_locations)

UNION ALL

SELECT 'GOLD LAYER - DIMS', 'dim_sessions', (SELECT COUNT(*) FROM dev_f1_data_analytics.gold_analytics.dim_sessions)
UNION ALL
SELECT 'GOLD LAYER - DIMS', 'dim_drivers', (SELECT COUNT(*) FROM dev_f1_data_analytics.gold_analytics.dim_drivers WHERE is_current_record = TRUE)

UNION ALL

SELECT 'GOLD LAYER - FACTS', 'fact_laps', (SELECT COUNT(*) FROM dev_f1_data_analytics.gold_analytics.fact_laps)

UNION ALL

SELECT 'GOLD LAYER - MARTS', 'session_leaderboard', (SELECT COUNT(*) FROM dev_f1_data_analytics.gold_analytics.session_leaderboard)
UNION ALL
SELECT 'GOLD LAYER - MARTS', 'driver_session_summary', (SELECT COUNT(*) FROM dev_f1_data_analytics.gold_analytics.driver_session_summary)
UNION ALL
SELECT 'GOLD LAYER - MARTS', 'team_performance', (SELECT COUNT(*) FROM dev_f1_data_analytics.gold_analytics.team_performance)

ORDER BY layer, table_name;

-- ============================================================================
-- END OF VALIDATION QUERIES
-- ============================================================================

/*
USAGE INSTRUCTIONS:

1. Quick Health Check:
   - Run Section 8 first to get a quick overview of all layers

2. Detailed Validation:
   - Run Section 1 for Silver Layer validation
   - Run Section 2-4 for Gold Layer validation
   - Run Section 5 for Data Lineage validation

3. Data Quality Checks:
   - Run Section 6 for business logic validation
   - Run Section 7 for incremental load validation

4. Expected Results (for 15 sessions):
   Bronze:
   - bronze_sessions: 15
   - bronze_drivers: 300
   - bronze_laps: ~7,747
   - bronze_locations: ~6,097,599

   Silver (should match bronze):
   - silver_sessions: 15
   - silver_drivers: 300
   - silver_laps: ~7,747
   - silver_locations: ~6,097,599

   Gold:
   - dim_sessions: 15
   - dim_drivers: 20-30 (current records)
   - fact_laps: ~7,747
   - session_leaderboard: ~300 (15 sessions × ~20 drivers)
   - driver_session_summary: ~300
   - team_performance: ~150 (15 sessions × ~10 teams)

5. If any query fails:
   - Table doesn't exist: dbt model hasn't run yet
   - Zero records: Check bronze layer first
   - Unexpected counts: Review dbt model logic

6. Performance Note:
   - Queries on silver_locations and bronze_locations are expensive (6M+ records)
   - Use LIMIT clauses when exploring data
*/
