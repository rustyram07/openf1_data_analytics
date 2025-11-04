-- Complete Validation Query for dbt Silver, Gold, and Mart Tables
-- Run this in Databricks SQL Editor

-- IMPORTANT: Actual Schema Names
-- Silver: dev_f1_data_analytics.default_silver_clean
-- Gold: dev_f1_data_analytics.default_gold_analytics

-- =====================================================
-- PART 1: Record Counts for All Tables
-- =====================================================

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
SELECT 'GOLD-DIM', 'dim_sessions',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions)
UNION ALL
SELECT 'GOLD-DIM', 'dim_drivers',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_drivers)
UNION ALL
SELECT 'GOLD-FACT', 'fact_laps',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.fact_laps)
UNION ALL
SELECT 'GOLD-MART', 'session_leaderboard',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard)
UNION ALL
SELECT 'GOLD-MART', 'driver_session_summary',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary)
UNION ALL
SELECT 'GOLD-MART', 'team_performance',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.team_performance)
ORDER BY layer, table_name;

-- =====================================================
-- PART 2: Session Leaderboard - Top 10 per Session
-- =====================================================

SELECT
    session_name,
    session_category,
    location,
    position,
    driver_name,
    team_name,
    ROUND(fastest_lap_time, 3) AS fastest_lap,
    ROUND(delta_to_fastest, 3) AS delta,
    ROUND(gap_percentage, 2) AS gap_pct,
    is_top_3,
    is_fastest_in_team
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard
WHERE position <= 10
ORDER BY session_name, position;

-- =====================================================
-- PART 3: Driver Performance Summary
-- =====================================================

SELECT
    driver_name,
    team_name,
    session_name,
    session_category,
    total_laps,
    valid_laps,
    ROUND(fastest_lap_time, 3) AS fastest_lap,
    ROUND(avg_lap_time, 3) AS avg_lap,
    ROUND(lap_time_stddev, 3) AS lap_stddev,
    ROUND(consistency_coefficient, 4) AS consistency,
    top_speed
FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary
ORDER BY session_name, fastest_lap_time;

-- =====================================================
-- PART 4: Team Performance Rankings
-- =====================================================

SELECT
    session_name,
    session_category,
    team_rank,
    team_name,
    drivers_count,
    ROUND(team_fastest_lap, 3) AS team_fastest,
    ROUND(team_avg_fastest_lap, 3) AS team_avg,
    ROUND(intra_team_gap, 3) AS gap_between_drivers,
    ROUND(team_avg_consistency, 4) AS avg_consistency,
    team_top_speed
FROM dev_f1_data_analytics.default_gold_analytics.team_performance
WHERE session_category = 'Race'
ORDER BY session_name, team_rank;

-- =====================================================
-- PART 5: Data Quality Checks
-- =====================================================

-- Check for NULL values in critical fields
SELECT
    'dim_sessions - NULL session_key' AS check_name,
    COUNT(*) AS issue_count
FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions
WHERE session_key IS NULL
UNION ALL
SELECT
    'fact_laps - NULL lap_duration (valid laps)',
    COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.fact_laps
WHERE lap_duration IS NULL AND is_valid_lap = TRUE
UNION ALL
SELECT
    'session_leaderboard - NULL position',
    COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard
WHERE position IS NULL
UNION ALL
SELECT
    'driver_session_summary - NULL fastest_lap_time',
    COUNT(*)
FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary
WHERE fastest_lap_time IS NULL;

-- =====================================================
-- PART 6: Data Lineage Validation
-- =====================================================

-- Check consistency across layers
SELECT
    'Sessions: Silver vs Gold' AS check_name,
    (SELECT COUNT(DISTINCT session_key) FROM dev_f1_data_analytics.default_silver_clean.silver_sessions) AS silver_count,
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions) AS gold_count,
    CASE
        WHEN (SELECT COUNT(DISTINCT session_key) FROM dev_f1_data_analytics.default_silver_clean.silver_sessions) =
             (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.dim_sessions)
        THEN 'PASS ✓'
        ELSE 'FAIL ✗'
    END AS status
UNION ALL
SELECT
    'Laps: Silver vs Gold',
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_laps),
    (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.fact_laps),
    CASE
        WHEN (SELECT COUNT(*) FROM dev_f1_data_analytics.default_silver_clean.silver_laps) =
             (SELECT COUNT(*) FROM dev_f1_data_analytics.default_gold_analytics.fact_laps)
        THEN 'PASS ✓'
        ELSE 'FAIL ✗'
    END;

-- =====================================================
-- PART 7: Business Logic Validation
-- =====================================================

-- Verify leaderboard position = 1 is the fastest lap in session
SELECT
    session_name,
    COUNT(*) AS sessions_validated,
    SUM(CASE WHEN is_fastest = TRUE AND position = 1 THEN 1 ELSE 0 END) AS correct_fastest_flags,
    CASE
        WHEN COUNT(*) = SUM(CASE WHEN is_fastest = TRUE AND position = 1 THEN 1 ELSE 0 END)
        THEN 'PASS ✓'
        ELSE 'FAIL ✗'
    END AS validation_status
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard
WHERE position = 1
GROUP BY session_name;

-- Verify team performance has correct driver counts
SELECT
    session_name,
    team_name,
    drivers_count AS reported_count,
    (SELECT COUNT(DISTINCT driver_number)
     FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary dss
     WHERE dss.session_name = tp.session_name
       AND dss.team_name = tp.team_name) AS actual_count,
    CASE
        WHEN drivers_count = (SELECT COUNT(DISTINCT driver_number)
                              FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary dss
                              WHERE dss.session_name = tp.session_name
                                AND dss.team_name = tp.team_name)
        THEN 'PASS ✓'
        ELSE 'FAIL ✗'
    END AS validation_status
FROM dev_f1_data_analytics.default_gold_analytics.team_performance tp
ORDER BY session_name, team_name;

-- =====================================================
-- PART 8: Sample Queries for Dashboard Use
-- =====================================================

-- Top 5 fastest laps across all sessions
SELECT
    sl.session_name,
    sl.location,
    sl.driver_name,
    sl.team_name,
    ROUND(sl.fastest_lap_time, 3) AS lap_time,
    sl.position
FROM dev_f1_data_analytics.default_gold_analytics.session_leaderboard sl
WHERE sl.session_category = 'Qualifying'
ORDER BY sl.fastest_lap_time ASC
LIMIT 5;

-- Driver consistency rankings
SELECT
    driver_name,
    team_name,
    session_name,
    ROUND(consistency_coefficient, 4) AS consistency,
    valid_laps,
    ROUND(lap_time_stddev, 3) AS stddev
FROM dev_f1_data_analytics.default_gold_analytics.driver_session_summary
WHERE valid_laps >= 10  -- Only drivers with significant laps
ORDER BY consistency_coefficient ASC
LIMIT 10;

-- Team battle - which team performed best across sessions
SELECT
    team_name,
    COUNT(*) AS sessions_participated,
    AVG(team_rank) AS avg_rank,
    MIN(team_rank) AS best_rank,
    AVG(team_fastest_lap) AS avg_fastest_lap,
    AVG(team_avg_consistency) AS avg_consistency
FROM dev_f1_data_analytics.default_gold_analytics.team_performance
WHERE session_category = 'Race'
GROUP BY team_name
ORDER BY avg_rank ASC;
