-- ============================================================================
-- Bronze Layer Verification SQL Queries
-- Use these queries in Databricks SQL Editor to verify bronze tables
-- ============================================================================

-- ============================================================================
-- SECTION 1: TABLE EXISTENCE & STRUCTURE
-- ============================================================================

-- Query 1.1: Show all bronze tables
SHOW TABLES IN dev_f1_data_analytics.bronze_raw;

-- Query 1.2: Describe bronze_sessions table
DESCRIBE dev_f1_data_analytics.bronze_raw.bronze_sessions;

-- Query 1.3: Describe bronze_drivers table
DESCRIBE dev_f1_data_analytics.bronze_raw.bronze_drivers;

-- Query 1.4: Describe bronze_laps table
DESCRIBE dev_f1_data_analytics.bronze_raw.bronze_laps;

-- Query 1.5: Describe bronze_locations table (if exists)
DESCRIBE dev_f1_data_analytics.bronze_raw.bronze_locations;


-- ============================================================================
-- SECTION 2: RECORD COUNTS
-- ============================================================================

-- Query 2.1: Count all bronze tables
SELECT 'bronze_sessions' as table_name, COUNT(*) as record_count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
UNION ALL
SELECT 'bronze_drivers', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
UNION ALL
SELECT 'bronze_laps', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_laps;

-- Query 2.2: If locations table exists
-- SELECT 'bronze_locations', COUNT(*)
-- FROM dev_f1_data_analytics.bronze_raw.bronze_locations;

-- Query 2.3: Expected vs Actual counts
SELECT
    'Expected' as type,
    'bronze_sessions' as table_name,
    123 as count
UNION ALL
SELECT 'Expected', 'bronze_drivers', 2460
UNION ALL
SELECT 'Expected', 'bronze_laps', 110000
UNION ALL
SELECT 'Actual', 'bronze_sessions', (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions)
UNION ALL
SELECT 'Actual', 'bronze_drivers', (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_drivers)
UNION ALL
SELECT 'Actual', 'bronze_laps', (SELECT COUNT(*) FROM dev_f1_data_analytics.bronze_raw.bronze_laps)
ORDER BY table_name, type;


-- ============================================================================
-- SECTION 3: SAMPLE DATA
-- ============================================================================

-- Query 3.1: Latest 10 sessions
SELECT
    session_key,
    session_name,
    location,
    date_start,
    year,
    _ingestion_timestamp
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
ORDER BY date_start DESC
LIMIT 10;

-- Query 3.2: Drivers from latest session
SELECT
    session_key,
    driver_number,
    full_name,
    name_acronym,
    team_name,
    _ingestion_timestamp
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
WHERE session_key = (SELECT MAX(session_key) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions)
ORDER BY driver_number;

-- Query 3.3: Sample laps from latest session, driver 1
SELECT
    session_key,
    driver_number,
    lap_number,
    lap_duration,
    sector_1_duration,
    sector_2_duration,
    sector_3_duration,
    _ingestion_timestamp
FROM dev_f1_data_analytics.bronze_raw.bronze_laps
WHERE session_key = (SELECT MAX(session_key) FROM dev_f1_data_analytics.bronze_raw.bronze_sessions)
AND driver_number = 1
ORDER BY lap_number
LIMIT 10;


-- ============================================================================
-- SECTION 4: DATA QUALITY CHECKS
-- ============================================================================

-- Query 4.1: Check for duplicate session keys
SELECT
    session_key,
    COUNT(*) as duplicate_count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
GROUP BY session_key
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Expected: No rows (no duplicates)

-- Query 4.2: Check for sessions with missing drivers
SELECT
    s.session_key,
    s.session_name,
    s.location,
    COUNT(DISTINCT d.driver_number) as driver_count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions s
LEFT JOIN dev_f1_data_analytics.bronze_raw.bronze_drivers d
ON s.session_key = d.session_key
GROUP BY s.session_key, s.session_name, s.location
HAVING COUNT(DISTINCT d.driver_number) < 20
ORDER BY driver_count;

-- Expected: Few or no rows (most sessions have 20 drivers)

-- Query 4.3: Check for sessions with no laps
SELECT
    s.session_key,
    s.session_name,
    s.location,
    COUNT(l.lap_number) as lap_count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions s
LEFT JOIN dev_f1_data_analytics.bronze_raw.bronze_laps l
ON s.session_key = l.session_key
GROUP BY s.session_key, s.session_name, s.location
HAVING COUNT(l.lap_number) = 0
ORDER BY s.date_start DESC;

-- Expected: Some rows (practice sessions may have limited laps)

-- Query 4.4: Check for NULL values in key columns
SELECT
    'bronze_sessions' as table_name,
    'session_key' as column_name,
    COUNT(*) as null_count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
WHERE session_key IS NULL
UNION ALL
SELECT 'bronze_drivers', 'driver_number', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
WHERE driver_number IS NULL
UNION ALL
SELECT 'bronze_laps', 'lap_number', COUNT(*)
FROM dev_f1_data_analytics.bronze_raw.bronze_laps
WHERE lap_number IS NULL;

-- Expected: All 0 (no nulls in key columns)


-- ============================================================================
-- SECTION 5: DATA DISTRIBUTION & STATISTICS
-- ============================================================================

-- Query 5.1: Sessions by year
SELECT
    year,
    COUNT(*) as session_count,
    COUNT(DISTINCT location) as unique_locations
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
GROUP BY year
ORDER BY year DESC;

-- Query 5.2: Sessions by type
SELECT
    session_name,
    COUNT(*) as session_count
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
GROUP BY session_name
ORDER BY session_count DESC;

-- Query 5.3: Drivers by team
SELECT
    team_name,
    COUNT(DISTINCT driver_number) as unique_drivers,
    COUNT(*) as driver_session_records
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
GROUP BY team_name
ORDER BY unique_drivers DESC, team_name;

-- Query 5.4: Lap statistics by session type
SELECT
    s.session_name,
    COUNT(DISTINCT s.session_key) as session_count,
    COUNT(l.lap_number) as total_laps,
    ROUND(AVG(l.lap_duration), 3) as avg_lap_duration,
    ROUND(MIN(l.lap_duration), 3) as min_lap_duration,
    ROUND(MAX(l.lap_duration), 3) as max_lap_duration
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions s
JOIN dev_f1_data_analytics.bronze_raw.bronze_laps l
ON s.session_key = l.session_key
GROUP BY s.session_name
ORDER BY session_count DESC;

-- Query 5.5: Driver participation across sessions
SELECT
    d.driver_number,
    MAX(d.full_name) as driver_name,
    MAX(d.name_acronym) as acronym,
    COUNT(DISTINCT d.session_key) as sessions_participated
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers d
GROUP BY d.driver_number
ORDER BY sessions_participated DESC, d.driver_number
LIMIT 25;


-- ============================================================================
-- SECTION 6: COMPREHENSIVE SUMMARY
-- ============================================================================

-- Query 6.1: Overall bronze layer summary
SELECT
    'Total Sessions' as metric,
    CAST(COUNT(*) AS STRING) as value
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
UNION ALL
SELECT
    'Total Drivers (records)',
    CAST(COUNT(*) AS STRING)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
UNION ALL
SELECT
    'Unique Drivers',
    CAST(COUNT(DISTINCT driver_number) AS STRING)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
UNION ALL
SELECT
    'Total Laps',
    CAST(COUNT(*) AS STRING)
FROM dev_f1_data_analytics.bronze_raw.bronze_laps
UNION ALL
SELECT
    'Unique Sessions in 2024',
    CAST(COUNT(DISTINCT session_key) AS STRING)
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
WHERE year = 2024
UNION ALL
SELECT
    'Date Range',
    CONCAT(
        CAST(MIN(date_start) AS STRING),
        ' to ',
        CAST(MAX(date_start) AS STRING)
    )
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
UNION ALL
SELECT
    'Unique Locations',
    CAST(COUNT(DISTINCT location) AS STRING)
FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
UNION ALL
SELECT
    'Unique Teams',
    CAST(COUNT(DISTINCT team_name) AS STRING)
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers;

-- Query 6.2: Data freshness check
SELECT
    table_name,
    MAX(_ingestion_timestamp) as latest_ingestion,
    MIN(_ingestion_timestamp) as earliest_ingestion
FROM (
    SELECT 'bronze_sessions' as table_name, _ingestion_timestamp
    FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
    UNION ALL
    SELECT 'bronze_drivers', _ingestion_timestamp
    FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
    UNION ALL
    SELECT 'bronze_laps', _ingestion_timestamp
    FROM dev_f1_data_analytics.bronze_raw.bronze_laps
)
GROUP BY table_name
ORDER BY table_name;


-- ============================================================================
-- SECTION 7: RELATIONSHIP VALIDATION
-- ============================================================================

-- Query 7.1: Orphaned drivers (drivers without sessions)
SELECT d.*
FROM dev_f1_data_analytics.bronze_raw.bronze_drivers d
LEFT JOIN dev_f1_data_analytics.bronze_raw.bronze_sessions s
ON d.session_key = s.session_key
WHERE s.session_key IS NULL
LIMIT 10;

-- Expected: No rows

-- Query 7.2: Orphaned laps (laps without sessions)
SELECT l.*
FROM dev_f1_data_analytics.bronze_raw.bronze_laps l
LEFT JOIN dev_f1_data_analytics.bronze_raw.bronze_sessions s
ON l.session_key = s.session_key
WHERE s.session_key IS NULL
LIMIT 10;

-- Expected: No rows

-- Query 7.3: Laps without corresponding drivers
SELECT
    l.session_key,
    l.driver_number,
    COUNT(*) as lap_count
FROM dev_f1_data_analytics.bronze_raw.bronze_laps l
LEFT JOIN dev_f1_data_analytics.bronze_raw.bronze_drivers d
ON l.session_key = d.session_key
AND l.driver_number = d.driver_number
WHERE d.driver_number IS NULL
GROUP BY l.session_key, l.driver_number
LIMIT 10;

-- Expected: Few or no rows


-- ============================================================================
-- SECTION 8: PERFORMANCE & OPTIMIZATION CHECKS
-- ============================================================================

-- Query 8.1: Table statistics
DESCRIBE EXTENDED dev_f1_data_analytics.bronze_raw.bronze_sessions;

-- Query 8.2: Show table properties
SHOW TBLPROPERTIES dev_f1_data_analytics.bronze_raw.bronze_sessions;

-- Query 8.3: Show table history (last 10 operations)
DESCRIBE HISTORY dev_f1_data_analytics.bronze_raw.bronze_sessions
LIMIT 10;


-- ============================================================================
-- SECTION 9: QUICK VALIDATION (RUN THIS FIRST!)
-- ============================================================================

-- Query 9.1: Quick health check - Run this first!
WITH table_counts AS (
    SELECT 'bronze_sessions' as table_name, COUNT(*) as actual_count, 123 as expected_count
    FROM dev_f1_data_analytics.bronze_raw.bronze_sessions
    UNION ALL
    SELECT 'bronze_drivers', COUNT(*), 2460
    FROM dev_f1_data_analytics.bronze_raw.bronze_drivers
    UNION ALL
    SELECT 'bronze_laps', COUNT(*), 110000
    FROM dev_f1_data_analytics.bronze_raw.bronze_laps
)
SELECT
    table_name,
    actual_count,
    expected_count,
    CASE
        WHEN actual_count >= expected_count * 0.9 THEN '✅ PASS'
        WHEN actual_count = 0 THEN '❌ EMPTY'
        ELSE '⚠️  LOW'
    END as status
FROM table_counts
ORDER BY table_name;

-- Expected output:
-- bronze_sessions   123      123       ✅ PASS
-- bronze_drivers    2,460    2,460     ✅ PASS
-- bronze_laps       110,000  110,000   ✅ PASS


-- ============================================================================
-- NOTES:
-- - Run Query 9.1 first for quick validation
-- - If all tables show ✅ PASS, bronze layer is ready
-- - Some queries are commented out (locations table) - uncomment when available
-- - Expected counts are approximate for laps (varies by session type)
-- ============================================================================
