-- Test: Ensure each driver has exactly one current record
-- This validates the SCD Type 2 implementation for dim_drivers
--
-- Expected behavior: Each driver_number should have exactly 1 record where is_current = TRUE
-- If this test fails, it means:
--   - A driver has multiple current records (data quality issue)
--   - A driver has no current records (data quality issue)

WITH driver_current_counts AS (
    SELECT
        driver_number,
        COUNT(CASE WHEN is_current = TRUE THEN 1 END) AS current_count
    FROM {{ ref('dim_drivers') }}
    GROUP BY driver_number
)

SELECT
    driver_number,
    current_count
FROM driver_current_counts
WHERE current_count != 1  -- Should have exactly 1 current record per driver
