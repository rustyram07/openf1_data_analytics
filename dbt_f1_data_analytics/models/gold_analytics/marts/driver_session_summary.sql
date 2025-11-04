{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Driver Session Summary
    Aggregated performance metrics per driver per session.
*/

WITH session_laps AS (
    SELECT
        f.session_key,
        f.driver_number,
        s.session_name,
        s.session_category,
        s.location,
        s.session_date,

        -- Lap counts
        COUNT(*) AS total_laps,
        COUNT(CASE WHEN f.is_valid_lap = TRUE THEN 1 END) AS valid_laps,
        COUNT(CASE WHEN f.is_pit_out_lap = TRUE THEN 1 END) AS pit_out_laps,

        -- Lap times
        MIN(CASE WHEN f.is_valid_lap = TRUE THEN f.lap_duration END) AS fastest_lap_time,
        MAX(CASE WHEN f.is_valid_lap = TRUE THEN f.lap_duration END) AS slowest_lap_time,
        AVG(CASE WHEN f.is_valid_lap = TRUE THEN f.lap_duration END) AS avg_lap_time,
        STDDEV(CASE WHEN f.is_valid_lap = TRUE THEN f.lap_duration END) AS lap_time_stddev,

        -- Sector times
        AVG(CASE WHEN f.is_valid_lap = TRUE THEN f.duration_sector_1 END) AS avg_sector_1_time,
        AVG(CASE WHEN f.is_valid_lap = TRUE THEN f.duration_sector_2 END) AS avg_sector_2_time,
        AVG(CASE WHEN f.is_valid_lap = TRUE THEN f.duration_sector_3 END) AS avg_sector_3_time,

        MIN(CASE WHEN f.is_valid_lap = TRUE THEN f.duration_sector_1 END) AS best_sector_1_time,
        MIN(CASE WHEN f.is_valid_lap = TRUE THEN f.duration_sector_2 END) AS best_sector_2_time,
        MIN(CASE WHEN f.is_valid_lap = TRUE THEN f.duration_sector_3 END) AS best_sector_3_time,

        -- Speed metrics
        MAX(f.max_speed) AS top_speed,
        AVG(CASE WHEN f.is_valid_lap = TRUE THEN f.max_speed END) AS avg_speed,

        -- Consistency metrics
        COUNT(CASE WHEN f.sector_completeness = 'Complete' THEN 1 END) AS complete_sector_laps

    FROM {{ ref('fact_laps') }} f
    INNER JOIN {{ ref('dim_sessions') }} s ON f.session_dim_key = s.session_dim_key
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['sl.session_key', 'sl.driver_number']) }} AS summary_key,
    {{ dbt_utils.generate_surrogate_key(['sl.session_key']) }} AS session_dim_key,
    {{ dbt_utils.generate_surrogate_key(['sl.driver_number']) }} AS driver_key,

    sl.session_key,
    sl.driver_number,
    d.full_name AS driver_name,
    d.team_name,
    d.team_category,
    sl.session_name,
    sl.session_category,
    sl.location,
    sl.session_date,

    -- Lap statistics
    sl.total_laps,
    sl.valid_laps,
    sl.pit_out_laps,
    ROUND(sl.valid_laps * 100.0 / NULLIF(sl.total_laps, 0), 2) AS valid_lap_percentage,

    -- Lap time metrics
    ROUND(sl.fastest_lap_time, 3) AS fastest_lap_time,
    ROUND(sl.slowest_lap_time, 3) AS slowest_lap_time,
    ROUND(sl.avg_lap_time, 3) AS avg_lap_time,
    ROUND(sl.lap_time_stddev, 3) AS lap_time_stddev,
    ROUND(sl.slowest_lap_time - sl.fastest_lap_time, 3) AS lap_time_range,

    -- Sector performance
    ROUND(sl.avg_sector_1_time, 3) AS avg_sector_1_time,
    ROUND(sl.avg_sector_2_time, 3) AS avg_sector_2_time,
    ROUND(sl.avg_sector_3_time, 3) AS avg_sector_3_time,
    ROUND(sl.best_sector_1_time, 3) AS best_sector_1_time,
    ROUND(sl.best_sector_2_time, 3) AS best_sector_2_time,
    ROUND(sl.best_sector_3_time, 3) AS best_sector_3_time,

    -- Theoretical best lap (sum of best sectors)
    ROUND(
        COALESCE(sl.best_sector_1_time, 0) +
        COALESCE(sl.best_sector_2_time, 0) +
        COALESCE(sl.best_sector_3_time, 0),
        3
    ) AS theoretical_best_lap,

    -- Speed metrics
    sl.top_speed,
    ROUND(sl.avg_speed, 1) AS avg_speed,

    -- Consistency
    sl.complete_sector_laps,
    ROUND(sl.lap_time_stddev / NULLIF(sl.avg_lap_time, 0), 4) AS consistency_coefficient,

    CURRENT_TIMESTAMP() AS created_at

FROM session_laps sl
INNER JOIN {{ ref('dim_drivers') }} d ON sl.driver_number = d.driver_number AND d.is_current = TRUE
