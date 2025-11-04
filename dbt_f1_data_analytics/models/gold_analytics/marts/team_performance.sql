{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Team Performance Summary
    Aggregated team performance metrics across sessions.
*/

WITH team_session_stats AS (
    SELECT
        dss.session_key,
        dss.team_category,
        dss.session_name,
        dss.session_category,
        dss.location,
        dss.session_date,

        -- Team metrics
        COUNT(DISTINCT dss.driver_number) AS drivers_count,
        SUM(dss.total_laps) AS team_total_laps,
        SUM(dss.valid_laps) AS team_valid_laps,

        -- Best performance from team
        MIN(dss.fastest_lap_time) AS team_fastest_lap,
        AVG(dss.fastest_lap_time) AS team_avg_fastest_lap,
        MAX(dss.top_speed) AS team_top_speed,
        AVG(dss.top_speed) AS team_avg_top_speed,

        -- Consistency
        AVG(dss.consistency_coefficient) AS team_avg_consistency,
        AVG(dss.lap_time_stddev) AS team_avg_lap_stddev

    FROM {{ ref('driver_session_summary') }} dss
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['session_key', 'team_category']) }} AS team_performance_key,
    {{ dbt_utils.generate_surrogate_key(['session_key']) }} AS session_dim_key,

    session_key,
    team_category AS team_name,
    session_name,
    session_category,
    location,
    session_date,

    -- Team size
    drivers_count,

    -- Lap statistics
    team_total_laps,
    team_valid_laps,
    ROUND(team_valid_laps * 100.0 / NULLIF(team_total_laps, 0), 2) AS team_valid_lap_percentage,

    -- Performance metrics
    ROUND(team_fastest_lap, 3) AS team_fastest_lap,
    ROUND(team_avg_fastest_lap, 3) AS team_avg_fastest_lap,
    ROUND(team_avg_fastest_lap - team_fastest_lap, 3) AS intra_team_gap,

    -- Speed metrics
    team_top_speed,
    ROUND(team_avg_top_speed, 1) AS team_avg_top_speed,

    -- Consistency metrics
    ROUND(team_avg_consistency, 4) AS team_avg_consistency,
    ROUND(team_avg_lap_stddev, 3) AS team_avg_lap_stddev,

    -- Session ranking
    ROW_NUMBER() OVER (
        PARTITION BY session_key
        ORDER BY team_fastest_lap ASC
    ) AS team_rank,

    CURRENT_TIMESTAMP() AS created_at

FROM team_session_stats
