{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Session Leaderboard
    Ranking of drivers by fastest lap in each session.
*/

WITH fastest_laps AS (
    SELECT
        f.session_key,
        f.driver_number,
        MIN(CASE WHEN f.is_valid_lap = TRUE THEN f.lap_duration END) AS fastest_lap_time,
        MIN(CASE WHEN f.is_valid_lap = TRUE THEN f.lap_number END) AS fastest_lap_number

    FROM {{ ref('fact_laps') }} f
    GROUP BY 1, 2
    HAVING fastest_lap_time IS NOT NULL
),

ranked AS (
    SELECT
        fl.*,
        s.session_name,
        s.session_category,
        s.location,
        s.session_date,
        s.year,
        d.full_name AS driver_name,
        d.team_name,
        d.team_category,

        -- Overall ranking in session
        ROW_NUMBER() OVER (
            PARTITION BY fl.session_key
            ORDER BY fl.fastest_lap_time ASC
        ) AS position,

        -- Team ranking
        ROW_NUMBER() OVER (
            PARTITION BY fl.session_key, d.team_category
            ORDER BY fl.fastest_lap_time ASC
        ) AS team_position,

        -- Delta to fastest in session
        fl.fastest_lap_time - MIN(fl.fastest_lap_time) OVER (
            PARTITION BY fl.session_key
        ) AS delta_to_fastest,

        -- Percentage gap to fastest
        ((fl.fastest_lap_time - MIN(fl.fastest_lap_time) OVER (PARTITION BY fl.session_key)) /
         MIN(fl.fastest_lap_time) OVER (PARTITION BY fl.session_key)) * 100 AS gap_percentage

    FROM fastest_laps fl
    INNER JOIN {{ ref('dim_sessions') }} s ON fl.session_key = s.session_key
    INNER JOIN {{ ref('dim_drivers') }} d ON fl.driver_number = d.driver_number AND d.is_current = TRUE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number']) }} AS leaderboard_key,
    {{ dbt_utils.generate_surrogate_key(['session_key']) }} AS session_dim_key,
    {{ dbt_utils.generate_surrogate_key(['driver_number']) }} AS driver_key,

    session_key,
    driver_number,
    driver_name,
    team_name,
    team_category,
    session_name,
    session_category,
    location,
    session_date,
    year,

    -- Position metrics
    position,
    team_position,

    -- Lap time metrics
    ROUND(fastest_lap_time, 3) AS fastest_lap_time,
    fastest_lap_number,
    ROUND(delta_to_fastest, 3) AS delta_to_fastest,
    ROUND(gap_percentage, 2) AS gap_percentage,

    -- Position indicators
    CASE WHEN position = 1 THEN TRUE ELSE FALSE END AS is_fastest,
    CASE WHEN position <= 3 THEN TRUE ELSE FALSE END AS is_top_3,
    CASE WHEN position <= 10 THEN TRUE ELSE FALSE END AS is_top_10,
    CASE WHEN team_position = 1 THEN TRUE ELSE FALSE END AS is_fastest_in_team,

    CURRENT_TIMESTAMP() AS created_at

FROM ranked
