{{
    config(
        materialized='table',
        partition_by=['batch_day']
    )
}}

/*
    Gold Layer - Lap Fact Table
    Central fact table for lap-level analysis.
*/

WITH laps_with_ranking AS (
    SELECT
        l.*,
        -- Rank laps within session
        ROW_NUMBER() OVER (
            PARTITION BY l.session_key, l.driver_number
            ORDER BY l.lap_number
        ) AS driver_lap_rank,

        -- Rank lap times within session (fastest to slowest)
        CASE
            WHEN l.is_valid_lap = TRUE
            THEN ROW_NUMBER() OVER (
                PARTITION BY l.session_key
                ORDER BY l.lap_duration ASC
            )
            ELSE NULL
        END AS session_lap_time_rank,

        -- Best lap for driver in session
        MIN(CASE WHEN l.is_valid_lap = TRUE THEN l.lap_duration END) OVER (
            PARTITION BY l.session_key, l.driver_number
        ) AS driver_best_lap_time

    FROM {{ ref('silver_laps') }} l
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'lap_number']) }} AS lap_key,
    {{ dbt_utils.generate_surrogate_key(['session_key']) }} AS session_dim_key,
    {{ dbt_utils.generate_surrogate_key(['driver_number']) }} AS driver_key,

    -- Lap identifiers
    session_key,
    meeting_key,
    driver_number,
    lap_number,

    -- Lap timing
    date_start,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    calculated_lap_duration,
    sector_duration_delta,

    -- Lap flags
    is_pit_out_lap,
    is_valid_lap,

    -- Speed metrics
    intermediate_1_speed,
    intermediate_2_speed,
    speed_trap_speed,
    max_speed,

    -- Sector data
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,
    sector_completeness,

    -- Rankings
    driver_lap_rank,
    session_lap_time_rank,
    driver_best_lap_time,

    -- Delta from best
    CASE
        WHEN is_valid_lap = TRUE AND driver_best_lap_time IS NOT NULL
        THEN lap_duration - driver_best_lap_time
        ELSE NULL
    END AS delta_to_personal_best,

    -- Lap position indicator
    CASE
        WHEN lap_duration = driver_best_lap_time THEN TRUE
        ELSE FALSE
    END AS is_personal_best_lap,

    -- Metadata
    batch_day,
    session_name,
    updated_at

FROM laps_with_ranking
