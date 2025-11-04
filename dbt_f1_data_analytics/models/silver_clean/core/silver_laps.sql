{{
    config(
        materialized='incremental',
        unique_key=['session_key', 'driver_number', 'lap_number'],
        incremental_strategy='append',
        partition_by=['batch_day']
    )
}}

/*
    Silver Layer - Laps
    Cleaned lap timing data with calculated metrics.
*/

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_laps') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        date_start,
        lap_duration,
        is_pit_out_lap,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        i1_speed,
        i2_speed,
        st_speed,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        batch_day,
        session_name,
        _ingestion_timestamp
    FROM source
    WHERE session_key IS NOT NULL
      AND driver_number IS NOT NULL
      AND lap_number IS NOT NULL
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    date_start,
    lap_duration,
    COALESCE(is_pit_out_lap, FALSE) AS is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed AS intermediate_1_speed,
    i2_speed AS intermediate_2_speed,
    st_speed AS speed_trap_speed,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,

    -- Calculated sector durations
    COALESCE(duration_sector_1, 0) + COALESCE(duration_sector_2, 0) + COALESCE(duration_sector_3, 0) AS calculated_lap_duration,

    -- Sector delta (difference from sum vs reported lap duration)
    CASE
        WHEN lap_duration IS NOT NULL
        THEN lap_duration - (COALESCE(duration_sector_1, 0) + COALESCE(duration_sector_2, 0) + COALESCE(duration_sector_3, 0))
        ELSE NULL
    END AS sector_duration_delta,

    -- Valid lap indicator
    CASE
        WHEN lap_duration IS NULL THEN FALSE
        WHEN is_pit_out_lap = TRUE THEN FALSE
        WHEN lap_duration < 10 THEN FALSE  -- Unrealistically fast
        WHEN lap_duration > 300 THEN FALSE -- Too slow (5 minutes+)
        ELSE TRUE
    END AS is_valid_lap,

    -- Sector completeness
    CASE
        WHEN duration_sector_1 IS NOT NULL AND duration_sector_2 IS NOT NULL AND duration_sector_3 IS NOT NULL
        THEN 'Complete'
        WHEN duration_sector_1 IS NULL AND duration_sector_2 IS NULL AND duration_sector_3 IS NULL
        THEN 'No Sectors'
        ELSE 'Partial'
    END AS sector_completeness,

    -- Speed metrics
    GREATEST(
        COALESCE(i1_speed, 0),
        COALESCE(i2_speed, 0),
        COALESCE(st_speed, 0)
    ) AS max_speed,

    batch_day,
    session_name,
    _ingestion_timestamp,
    CURRENT_TIMESTAMP() AS updated_at

FROM cleaned
