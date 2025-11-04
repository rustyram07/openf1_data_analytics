{{
    config(
        materialized='incremental',
        unique_key=['session_key', 'driver_number', 'date'],
        incremental_strategy='append',
        partition_by=['batch_day']
    )
}}

/*
    Silver Layer - Locations
    Cleaned location telemetry data.
    This is the largest table and requires careful partitioning.
*/

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_locations') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        session_key,
        meeting_key,
        driver_number,
        date,
        x,
        y,
        z,
        batch_day,
        session_name,
        _ingestion_timestamp
    FROM source
    WHERE session_key IS NOT NULL
      AND driver_number IS NOT NULL
      AND date IS NOT NULL
      AND x IS NOT NULL
      AND y IS NOT NULL
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    date,
    x AS position_x,
    y AS position_y,
    COALESCE(z, 0) AS position_z,

    -- Calculate distance from origin (can be used for track position analysis)
    SQRT(POWER(x, 2) + POWER(y, 2)) AS distance_from_origin,

    batch_day,
    session_name,
    _ingestion_timestamp,
    CURRENT_TIMESTAMP() AS updated_at

FROM cleaned
