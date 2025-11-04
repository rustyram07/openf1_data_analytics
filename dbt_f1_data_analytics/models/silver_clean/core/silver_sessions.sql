{{
    config(
        materialized='incremental',
        unique_key='session_key',
        incremental_strategy='merge',
        merge_update_columns=['session_name', 'location', 'country_name', 'updated_at']
    )
}}

/*
    Silver Layer - Sessions
    Cleaned and deduplicated session data with business logic applied.
*/

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_sessions') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        session_key,
        session_name,
        session_type,
        date_start,
        date_end,
        gmt_offset,
        meeting_key,
        location,
        country_key,
        country_code,
        country_name,
        circuit_key,
        circuit_short_name,
        year,
        _ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY session_key
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM source
    WHERE session_key IS NOT NULL
)

SELECT
    session_key,
    COALESCE(session_name, 'Unknown') AS session_name,
    COALESCE(session_type, session_name) AS session_type,
    date_start,
    date_end,
    gmt_offset,
    meeting_key,
    COALESCE(location, 'Unknown') AS location,
    country_key,
    country_code,
    COALESCE(country_name, 'Unknown') AS country_name,
    circuit_key,
    circuit_short_name,
    year,

    -- Session duration in minutes
    CASE
        WHEN date_start IS NOT NULL AND date_end IS NOT NULL
        THEN ROUND((UNIX_TIMESTAMP(CAST(date_end AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(date_start AS TIMESTAMP))) / 60.0, 2)
        ELSE NULL
    END AS session_duration_minutes,

    -- Session classification
    CASE
        WHEN LOWER(session_name) LIKE '%race%' THEN 'Race'
        WHEN LOWER(session_name) LIKE '%quali%' THEN 'Qualifying'
        WHEN LOWER(session_name) LIKE '%practice%' OR LOWER(session_name) LIKE '%fp%' THEN 'Practice'
        WHEN LOWER(session_name) LIKE '%sprint%' THEN 'Sprint'
        ELSE 'Other'
    END AS session_category,

    -- Metadata
    _ingestion_timestamp,
    CURRENT_TIMESTAMP() AS updated_at

FROM deduplicated
WHERE rn = 1
