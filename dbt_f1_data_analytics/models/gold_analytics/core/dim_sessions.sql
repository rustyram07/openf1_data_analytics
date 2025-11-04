{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Session Dimension
    Dimension table for sessions with enriched attributes.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['session_key']) }} AS session_dim_key,
    session_key,
    session_name,
    session_type,
    session_category,
    date_start,
    date_end,
    session_duration_minutes,
    gmt_offset,
    meeting_key,
    location,
    country_key,
    country_code,
    country_name,
    circuit_key,
    circuit_short_name,
    year,

    -- Date dimensions
    DATE(CAST(date_start AS TIMESTAMP)) AS session_date,
    YEAR(CAST(date_start AS TIMESTAMP)) AS session_year,
    MONTH(CAST(date_start AS TIMESTAMP)) AS session_month,
    DAYOFWEEK(CAST(date_start AS TIMESTAMP)) AS session_day_of_week,
    WEEKOFYEAR(CAST(date_start AS TIMESTAMP)) AS session_week_of_year,
    QUARTER(CAST(date_start AS TIMESTAMP)) AS session_quarter,

    -- Session flags
    CASE WHEN session_category = 'Race' THEN TRUE ELSE FALSE END AS is_race,
    CASE WHEN session_category = 'Qualifying' THEN TRUE ELSE FALSE END AS is_qualifying,
    CASE WHEN session_category = 'Practice' THEN TRUE ELSE FALSE END AS is_practice,
    CASE WHEN session_category = 'Sprint' THEN TRUE ELSE FALSE END AS is_sprint,

    updated_at

FROM {{ ref('silver_sessions') }}
