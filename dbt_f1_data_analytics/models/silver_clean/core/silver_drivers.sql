{{
    config(
        materialized='incremental',
        unique_key=['session_key', 'driver_number'],
        incremental_strategy='merge',
        merge_update_columns=['team_name', 'team_colour', 'updated_at']
    )
}}

/*
    Silver Layer - Drivers
    Cleaned and deduplicated driver data with enrichments.
*/

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_drivers') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        session_key,
        meeting_key,
        driver_number,
        broadcast_name,
        full_name,
        name_acronym,
        team_name,
        team_colour,
        first_name,
        last_name,
        headshot_url,
        country_code,
        _ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY session_key, driver_number
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM source
    WHERE session_key IS NOT NULL
      AND driver_number IS NOT NULL
)

SELECT
    session_key,
    meeting_key,
    driver_number,
    COALESCE(broadcast_name, full_name, CONCAT(first_name, ' ', last_name)) AS broadcast_name,
    COALESCE(full_name, broadcast_name) AS full_name,
    name_acronym,
    COALESCE(team_name, 'Unknown Team') AS team_name,
    COALESCE(team_colour, '000000') AS team_colour,
    first_name,
    last_name,
    headshot_url,
    country_code,

    -- Team category (for grouping)
    CASE
        WHEN LOWER(team_name) LIKE '%red bull%' THEN 'Red Bull Racing'
        WHEN LOWER(team_name) LIKE '%ferrari%' THEN 'Ferrari'
        WHEN LOWER(team_name) LIKE '%mercedes%' THEN 'Mercedes'
        WHEN LOWER(team_name) LIKE '%mclaren%' THEN 'McLaren'
        WHEN LOWER(team_name) LIKE '%alpine%' THEN 'Alpine'
        WHEN LOWER(team_name) LIKE '%aston martin%' THEN 'Aston Martin'
        WHEN LOWER(team_name) LIKE '%williams%' THEN 'Williams'
        WHEN LOWER(team_name) LIKE '%alphatauri%' OR LOWER(team_name) LIKE '%rb %' THEN 'AlphaTauri/RB'
        WHEN LOWER(team_name) LIKE '%alfa romeo%' OR LOWER(team_name) LIKE '%sauber%' THEN 'Alfa Romeo/Sauber'
        WHEN LOWER(team_name) LIKE '%haas%' THEN 'Haas'
        ELSE 'Other'
    END AS team_category,

    -- Metadata
    _ingestion_timestamp,
    CURRENT_TIMESTAMP() AS updated_at

FROM deduplicated
WHERE rn = 1
