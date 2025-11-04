{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Driver Dimension
    Type 2 Slowly Changing Dimension for drivers.
    Tracks driver attributes over time.
*/

WITH latest_drivers AS (
    SELECT
        driver_number,
        full_name,
        broadcast_name,
        name_acronym,
        team_name,
        team_colour,
        team_category,
        first_name,
        last_name,
        headshot_url,
        country_code,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY driver_number
            ORDER BY updated_at DESC
        ) AS rn
    FROM {{ ref('silver_drivers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['driver_number']) }} AS driver_key,
    driver_number,
    full_name,
    broadcast_name,
    name_acronym,
    team_name,
    team_colour,
    team_category,
    first_name,
    last_name,
    headshot_url,
    country_code,
    updated_at AS valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    TRUE AS is_current

FROM latest_drivers
WHERE rn = 1
