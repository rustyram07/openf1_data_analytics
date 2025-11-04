{% snapshot driver_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='driver_number',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot for Drivers
    Tracks changes to driver attributes over time, particularly team changes.
*/

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
    updated_at
FROM {{ ref('silver_drivers') }}
WHERE driver_number IS NOT NULL

{% endsnapshot %}
