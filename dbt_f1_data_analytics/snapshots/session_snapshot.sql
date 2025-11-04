{% snapshot session_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='session_key',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot for Sessions
    Tracks changes to session metadata over time.
*/

SELECT
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
    updated_at
FROM {{ ref('silver_sessions') }}
WHERE session_key IS NOT NULL

{% endsnapshot %}
