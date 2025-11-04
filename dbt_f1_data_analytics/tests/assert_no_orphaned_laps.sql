-- Singular test to ensure all laps have corresponding driver and session records
-- This test should return zero rows if all relationships are valid

with lap_data as (
    select distinct
        session_key,
        driver_number
    from {{ ref('silver_laps') }}
),

driver_data as (
    select distinct
        session_key,
        driver_number
    from {{ ref('silver_drivers') }}
),

session_data as (
    select distinct
        session_key
    from {{ ref('silver_sessions') }}
),

orphaned_laps as (
    select
        l.session_key,
        l.driver_number,
        'Missing driver record' as issue
    from lap_data l
    left join driver_data d
        on l.session_key = d.session_key
        and l.driver_number = d.driver_number
    where d.driver_number is null

    union all

    select
        l.session_key,
        l.driver_number,
        'Missing session record' as issue
    from lap_data l
    left join session_data s
        on l.session_key = s.session_key
    where s.session_key is null
)

select * from orphaned_laps
