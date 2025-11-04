-- Singular test to validate team_performance driver counts match actual drivers
-- Ensures drivers_count in team_performance is accurate

with team_driver_counts as (
    select
        session_key,
        team_name,
        drivers_count
    from {{ ref('team_performance') }}
),

actual_driver_counts as (
    select
        session_key,
        team_name,
        count(distinct driver_number) as actual_count
    from {{ ref('silver_drivers') }}
    group by session_key, team_name
),

mismatches as (
    select
        t.session_key,
        t.team_name,
        t.drivers_count as reported_count,
        a.actual_count,
        (t.drivers_count - a.actual_count) as difference
    from team_driver_counts t
    join actual_driver_counts a
        on t.session_key = a.session_key
        and t.team_name = a.team_name
    where t.drivers_count != a.actual_count
)

select * from mismatches
