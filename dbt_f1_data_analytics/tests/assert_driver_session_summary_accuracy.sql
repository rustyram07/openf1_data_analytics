-- Singular test to validate driver_session_summary aggregations match source data
-- Tests that total_laps and valid_laps counts are accurate

with summary_counts as (
    select
        session_key,
        driver_number,
        total_laps,
        valid_laps
    from {{ ref('driver_session_summary') }}
),

actual_counts as (
    select
        session_key,
        driver_number,
        count(*) as actual_total_laps,
        sum(case when is_valid_lap = true then 1 else 0 end) as actual_valid_laps
    from {{ ref('silver_laps') }}
    group by session_key, driver_number
),

mismatches as (
    select
        s.session_key,
        s.driver_number,
        s.total_laps as summary_total,
        a.actual_total_laps,
        s.valid_laps as summary_valid,
        a.actual_valid_laps
    from summary_counts s
    join actual_counts a
        on s.session_key = a.session_key
        and s.driver_number = a.driver_number
    where s.total_laps != a.actual_total_laps
       or s.valid_laps != a.actual_valid_laps
)

select * from mismatches
