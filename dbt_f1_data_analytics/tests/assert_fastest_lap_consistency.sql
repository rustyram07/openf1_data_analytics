-- Singular test to ensure the fastest lap in session_leaderboard matches fact_laps
-- The P1 driver's fastest_lap_time should match the minimum lap_duration for valid laps

with leaderboard_fastest as (
    select
        session_key,
        fastest_lap_time
    from {{ ref('session_leaderboard') }}
    where position = 1
),

fact_fastest as (
    select
        s.session_key,
        min(f.lap_duration) as actual_fastest_lap
    from {{ ref('fact_laps') }} f
    join {{ ref('dim_sessions') }} s
        on f.session_dim_key = s.session_dim_key
    where f.is_valid_lap = true
      and f.lap_duration is not null
    group by s.session_key
),

inconsistencies as (
    select
        l.session_key,
        l.fastest_lap_time as leaderboard_fastest,
        f.actual_fastest_lap as fact_fastest,
        abs(l.fastest_lap_time - f.actual_fastest_lap) as difference
    from leaderboard_fastest l
    join fact_fastest f
        on l.session_key = f.session_key
    where abs(l.fastest_lap_time - f.actual_fastest_lap) > 0.01  -- Allow small floating point differences
)

select * from inconsistencies
