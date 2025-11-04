-- Singular test to ensure leaderboard positions have no gaps
-- For each session, positions should be sequential: 1, 2, 3, ..., N

with position_check as (
    select
        session_key,
        position,
        row_number() over (partition by session_key order by position) as expected_position
    from {{ ref('session_leaderboard') }}
),

gaps as (
    select
        session_key,
        position as actual_position,
        expected_position,
        'Position gap detected' as issue
    from position_check
    where position != expected_position
)

select * from gaps
