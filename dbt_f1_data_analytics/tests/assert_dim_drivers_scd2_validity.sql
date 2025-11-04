-- Singular test to validate SCD Type 2 logic in dim_drivers
-- Ensures valid_from < valid_to for historical records and only one current record per driver

with scd_validation as (
    -- Check that historical records have valid_from < valid_to
    select
        driver_key,
        driver_number,
        is_current,
        valid_from,
        valid_to,
        'valid_from >= valid_to for historical record' as issue
    from {{ ref('dim_drivers') }}
    where is_current = false
      and valid_from >= valid_to

    union all

    -- Check that current records have NULL valid_to
    select
        driver_key,
        driver_number,
        is_current,
        valid_from,
        valid_to,
        'Current record has non-NULL valid_to' as issue
    from {{ ref('dim_drivers') }}
    where is_current = true
      and valid_to is not null

    union all

    -- Check for overlapping validity periods for the same driver
    select
        d1.driver_key,
        d1.driver_number,
        d1.is_current,
        d1.valid_from,
        d1.valid_to,
        'Overlapping validity periods' as issue
    from {{ ref('dim_drivers') }} d1
    join {{ ref('dim_drivers') }} d2
        on d1.driver_number = d2.driver_number
        and d1.driver_key != d2.driver_key
    where d1.valid_from < coalesce(d2.valid_to, current_date() + interval 1 day)
      and coalesce(d1.valid_to, current_date() + interval 1 day) > d2.valid_from
)

select * from scd_validation
