with user_events as (
    select
        user_id,
        event_timestamp::date as event_date
    from {{ ref('events') }}
    where event_name = 'sign_in'
),

cohorts as (
    select
        user_id,
        min(event_date) as cohort_date
    from user_events
    group by user_id
),

event_with_cohort as (
    select
        ue.user_id,
        ue.event_date,
        c.cohort_date,
        datediff(day, c.cohort_date, ue.event_date) as days_after_signup
    from user_events ue
    join cohorts c using(user_id)
),

weekly_retention as (
    select
        cohort_date,
        floor(days_after_signup/7) as week_number,
        count(distinct user_id) as active_users
    from event_with_cohort
    group by 1,2
)

select
    cohort_date,
    week_number,
    active_users
from weekly_retention
order by cohort_date, week_number
