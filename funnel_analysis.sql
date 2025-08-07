with events as (
    select
        user_id,
        event_name,
        event_timestamp::date as event_date
    from {{ ref('events') }}
),

signup as (
    select user_id, min(event_date) as signup_date
    from events
    where event_name = 'sign_up'
    group by 1
),

product_view as (
    select user_id, min(event_date) as view_date
    from events
    where event_name = 'view_product'
    group by 1
),

purchase as (
    select user_id, min(event_date) as purchase_date
    from events
    where event_name = 'purchase'
    group by 1
),

funnel as (
    select
        signup.user_id,
        signup.signup_date,
        product_view.view_date,
        purchase.purchase_date
    from signup
    left join product_view using(user_id)
    left join purchase using(user_id)
)

select
    count(distinct user_id) as total_signups,
    count(distinct case when view_date is not null then user_id end) as viewed_product,
    count(distinct case when purchase_date is not null then user_id end) as made_purchase,
    round(100.0 * count(distinct case when view_date is not null then user_id end)/count(distinct user_id),2) as pct_viewed,
    round(100.0 * count(distinct case when purchase_date is not null then user_id end)/count(distinct user_id),2) as pct_purchased
from funnel