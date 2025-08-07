with payments as (
    select
        user_id,
        amount,
        status
    from {{ ref('payments') }}
    where status = 'success'
),

clv as (
    select
        user_id,
        sum(amount) as lifetime_value,
        count(*) as successful_payments
    from payments
    group by user_id
)

select * from clv