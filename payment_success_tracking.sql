with all_payments as (
    select
        payment_id,
        user_id,
        amount,
        status,
        payment_date::date
    from {{ ref('payments') }}
),

aggregated as (
    select
        status,
        count(*) as total_transactions,
        sum(amount) as total_amount
    from all_payments
    group by status
)

select * from aggregated