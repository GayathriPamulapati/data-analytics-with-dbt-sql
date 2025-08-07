-- Dimension: customers
create or replace view {{ ref('dim_customers') }} as
select
    id as customer_id,
    first_name,
    last_name,
    email,
    created_at::date as signup_date
from {{ ref('customers') }};


-- Fact: orders
create or replace view {{ ref('fact_orders') }} as
select
    id as order_id,
    user_id as customer_id,
    order_date::date,
    status,
    amount
from {{ ref('orders') }};