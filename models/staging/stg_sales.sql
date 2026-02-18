with src as (
  select
    order_id,
    order_date,
    product_id,
    qty,
    unit_price,
    customer_id,
    (qty * unit_price) as revenue
  from {{ source('raw', 'RAW_SALES') }}
)

select * from src
