select
  order_date,
  count(distinct order_id) as orders,
  sum(revenue) as total_revenue,
  round(sum(revenue) / nullif(count(distinct order_id),0), 2) as avg_order_value
from {{ ref('stg_sales') }}
group by 1
order by 1
