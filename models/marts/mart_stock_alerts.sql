select
  product_id,
  snapshot_date,
  on_hand_qty,
  reorder_level,
  case when on_hand_qty < reorder_level then true else false end as is_low_stock
from {{ ref('stg_inventory') }}
where on_hand_qty < reorder_level
order by snapshot_date desc, product_id
