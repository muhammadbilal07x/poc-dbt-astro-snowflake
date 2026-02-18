with src as (
  select
    product_id,
    snapshot_date,
    on_hand_qty,
    reorder_level
  from {{ source('raw', 'RAW_INVENTORY') }}
)

select * from src
