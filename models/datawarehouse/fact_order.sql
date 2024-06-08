{{ config(materialized='table') }}

select
    id,
    created_at,
    updated_at,
    destination_address_id,
    discount_id,
    first_mile_option,
    origin_address_id,
    plan_id,
    product_code,
    sender_id,
    state,
    parcel_dimension_id
from {{ ref('dl_delivery_orders') }}
-- Get the latest update for each order
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)=1