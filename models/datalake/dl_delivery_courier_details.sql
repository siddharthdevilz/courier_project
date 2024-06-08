{{ config(materialized='table') }}

select
    order_id,
    courier,
    component,
    amount,
    currency
from {{ ref('delivery_courier_details') }}