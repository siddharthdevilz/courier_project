{{ config(materialized='table') }}

select
    order_id,
    component,
    amount,
    currency
from {{ ref('delivery_price_components') }}