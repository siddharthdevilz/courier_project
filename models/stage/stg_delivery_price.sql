{{ config(materialized='table') }}

select
    order_id,
    -- component,
    -- Assuming component is either fuel_surcharge_ex_tax or base_price_ex_tax
    CASE
      WHEN component = 'base_price_ex_tax' THEN amount
      ELSE 0
    END AS base_price_ex_tax,
    CASE
      WHEN component = 'fuel_surcharge_ex_tax' THEN amount
      ELSE 0
    END AS fuel_surcharge_ex_tax,
    -- amount,
    currency
from {{ ref('delivery_price_components') }}