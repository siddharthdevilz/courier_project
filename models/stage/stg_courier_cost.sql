{{ config(materialized='table') }}

select
    order_id,
    courier,
    -- component,
    -- Assuming component is either fuel_surcharge_ex_tax or base_cost_ex_tax
    CASE
      WHEN component = 'base_cost_ex_tax' THEN amount
      ELSE 0
    END AS base_cost_ex_tax,
    CASE
      WHEN component = 'fuel_surcharge_ex_tax' THEN amount
      ELSE 0
    END AS fuel_surcharge_ex_tax,
    -- amount,
    currency
from {{ ref('delivery_courier_details') }}