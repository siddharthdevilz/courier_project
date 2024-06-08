{{ config(materialized='table') }}

select
    id,
    name
from {{ ref('pricing_plans') }}