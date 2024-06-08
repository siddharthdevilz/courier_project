{{ config(materialized='table') }}

select
    id,
    name,
    -- Generating random value for a value missing from the table
    floor(random() * 10) as discount_ex_tax
from {{ ref('dl_discounts') }}