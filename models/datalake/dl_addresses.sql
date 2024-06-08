{{ config(materialized='table') }}

select
    id,
    country,
    state_code,
    suburb,
    postcode
from {{ ref('addresses') }}