{{ config(materialized='table') }}

select
    addr.id,
    addr.country,
    addr.state_code,
    addr.suburb,
    addr.postcode,
    tz.timezone 
from {{ ref('addresses') }} addr
left join {{ ref('timezones') }} tz
    on addr.country = tz.country
    and addr.state_code = tz.state_code