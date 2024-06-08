{{ config(materialized='table') }}

select
    addr.id,
    addr.country,
    addr.state_code,
    addr.suburb,
    addr.postcode,
    tz.timezone 
from {{ ref('addresses') }} addr
-- adding a timezone which can be used later 
-- to convert to local time/date
left join {{ ref('timezones') }} tz
    on addr.country = tz.country
    and addr.state_code = tz.state_code