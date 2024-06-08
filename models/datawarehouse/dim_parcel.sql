{{ config(materialized='table') }}

select
    id,
    height,
    height_units,
    length,
    length_units,
    volume,
    volume_units,
    weight,
    weight_units,
    width,
    width_units
from {{ ref('parcel_dimensions') }}