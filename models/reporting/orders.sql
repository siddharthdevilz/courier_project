{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns',
        partition_by = {
          "field": "order_placed_at",
          "data_type": "datetime",
          "granularity": "day"
        },
        unique_key = 'order_id',
        incremental_strategy='merge'
    )

}}

with base as
(
    select
        ord.id as order_id,
        ord.created_at as order_placed_at,
        -- Duckdb specific function to convert timezone
        -- coalesce as dummy data can have missing timezones
        coalesce(strftime(timezone(org.timezone, ord.created_at),'%d-%m-%Y'),strftime(created_at,'%d-%m-%Y')) as order_date_local,

        org.country as origin_country,
        org.state_code as origin_state_code,
        org.suburb as origin_suburb,
        org.postcode as origin_postcode,

        dest.country as destination_country,
        dest.state_code as destination_state_code,
        dest.suburb as destination_suburb,
        dest.postcode as destination_postcode,

        cc.courier,
        cc.base_cost_ex_tax as courier_base_cost_ex_tax,
        cc.fuel_surcharge_ex_tax as courier_fuel_surcharge_ex_tax,
        cc.currency as courier_currency,

        ord.sender_id,

        pp.name as order_pricing_plan,

        dp.base_price_ex_tax as customer_base_price_ex_tax,
        dp.fuel_surcharge_ex_tax as customer_fuel_surcharge_ex_tax,

        -- Field not found, added it to discounts table
        discount.discount_ex_tax as marketing_discount_ex_tax,
        discount.name as discount_name,

        dp.currency as customer_currency,
        ord.first_mile_option,
        ord.product_code,
        
        parcel.weight as weight_value,
        parcel.volume as volume_value,
        parcel.weight_units,
        parcel.volume_units,

        ord.state as order_state,
        ord.updated_at
    from {{ ref('fact_order') }} as ord
    left join {{ ref('dim_address') }} as org
        on ord.origin_address_id = org.id
    left join {{ ref('dim_address') }} as dest
        on ord.destination_address_id = dest.id
    left join {{ ref('dim_pricing_plans') }} as pp
        on ord.plan_id = pp.id
    left join {{ ref('dim_discount') }} as discount
        on ord.discount_id = discount.id
    left join {{ ref('stg_delivery_price') }} as dp
        on ord.id = dp.order_id
    left join {{ ref('stg_courier_cost') }} as cc
        on ord.id = cc.order_id
    left join {{ ref('dim_parcel') }} as parcel
        on ord.parcel_dimension_id = parcel.id

    -- Take only newly updated records on incremental runs
    {% if is_incremental() %}
    where
        ord.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from base