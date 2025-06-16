{{ config(schema='silver') }}  
with source as (
    select
        (raw_data ->> 'customer_id')::bigint as customer_id,
        jsonb_array_elements_text(raw_data -> 'contracted_services') as service
    from {{ source('raw', 'raw_customers') }}
    where jsonb_typeof(raw_data -> 'contracted_services') = 'array' and raw_data ->> 'customer_id' IS NOT NULL
)

select
  customer_id,
  service as contracted_service
from source