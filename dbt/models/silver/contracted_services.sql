{{ config(schema='silver') }}  
with source as (
    SELECT
        (raw_data ->> 'customer_id')::bigint AS customer_id,
        jsonb_array_elements_text(raw_data -> 'contracted_services') AS service
    FROM {{ source('raw', 'raw_customers') }}
    WHERE jsonb_typeof(raw_data -> 'contracted_services') = 'array' and raw_data ->> 'customer_id' IS NOT NULL
)

SELECT
  customer_id,
  service AS contracted_service
FROM source