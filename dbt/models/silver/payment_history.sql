{{ config(schema='silver') }} 
with source as (
    SELECT
        (raw_data ->> 'customer_id')::bigint AS customer_id,
        jsonb_array_elements(raw_data -> 'payment_history') AS payment
    FROM {{ source('raw', 'raw_customers') }}
    WHERE jsonb_typeof(raw_data -> 'payment_history') = 'array' AND raw_data ->> 'customer_id' IS NOT NULL
)

SELECT
  customer_id,
  (payment ->> 'date')::date AS payment_date,
  (payment ->> 'status') AS payment_status,
  CASE
    WHEN (payment ->> 'amount') ~ '^\d+(\.\d+)?$'
      THEN (payment ->> 'amount')::numeric
    ELSE NULL
  END AS payment_amount
FROM source