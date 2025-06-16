{{ config(schema='silver') }} 
with source as (
    select
        (raw_data ->> 'customer_id')::bigint as customer_id,
        jsonb_array_elements(raw_data -> 'payment_history') as payment
    from {{ source('raw', 'raw_customers') }}
    where jsonb_typeof(raw_data -> 'payment_history') = 'array' AND raw_data ->> 'customer_id' IS NOT NULL
)

select
  customer_id,
  (payment ->> 'date')::date as payment_date,
  (payment ->> 'status') as payment_status,
  case
    when (payment ->> 'amount') ~ '^\d+(\.\d+)?$'
      then (payment ->> 'amount')::numeric
    else null
  end as payment_amount
from source