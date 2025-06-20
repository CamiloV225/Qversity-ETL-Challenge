{{ config(schema='bronze')}} 
with source AS (
    SELECT 
        (raw_data ->> 'customer_id') AS customer_id,
        (raw_data ->> 'first_name') AS first_name,
        (raw_data ->> 'last_name') AS last_name,
        (raw_data ->> 'email') AS email,
        (raw_data ->> 'phone_number') AS phone_number,
        (raw_data ->> 'age') AS age,
        (raw_data ->> 'country') AS country,
        (raw_data ->> 'city') AS city,
        (raw_data ->> 'operator') AS operator,
        (raw_data ->> 'plan_type') AS plan_type,
        (raw_data ->> 'monthly_data_gb') AS monthly_data_gb,
        (raw_data ->> 'monthly_bill_usd') AS monthly_bill_usd,
        (raw_data ->> 'registration_date') AS registration_date,
        (raw_data ->> 'status') AS status,
        (raw_data ->> 'device_brand') AS device_brand,
        (raw_data ->> 'device_model') AS device_model,
        jsonb_array_elements_text(raw_data -> 'contracted_services') AS service,
        (raw_data ->> 'last_payment_date') AS last_payment_date,
        (raw_data ->> 'credit_limit') AS credit_limit,
        (raw_data ->> 'data_usage_current_month') AS data_usage_current_month,
        (raw_data ->> 'credit_score') AS credit_score,
        (raw_data ->> 'latitude') AS latitude,
        (raw_data ->> 'longitude') AS longitude,
        jsonb_array_elements(raw_data -> 'payment_history') AS payment,
        (raw_data ->> 'record_uuid') AS record_uuid

    FROM {{ source('raw', 'raw_customers') }}
    WHERE jsonb_typeof(raw_data -> 'contracted_services') = 'array' AND jsonb_typeof(raw_data -> 'payment_history') = 'array'
)

SELECT DISTINCT ON (customer_id) *
FROM source
ORDER BY customer_id
