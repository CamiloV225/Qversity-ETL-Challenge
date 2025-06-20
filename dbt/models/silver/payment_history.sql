{{ config(schema='silver') }} 

with payment_h as (
    SELECT
        customer_id,
        payment
    FROM {{ ref('mobile_customers_cleaned') }}
    WHERE jsonb_typeof(payment) = 'array'
),

exploded_payments as (
    SELECT 
        customer_id,
        jsonb_array_elements(payment) AS payment_json
    FROM payment_h
),

parsed_payments AS (
    SELECT 
        customer_id,
        (payment_json ->> 'date')::date AS payment_date,
        CASE
          WHEN payment_json ->> 'amount' = 'unknown' THEN NULL
          ELSE (payment_json ->> 'amount')::float
        END AS payment_amount,
        payment_json ->> 'status' AS payment_status
    FROM exploded_payments
)

SELECT *
FROM parsed_payments
ORDER BY customer_id, payment_date