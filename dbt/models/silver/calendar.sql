{{ config(schema='silver') }}  
with payment_date as (
    SELECT
        CASE
            WHEN (raw_data ->> 'last_payment_date') ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(raw_data ->> 'last_payment_date', '/', 3) || '-' ||  
                split_part(raw_data ->> 'last_payment_date', '/', 2) || '-' ||  
                split_part(raw_data ->> 'last_payment_date', '/', 1),           
                'YYYY-MM-DD')
            WHEN (raw_data ->> 'last_payment_date') ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (raw_data ->> 'last_payment_date')::date
            ELSE NULL
        END AS date
    FROM {{ source('raw', 'raw_customers') }}
),

registration_date AS (
    SELECT
        CASE
            WHEN (raw_data ->> 'registration_date') ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(raw_data ->> 'registration_date', '/', 3) || '-' ||  -- año
                split_part(raw_data ->> 'registration_date', '/', 2) || '-' ||  -- mes
                split_part(raw_data ->> 'registration_date', '/', 1),           -- dia
                'YYYY-MM-DD')
            WHEN (raw_data ->> 'registration_date') ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (raw_data ->> 'registration_date')::date
            ELSE NULL
        END AS date
    from {{ source('raw', 'raw_customers') }}
),

payment_history as (
    SELECT
        (raw_data ->> 'customer_id')::bigint AS customer_id,
        jsonb_array_elements(raw_data -> 'payment_history') AS payment
    FROM {{ source('raw', 'raw_customers') }}
    WHERE jsonb_typeof(raw_data -> 'payment_history') = 'array' AND raw_data ->> 'customer_id' IS NOT NULL
),

payment_history_date as (
  SELECT (payment ->> 'date')::date AS date
  FROM payment_history
),

all_dates as (
SELECT DISTINCT date
FROM payment_date
UNION
SELECT DISTINCT date
FROM registration_date 
UNION
SELECT DISTINCT date
FROM payment_history_date
)

SELECT 
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    TO_CHAR(date, 'Day') AS weekday,
    TO_CHAR(date, 'Month') AS month_name,
    date_trunc('week', date)::date AS week_start,
    date_trunc('month', date)::date AS month_start
FROM all_dates
ORDER BY date