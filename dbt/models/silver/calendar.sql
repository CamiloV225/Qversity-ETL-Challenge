{{ config(schema='silver') }}  
with payment_date as (
    SELECT
        CASE
            WHEN (last_payment_date) ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(last_payment_date, '/', 3) || '-' ||  
                split_part(last_payment_date, '/', 2) || '-' ||  
                split_part(last_payment_date, '/', 1),           
                'YYYY-MM-DD')
            WHEN (last_payment_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (last_payment_date)::date
            ELSE NULL
        END AS date
    FROM {{ ref('mobile_customers_cleaned') }}
),

registration_date AS (
    SELECT
        CASE
            WHEN (registration_date) ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(registration_date, '/', 3) || '-' ||  -- año
                split_part(registration_date, '/', 2) || '-' ||  -- mes
                split_part(registration_date, '/', 1),           -- dia
                'YYYY-MM-DD')
            WHEN (registration_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (registration_date)::date
            ELSE NULL
        END AS date
    FROM {{ ref('mobile_customers_cleaned') }}
),


payment_history_date as (
  SELECT customer_id::bigint,
        (payment ->> 'date')::date AS date
  FROM {{ ref('mobile_customers_cleaned') }}
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