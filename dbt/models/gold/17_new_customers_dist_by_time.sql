{{ config(schema='gold')}} 
WITH new_customers_distribution_per_time AS (
    SELECT
        DATE_TRUNC('month', registration_date) AS month,
        COUNT(*) AS new_customers
    FROM {{ ref('customers') }}
    WHERE registration_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', registration_date)
    ORDER BY month
)

SELECT * FROM new_customers_distribution_per_time

