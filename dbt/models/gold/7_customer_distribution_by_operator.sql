{{ config(schema='gold')}} 
with customers_dist_by_operator AS (
    SELECT operator, COUNT(*) AS customer_count
    FROM {{ ref('customers') }}
    WHERE operator IS NOT NULL
    GROUP BY operator
    ORDER BY customer_count DESC
)

SELECT * FROM customers_dist_by_operator
