{{ config(schema='gold')}} 
WITH all_customers AS (
    SELECT customer_id
    FROM {{ ref('customers') }}
),

customers_with_issues AS (
    SELECT DISTINCT customer_id
    FROM {{ ref('payment_history') }}
    WHERE LOWER(payment_status) IN ('failed', 'overdue', 'rejected') -- ajusta según tus valores reales
)

SELECT
    COUNT(cwi.customer_id)::FLOAT / COUNT(ac.customer_id)::FLOAT * 100 AS percentage_with_payment_issues
FROM all_customers ac
LEFT JOIN customers_with_issues cwi
  ON ac.customer_id = cwi.customer_id
