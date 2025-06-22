{{ config(schema='gold')}} 
WITH total AS (
    SELECT COUNT(*) AS total_customers
    FROM {{ ref('customers') }}
    WHERE status IS NOT NULL
),

status_counts AS (
    SELECT
        status,
        COUNT(*) AS customer_count
    FROM {{ ref('customers') }}
    WHERE status IS NOT NULL
    GROUP BY status
)

SELECT
    sc.status,
    sc.customer_count,
    ROUND((sc.customer_count::NUMERIC / t.total_customers) * 100, 2) AS percentage
FROM status_counts sc
CROSS JOIN total t
ORDER BY percentage DESC
