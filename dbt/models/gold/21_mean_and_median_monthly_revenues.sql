{{ config(schema='gold')}} 
WITH monthly_revenue AS (
    SELECT plan_type, operator,
    ROUND(AVG(monthly_bill_usd)::NUMERIC, 2) AS mean_revenue,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_bill_usd) AS median_revenue,
    COUNT(*) AS user_count
    FROM {{ ref('customers') }}
    WHERE monthly_bill_usd IS NOT NULL
    AND plan_type IS NOT NULL
    AND operator IS NOT NULL
    GROUP BY plan_type, operator
    ORDER BY plan_type, operator
)

SELECT *
FROM monthly_revenue
