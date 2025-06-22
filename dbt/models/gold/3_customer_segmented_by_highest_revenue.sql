{{ config(schema='gold')}} 
WITH segmented_customers AS (
    SELECT
        customer_id,
        plan_type,
        monthly_bill_usd,
        CASE
            WHEN credit_score >= 750 THEN 'high'
            WHEN credit_score >= 600 THEN 'medium'
            ELSE 'low'
        END AS credit_segment
    FROM {{ ref('customers') }}
    WHERE monthly_bill_usd IS NOT NULL
)

SELECT
    credit_segment,
    plan_type,
    COUNT(*) AS num_customers,
    ROUND(SUM(monthly_bill_usd)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(monthly_bill_usd)::NUMERIC, 2) AS avg_revenue_per_customer
FROM segmented_customers
GROUP BY credit_segment, plan_type
ORDER BY total_revenue DESC
