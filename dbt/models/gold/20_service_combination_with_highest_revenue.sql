{{ config(schema='gold')}} 
WITH services_per_customer AS (
    SELECT
        customer_id,
        STRING_AGG(DISTINCT service, ' + ' ORDER BY service) AS service_combination
    FROM {{ ref('contracted_services') }}
    WHERE service IS NOT NULL
    GROUP BY customer_id
),

customer_revenue AS (
    SELECT
        customer_id,
        monthly_bill_usd
    FROM {{ ref('customers') }}
    WHERE monthly_bill_usd IS NOT NULL
),

combined AS (
    SELECT
        s.service_combination,
        c.monthly_bill_usd
    FROM services_per_customer s
    JOIN customer_revenue c ON s.customer_id = c.customer_id
)

SELECT
    service_combination,
    COUNT(*) AS num_customers,
    ROUND(SUM(monthly_bill_usd)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(monthly_bill_usd)::NUMERIC, 2) AS avg_revenue_per_customer
FROM combined
GROUP BY service_combination
ORDER BY total_revenue DESC
