{{ config(schema='gold')}} 
with ARPU_by_plan_type AS (
    SELECT
        plan_type,
        ROUND(AVG(monthly_bill_usd), 2) AS arpu
    FROM {{ ref('customers') }}
    WHERE monthly_bill_usd IS NOT NULL
    GROUP BY plan_type
    ORDER BY arpu DESC
)

SELECT * FROM ARPU_by_plan_type