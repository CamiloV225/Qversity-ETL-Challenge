{{ config(schema='gold')}} 
WITH d_brand_by_plan_type AS (
    SELECT
        plan_type,
        device_brand,
        COUNT(*) AS customer_count
    FROM {{ ref('customers') }}
    WHERE device_brand IS NOT NULL AND plan_type IS NOT NULL
    GROUP BY plan_type, device_brand
    ORDER BY plan_type, customer_count DESC

)

SELECT *
FROM d_brand_by_plan_type
