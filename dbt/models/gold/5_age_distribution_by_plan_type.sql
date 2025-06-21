{{ config(schema='gold')}} 
with age_distribution_by_plan AS (
    SELECT plan_type,
            CASE
                WHEN age BETWEEN 18 AND 24 THEN '18-24'
                WHEN age BETWEEN 18 AND 24 THEN '25-34'
                WHEN age BETWEEN 18 AND 24 THEN '35-44'
                WHEN age BETWEEN 18 AND 24 THEN '44-54'
                WHEN age BETWEEN 18 AND 24 THEN '55-64'
                ELSE '65+'
            END AS age_group
            FROM {{ ref('customers') }}
            WHERE age IS NOT NULL AND plan_type IS NOT NULL
)

SELECT plan_type, age_group, COUNT(*) AS total FROM age_distribution_by_plan
GROUP BY plan_type, age_group
ORDER BY plan_type, age_group