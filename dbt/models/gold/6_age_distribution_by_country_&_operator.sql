{{ config(schema='gold')}} 
with age_distribution_by_country_and_plan AS (
    SELECT operator, location_id,
            CASE
                WHEN age BETWEEN 18 AND 24 THEN '18-24'
                WHEN age BETWEEN 25 AND 34 THEN '25-34'
                WHEN age BETWEEN 35 AND 44 THEN '35-44'
                WHEN age BETWEEN 45 AND 54 THEN '45-54'
                WHEN age BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
            END AS age_group
            FROM {{ ref('customers') }}
            WHERE age IS NOT NULL AND operator IS NOT NULL
            AND location_id IS NOT NULL
),

country as (
    SELECT id, country
    FROM {{ ref('locations') }}
)

SELECT a.operator, a.age_group, c.country, COUNT(*) AS total FROM age_distribution_by_country_and_plan a
LEFT JOIN country c 
ON a.location_id = c.id
GROUP BY a.operator, a.age_group, c.country
ORDER BY a.operator, a.age_group, c.country