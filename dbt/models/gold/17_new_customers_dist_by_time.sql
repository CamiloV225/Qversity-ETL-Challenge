{{ config(schema='gold')}} 
WITH new_customers_distribution_per_time AS (
    SELECT
        c.year,
        c.month,
        COUNT(*) AS new_customers
    FROM {{ ref('customers') }} cu
    JOIN {{ ref('calendar') }} c
        ON cu.registration_date = c.week_start  -- or c.date if more granular
    WHERE cu.registration_date IS NOT NULL
    GROUP BY c.year, c.month
    ORDER BY c.year, c.month
)

SELECT * FROM new_customers_distribution_per_time

