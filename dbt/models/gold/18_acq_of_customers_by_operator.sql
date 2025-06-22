{{ config(schema='gold')}} 
WITH acq_by_operator AS (
    SELECT
    cal.year, cal.month, cal.month_name, c.operator,
    COUNT(c.customer_id) AS new_customers
    FROM {{ ref('customers') }} AS c
    JOIN {{ ref('calendar') }} AS cal
    ON c.registration_date = cal.date
    WHERE c.registration_date IS NOT NULL
    AND c.operator IS NOT NULL
    GROUP BY cal.year, cal.month, cal.month_name, c.operator
    ORDER BY cal.year, cal.month, c.operator
)

SELECT * FROM acq_by_operator

