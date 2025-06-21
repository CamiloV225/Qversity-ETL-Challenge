{{ config(schema='gold')}} 
with revenue_location AS (
    SELECT
    loc.country,
    loc.city,
    ROUND(SUM(c.monthly_bill_usd)::NUMERIC, 2) AS total_revenue
    FROM {{ ref('customers') }} AS c
    JOIN {{ ref('locations') }} AS loc
    ON c.location_id = loc.id
    WHERE c.monthly_bill_usd IS NOT NULL
    GROUP BY loc.country, loc.city
    ORDER BY total_revenue DESC
)

SELECT * FROM revenue_location