{{ config(schema='gold')}} 
WITH d_brand_by_country AS (
    SELECT
    loc.country,
    c.operator,
    c.device_brand,
    COUNT(*) AS customer_count
    FROM {{ ref('customers') }} AS c
    JOIN {{ ref('locations') }} AS loc
        ON c.location_id = loc.id
    WHERE c.device_brand IS NOT NULL
    GROUP BY loc.country, c.operator, c.device_brand
    ORDER BY loc.country, c.operator, customer_count DESC


)

SELECT *
FROM d_brand_by_country
