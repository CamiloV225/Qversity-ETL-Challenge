{{ config(schema='gold')}} 
with customers_by_location AS (
    SELECT
    loc.country,
    loc.city,
    COUNT(*)
    FROM {{ ref('customers') }} AS c
    JOIN {{ ref('locations') }} AS loc
    ON c.location_id = loc.id
    GROUP BY loc.country, loc.city
	ORDER BY COUNT(*) DESC
)

SELECT * FROM customers_by_location