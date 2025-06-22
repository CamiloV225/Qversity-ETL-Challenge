{{ config(schema='gold')}} 
WITH device_brand AS (
    SELECT
        device_brand,
        COUNT(*) AS total
        FROM {{ ref('customers') }}
        GROUP BY device_brand
        ORDER BY device_brand
)

SELECT *
FROM device_brand
