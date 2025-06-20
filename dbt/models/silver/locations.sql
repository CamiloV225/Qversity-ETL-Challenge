{{ config(schema='silver') }} 

WITH location AS (
    SELECT 
        CASE
            WHEN lower(country) = 'colombia' AND lower(city) = 'bogotá' THEN 1 
            WHEN lower(country) = 'colombia' AND lower(city) = 'medellin' THEN 2 
            WHEN lower(country) = 'colombia' AND lower(city) = 'barranquilla' THEN 3 
            WHEN lower(country) = 'colombia' AND lower(city) = 'cali' THEN 4 
            WHEN lower(country) = 'chile' AND lower(city) = 'santiago' THEN 5 
            WHEN lower(country) = 'chile' AND lower(city) = 'concepción' THEN 6 
            WHEN lower(country) = 'chile' AND lower(city) = 'valparaíso' THEN 7 
            WHEN lower(country) = 'peru' AND lower(city) = 'trujillo' THEN 8 
            WHEN lower(country) = 'peru' AND lower(city) = 'lima' THEN 9 
            WHEN lower(country) = 'peru' AND lower(city) = 'arequipa' THEN 10 
            WHEN lower(country) = 'mexico' AND lower(city) = 'guadalajara' THEN 11 
            WHEN lower(country) = 'mexico' AND lower(city) = 'ciudad de méxico' THEN 12
            WHEN lower(country) = 'mexico' AND lower(city) = 'monterrey' THEN 13 
            WHEN lower(country) = 'argentina' AND lower(city) = 'buenos aires' THEN 14 
            WHEN lower(country) = 'argentina' AND lower(city) = 'córdoba' THEN 15
            WHEN lower(country) = 'argentina' AND lower(city) = 'rosario' THEN 16 
            ELSE 999
        END AS id,
        TRIM(lower(country)) AS country,
        CASE
            WHEN lower(city) ilike 'are%' THEN 'arequipa'
            WHEN lower(city) ilike 'bog%' THEN 'bogotá'
            WHEN lower(city) ilike 'cal%' THEN 'cali'
            WHEN lower(city) ilike 'cd%' or lower(city) ilike 'ciudad%' THEN 'ciudad de méxico'
            WHEN lower(city) ilike 'con%' THEN 'concepción'
            WHEN lower(city) ilike 'cor%' THEN 'córdoba'
            WHEN lower(city) ilike 'gua%' THEN 'guadalajara'
            WHEN lower(city) ilike 'me%' THEN 'medellín'
            WHEN lower(city) ilike 'san%' THEN 'santiago de chile'
            WHEN lower(city) ilike 'val%'THEN 'valparaíso'
            else lower(city)
        END AS city
    FROM {{ ref('mobile_customers_cleaned') }}
    WHERE
        country IS NOT NULL
        AND city IS NOT NULL
        AND trim(country) <> ''
        AND trim(city) <> ''
    GROUP BY id, country, city
)

SELECT DISTINCT id, country, city
FROM location
WHERE id < 17
ORDER BY id