{{ config(schema='silver') }} 

WITH source AS (
    SELECT 
        CASE
            WHEN LOWER(raw_data ->> 'country') = 'colombia' AND LOWER(raw_data ->> 'city') = 'bogotá' THEN 1 
            WHEN LOWER(raw_data ->> 'country') = 'colombia' AND LOWER(raw_data ->> 'city') = 'medellín' THEN 2 
            WHEN LOWER(raw_data ->> 'country') = 'colombia' AND LOWER(raw_data ->> 'city') = 'barranquilla' THEN 3 
            WHEN LOWER(raw_data ->> 'country') = 'colombia' AND LOWER(raw_data ->> 'city') = 'cali' THEN 4 
            WHEN LOWER(raw_data ->> 'country') = 'chile'    AND LOWER(raw_data ->> 'city') = 'santiago de chile' THEN 5 
            WHEN LOWER(raw_data ->> 'country') = 'chile'    AND LOWER(raw_data ->> 'city') = 'concepción' THEN 6 
            WHEN LOWER(raw_data ->> 'country') = 'chile'    AND LOWER(raw_data ->> 'city') = 'valparaíso' THEN 7 
            WHEN LOWER(raw_data ->> 'country') = 'peru'     AND LOWER(raw_data ->> 'city') = 'trujillo' THEN 8 
            WHEN LOWER(raw_data ->> 'country') = 'peru'     AND LOWER(raw_data ->> 'city') = 'lima' THEN 9 
            WHEN LOWER(raw_data ->> 'country') = 'peru'     AND LOWER(raw_data ->> 'city') = 'arequipa' THEN 10 
            WHEN LOWER(raw_data ->> 'country') = 'mexico'   AND LOWER(raw_data ->> 'city') = 'guadalajara' THEN 11 
            WHEN LOWER(raw_data ->> 'country') = 'mexico'   AND LOWER(raw_data ->> 'city') = 'ciudad de méxico' THEN 12
            WHEN LOWER(raw_data ->> 'country') = 'mexico'   AND LOWER(raw_data ->> 'city') = 'monterrey' THEN 13 
            WHEN LOWER(raw_data ->> 'country') = 'argentina' AND LOWER(raw_data ->> 'city') = 'buenos aires' THEN 14 
            WHEN LOWER(raw_data ->> 'country') = 'argentina' AND LOWER(raw_data ->> 'city') = 'córdoba' THEN 15
            WHEN LOWER(raw_data ->> 'country') = 'argentina' AND LOWER(raw_data ->> 'city') = 'rosario' THEN 16 
            ELSE 999
        END AS id,
        TRIM(LOWER(raw_data ->> 'country')) AS country,
        TRIM(LOWER(raw_data ->> 'city')) AS city
    FROM {{ source('raw', 'raw_customers') }}
    WHERE
        raw_data ->> 'country' IS NOT NULL
        AND raw_data ->> 'city' IS NOT NULL
        AND TRIM(raw_data ->> 'country') <> ''
        AND TRIM(raw_data ->> 'city') <> ''
    GROUP BY id, country, city
)

SELECT *
FROM source
WHERE id < 17
GROUP BY id, country, city
ORDER BY id