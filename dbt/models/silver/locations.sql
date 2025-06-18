{{ config(schema='silver') }} 

WITH source AS (
    SELECT 
        CASE
            WHEN lower(raw_data ->> 'country') = 'colombia' AND lower(raw_data ->> 'city') = 'bogotá' THEN 1 
            WHEN lower(raw_data ->> 'country') = 'colombia' AND lower(raw_data ->> 'city') = 'medellin' THEN 2 
            WHEN lower(raw_data ->> 'country') = 'colombia' AND lower(raw_data ->> 'city') = 'barranquilla' THEN 3 
            WHEN lower(raw_data ->> 'country') = 'colombia' AND lower(raw_data ->> 'city') = 'cali' THEN 4 
            WHEN lower(raw_data ->> 'country') = 'chile' AND lower(raw_data ->> 'city') = 'santiago' THEN 5 
            WHEN lower(raw_data ->> 'country') = 'chile' AND lower(raw_data ->> 'city') = 'concepción' THEN 6 
            WHEN lower(raw_data ->> 'country') = 'chile' AND lower(raw_data ->> 'city') = 'valparaíso' THEN 7 
            WHEN lower(raw_data ->> 'country') = 'peru' AND lower(raw_data ->> 'city') = 'trujillo' THEN 8 
            WHEN lower(raw_data ->> 'country') = 'peru' AND lower(raw_data ->> 'city') = 'lima' THEN 9 
            WHEN lower(raw_data ->> 'country') = 'peru' AND lower(raw_data ->> 'city') = 'arequipa' THEN 10 
            WHEN lower(raw_data ->> 'country') = 'mexico' AND lower(raw_data ->> 'city') = 'guadalajara' THEN 11 
            WHEN lower(raw_data ->> 'country') = 'mexico' AND lower(raw_data ->> 'city') = 'ciudad de méxico' THEN 12
            WHEN lower(raw_data ->> 'country') = 'mexico' AND lower(raw_data ->> 'city') = 'monterrey' THEN 13 
            WHEN lower(raw_data ->> 'country') = 'argentina' AND lower(raw_data ->> 'city') = 'buenos aires' THEN 14 
            WHEN lower(raw_data ->> 'country') = 'argentina' AND lower(raw_data ->> 'city') = 'córdoba' THEN 15
            WHEN lower(raw_data ->> 'country') = 'argentina' AND lower(raw_data ->> 'city') = 'rosario' THEN 16 
            ELSE 999
        END AS id,
        TRIM(lower(raw_data ->> 'country')) AS country,
        CASE
            WHEN lower(raw_data ->> 'city') ilike 'are%' THEN 'arequipa'
            WHEN lower(raw_data ->> 'city') ilike 'bog%' THEN 'bogotá'
            WHEN lower(raw_data ->> 'city') ilike 'cal%' THEN 'cali'
            WHEN lower(raw_data ->> 'city') ilike 'cd%' or lower(raw_data ->> 'city') ilike 'ciudad%' THEN 'ciudad de méxico'
            WHEN lower(raw_data ->> 'city') ilike 'con%' THEN 'concepción'
            WHEN lower(raw_data ->> 'city') ilike 'cor%' THEN 'córdoba'
            WHEN lower(raw_data ->> 'city') ilike 'gua%' THEN 'guadalajara'
            WHEN lower(raw_data ->> 'city') ilike 'me%' THEN 'medellín'
            WHEN lower(raw_data ->> 'city') ilike 'san%' THEN 'santiago de chile'
            WHEN lower(raw_data ->> 'city') ilike 'val%'THEN 'valparaíso'
            else lower(raw_data ->> 'city')
        END AS city
    FROM {{ source('raw', 'raw_customers') }}
    WHERE
        raw_data ->> 'country' IS NOT NULL
        AND raw_data ->> 'city' IS NOT NULL
        AND trim(raw_data ->> 'country') <> ''
        AND trim(raw_data ->> 'city') <> ''
    GROUP BY id, country, city
)

SELECT DISTINCT id, country, city
FROM source
WHERE id < 17
ORDER BY id