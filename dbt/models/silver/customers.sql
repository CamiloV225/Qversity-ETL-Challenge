{{ config(schema='silver') }} 
with source AS (
    SELECT
        DISTINCT(raw_data ->> 'customer_id')::BIGINT AS customer_id,
        --CASE 
        --    WHEN lower(raw_data ->> 'first_name') = '' THEN NULL
        --    ELSE lower(raw_data ->> 'first_name')
        --END AS first_name,
        lower(raw_data ->> 'first_name') AS first_name,
        lower(raw_data ->> 'last_name') AS last_name,
        CASE
            WHEN raw_data ->> 'email' = '' THEN NULL
            ELSE raw_data ->> 'email'
        END AS email,
        CASE
            WHEN raw_data ->> 'phone_number' ILIKE '(%' THEN
                regexp_replace(trim(raw_data ->> 'phone_number'), '[^0-9]', '', 'g')::BIGINT

            WHEN raw_data ->> 'phone_number' ILIKE '+57%' THEN
                regexp_replace(trim(raw_data ->> 'phone_number'), '^\+57', '')::BIGINT

            WHEN trim(raw_data ->> 'phone_number') = '' THEN
                NULL

            ELSE regexp_replace(trim(raw_data ->> 'phone_number'), '[^0-9]', '', 'g')::BIGINT
        END AS phone_number,
        -- Al mirar mas de cerca los registros de age, encontre que esta posee valores float y valores negativos
        -- Por tal motivo decidi primero formatearlos como numero, luego quitarles los decimales con floor()
        CASE
            WHEN floor((raw_data ->> 'age')::numeric)::int < 0 THEN NULL
            ELSE floor((raw_data ->> 'age')::numeric)::int
        END AS age,
        -- Luego estandarice los nombres de los paises y las ciudades
        CASE
            WHEN lower(raw_data ->> 'country') ilike 'co%' or lower(raw_data ->> 'country') ilike 'cl%' THEN 'colombia'
            WHEN lower(raw_data ->> 'country') ilike 'p%' THEN 'peru'
            WHEN lower(raw_data ->> 'country') ilike 'ch%' THEN 'chile'
            WHEN lower(raw_data ->> 'country') ilike 'm%' THEN 'mexico'
            WHEN lower(raw_data ->> 'country') ilike 'a%' THEN 'argentina'
            ELSE lower(raw_data ->> 'country')
        END AS country,
        
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
            ELSE lower(raw_data ->> 'city')
        END AS city,

        CASE
            WHEN lower(raw_data ->> 'operator') ilike 'cla%' THEN 'claro'
            WHEN lower(raw_data ->> 'operator') ilike 'mov%' THEN 'movistar'
            WHEN lower(raw_data ->> 'operator') ilike 't%' THEN 'tigo'
            WHEN lower(raw_data ->> 'operator') ilike 'w%' THEN 'wom'
            ELSE lower(raw_data ->> 'operator')
        END AS operator,

        CASE
            WHEN lower(raw_data ->> 'plan_type') ilike 'pre%' THEN 'prepago'
            WHEN lower(raw_data ->> 'plan_type') ilike 'pos%' THEN 'pospago'
            WHEN lower(raw_data ->> 'plan_type') ilike 'c%' THEN 'control'
            ELSE lower(raw_data ->> 'plan_type')
        END AS plan_type,
        (raw_data ->> 'monthly_data_gb')::float AS monthly_data_gb,
        (raw_data ->> 'monthly_bill_usd')::float AS monthly_bill_usd,
        -- En cuanto a los formatos de fecha, los registros poseen principalmente dos variantes: el formato europeo 'DD/MM/YYYY' y 
        -- el formato ISO estándar 'YYYY-MM-DD'. Para asegurar una conversión correcta y uniforme, se detecta el formato mediante  
        -- expresiones regulares, comprobando el tipo (EU o ISO), y en caso de EU se hace un split y se reorganiza en el formato ISO.
        CASE
            WHEN (raw_data ->> 'registration_date') ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(raw_data ->> 'registration_date', '/', 3) || '-' ||  -- año
                split_part(raw_data ->> 'registration_date', '/', 2) || '-' ||  -- mes
                split_part(raw_data ->> 'registration_date', '/', 1),           -- dia
                'YYYY-MM-DD')
            WHEN (raw_data ->> 'registration_date') ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (raw_data ->> 'registration_date')::date
            ELSE NULL
        END AS registration_date,
        CASE
            WHEN lower(raw_data ->> 'status') ilike 'a%' THEN 'active'
            WHEN lower(raw_data ->> 'status') ilike 'ina%' THEN 'inactive'
            WHEN lower(raw_data ->> 'status') ilike 'sus%' THEN 'suspended'
            WHEN lower(raw_data ->> 'status') ilike 'v%' THEN 'valid'
            WHEN lower(raw_data ->> 'status') = '' THEN NULL
            ELSE lower(raw_data ->> 'status')
        END AS status,

        CASE
            WHEN lower(raw_data ->> 'device_brand') ilike 'a%' THEN 'apple'
            WHEN lower(raw_data ->> 'device_brand') ilike 'h%' THEN 'huawei'
            WHEN lower(raw_data ->> 'device_brand') ilike 's%' THEN 'samsung'
            WHEN lower(raw_data ->> 'device_brand') ilike 'x%' THEN 'xiaomi'
            ELSE lower(raw_data ->> 'device_brand')
        END AS device_brand,
        
        CASE 
            WHEN raw_data ->> 'device_model' is NULL 
                or trim(raw_data ->> 'device_model') = '' 
                or trim(lower(raw_data ->> 'status')) = '' THEN NULL
                -- de paso, aprendi sobre regexp y su uso.
            ELSE regexp_replace(replace(lower(raw_data ->> 'device_model'), '-', ' '), '[^a-z0-9 ]','','g')
        END AS device_model,

        CASE
            WHEN (raw_data ->> 'last_payment_date') ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(raw_data ->> 'last_payment_date', '/', 3) || '-' ||  
                split_part(raw_data ->> 'last_payment_date', '/', 2) || '-' ||  
                split_part(raw_data ->> 'last_payment_date', '/', 1),           
                'YYYY-MM-DD')
            WHEN (raw_data ->> 'last_payment_date') ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (raw_data ->> 'last_payment_date')::date
            ELSE NULL
        END AS last_payment_date,

        (raw_data ->> 'credit_limit')::float AS credit_limit,
        (raw_data ->> 'data_usage_current_month')::float AS data_usage_current_month,
        (raw_data ->> 'credit_score')::float AS credit_score,
        (raw_data ->> 'latitude')::float AS latitude,
        (raw_data ->> 'longitude')::float AS longitude,
        ingestion_time,
        current_timestamp AS transformation_time,
        batch_id,
        source_file
    FROM {{ source('raw', 'raw_customers') }}

    WHERE raw_data ->> 'customer_id' IS NOT NULL
    AND TRIM(raw_data ->> 'first_name') <> ''
),

locations_base AS (
    SELECT
        id AS location_id,
        city
    from {{ ref('locations') }}
)

SELECT c.customer_id, c.first_name, c.last_name, c.email, c.phone_number, c.age, l.location_id, 
c.operator, c.monthly_data_gb, c.monthly_bill_usd, c.registration_date, c.status, c.device_brand, 
c.device_model, c.last_payment_date, c.credit_limit, c.data_usage_current_month, c.credit_score, 
c.latitude, c.longitude, c.ingestion_time,c.transformation_time, c.batch_id, c.source_file    
from source c
left join locations_base l
  on c.city = l.city

ORDER BY customer_id