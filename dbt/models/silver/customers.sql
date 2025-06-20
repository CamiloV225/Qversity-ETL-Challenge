{{ config(schema='silver')}} 
with customer AS (
    SELECT 
        customer_id,

        -- Comencé por estandarizar los nombres y apellidos, ya que presentaban inconsistencias como espacios en blanco, 
        -- caracteres numéricos, y registros incompletos o truncados.
        CASE 
            WHEN lower(first_name) = '' THEN NULL
            WHEN lower(trim(first_name)) ILIKE 'a%' THEN 'ana'
            WHEN lower(trim(first_name)) ILIKE 'car%' THEN 'carlos'
            WHEN lower(trim(first_name)) ILIKE 'ju%' THEN 'juan'
            WHEN lower(trim(first_name)) ILIKE 'lau%' THEN 'laura'
            WHEN lower(trim(first_name)) ILIKE 'lu%' THEN 'luis'
            WHEN lower(trim(first_name)) ILIKE 'mara%' THEN 'mara'
            WHEN lower(trim(first_name)) ILIKE 'mar%' THEN 'maria'
            WHEN lower(trim(first_name)) ILIKE 'mig%' THEN 'miguel'
            WHEN lower(trim(first_name)) ILIKE 'pe%' THEN 'pedro'
            WHEN lower(trim(first_name)) ILIKE 'sof%' THEN 'sofia'
            ELSE regexp_replace(lower(first_name), '[^a-z]', '', 'g')
        END AS first_name,

        CASE
            WHEN lower(last_name) = '' THEN NULL
            WHEN lower(trim(last_name)) ILIKE 'fern%' THEN 'fernández'
            WHEN lower(trim(last_name)) ILIKE 'garc%' THEN 'garcía'
            WHEN lower(trim(last_name)) ILIKE 'gon%' THEN 'gonzález'
            WHEN lower(trim(last_name)) ILIKE 'fern%' THEN 'fernández'
            WHEN lower(trim(last_name)) ILIKE 'l%' THEN 'lópez'
            WHEN lower(trim(last_name)) ILIKE 'mart%' THEN 'martínez'
            WHEN lower(trim(last_name)) ILIKE 'rodr%' THEN 'rodríguez'
            ELSE lower(last_name)
        END AS last_name,

        CASE
            WHEN (email) = '' THEN NULL
            ELSE (email)
        END AS email,

        -- Estandarice los numeros telefonicos debido a que algunos fueron insertados inicialmente con el prefijo del 
        -- pais o presentaban otras inconsistencias.
        CASE
            WHEN phone_number ILIKE '(%' THEN
                regexp_replace(trim(phone_number), '[^0-9]', '', 'g')::BIGINT

            WHEN phone_number ILIKE '+57%' THEN
                regexp_replace(trim(phone_number), '^\+57', '')::BIGINT

            WHEN trim(phone_number) = '' THEN
                NULL

            ELSE regexp_replace(trim(phone_number), '[^0-9]', '', 'g')::BIGINT
        END AS phone_number,
        -- Al mirar mas de cerca los registros de age, encontre que esta posee valores float y valores negativos
        -- Por tal motivo decidi primero formatearlos como numero, luego quitarles los decimales con floor()
        CASE
            WHEN floor((age)::numeric)::int < 0 THEN NULL
            ELSE floor((age)::numeric)::int
        END AS age,
        -- Luego estandarice los nombres de los paises y las ciudades
        -- PD: debido a los cambios realizados decidi prescindir de estas columnas y cree una nueva tabla de forma organizada
        CASE
            WHEN lower(country) ilike 'co%' or lower(country) ilike 'cl%' THEN 'colombia'
            WHEN lower(country) ilike 'p%' THEN 'peru'
            WHEN lower(country) ilike 'ch%' THEN 'chile'
            WHEN lower(country) ilike 'm%' THEN 'mexico'
            WHEN lower(country) ilike 'a%' THEN 'argentina'
            ELSE lower(country)
        END AS country,
        
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
            ELSE lower(city)
        END AS city,

        CASE
            WHEN lower(operator) ilike 'cla%' THEN 'claro'
            WHEN lower(operator) ilike 'mov%' THEN 'movistar'
            WHEN lower(operator) ilike 't%' THEN 'tigo'
            WHEN lower(operator) ilike 'w%' THEN 'wom'
            ELSE lower(operator)
        END AS operator,

        CASE
            WHEN lower(plan_type) ilike 'pre%' THEN 'prepago'
            WHEN lower(plan_type) ilike 'pos%' THEN 'pospago'
            WHEN lower(plan_type) ilike 'c%' THEN 'control'
            ELSE lower(plan_type)
        END AS plan_type,

        (monthly_data_gb)::float AS monthly_data_gb,
        (monthly_bill_usd)::float AS monthly_bill_usd,
        -- En cuanto a los formatos de fecha, los registros poseen principalmente dos variantes: el formato europeo 'DD/MM/YYYY' y 
        -- el formato ISO estándar 'YYYY-MM-DD'. Para asegurar una conversión correcta y uniforme, se detecta el formato mediante  
        -- expresiones regulares, comprobando el tipo (EU o ISO), y en caso de EU se hace un split y se reorganiza en el formato ISO.
        CASE
            WHEN (registration_date) ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(registration_date, '/', 3) || '-' ||  -- año
                split_part(registration_date, '/', 2) || '-' ||  -- mes
                split_part(registration_date, '/', 1),           -- dia
                'YYYY-MM-DD')
            WHEN (registration_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (registration_date)::date
            ELSE NULL
        END AS registration_date,
        CASE
            WHEN lower(status) ilike 'a%' THEN 'active'
            WHEN lower(status) ilike 'ina%' THEN 'inactive'
            WHEN lower(status) ilike 'sus%' THEN 'suspended'
            WHEN lower(status) ilike 'v%' THEN 'valid'
            WHEN lower(status) = '' THEN NULL
            ELSE lower(status)
        END AS status,

        CASE
            WHEN lower(device_brand) ilike 'a%' THEN 'apple'
            WHEN lower(device_brand) ilike 'h%' THEN 'huawei'
            WHEN lower(device_brand) ilike 's%' THEN 'samsung'
            WHEN lower(device_brand) ilike 'x%' THEN 'xiaomi'
            ELSE lower(device_brand)
        END AS device_brand,
        -- Algunos modelos presentaba caracteres inusuales los cuales fueron removidos
        CASE 
            WHEN device_model is NULL 
                or trim(device_model) = '' 
                or trim(lower(device_model)) = '' THEN NULL
                -- de paso, aprendi sobre regexp y su uso.
            ELSE regexp_replace(replace(lower(device_model), '-', ' '), '[^a-z0-9 ]','','g')
        END AS device_model,

        CASE
            WHEN (last_payment_date) ~ '^\d{2}/\d{2}/\d{4}' THEN
            to_date(
                split_part(last_payment_date, '/', 3) || '-' ||  
                split_part(last_payment_date, '/', 2) || '-' ||  
                split_part(last_payment_date, '/', 1),           
                'YYYY-MM-DD')
            WHEN (last_payment_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
                (last_payment_date)::date
            ELSE NULL
        END AS last_payment_date,

        (credit_limit)::float,
        (data_usage_current_month)::float,
        (credit_score)::float,
        ROUND((latitude)::numeric, 6) AS latitude,
        ROUND((longitude)::numeric, 6) AS longitude,
        ingestion_time,
        current_timestamp AS transformation_time,
        CASE
            WHEN record_uuid = 'invalid-uuid-6731' THEN NULL
            ELSE record_uuid
        END AS record_uuid,
        batch_id,
        source_file
    FROM {{ ref('mobile_customers_cleaned') }}
    --(customer_id) IS NOT NULL AND 
    WHERE phone_number ~ '^\d{10}$' AND email ~* '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
),

locations_base AS (
    SELECT
        id AS location_id,
        city
    from {{ ref('locations') }}
),

-- Se asigna el ID de ubicación a cada cliente mediante una relación con la ciudad, 
-- considerando que la ciudad proporciona mayor granularidad que el país.

-- Luego se filtran los registros para conservar únicamente un cliente por email, 
-- ya que existían casos donde un mismo correo electrónico estaba asociado a múltiples customer_id. 

temp_customer AS (
    SELECT DISTINCT ON (c.email) c.customer_id, c.first_name, c.last_name, c.email, c.phone_number, c.age, l.location_id, 
    c.operator, c.monthly_data_gb, c.monthly_bill_usd, c.registration_date, c.status, c.device_brand, 
    c.device_model, c.last_payment_date, c.credit_limit, c.data_usage_current_month, c.credit_score, 
    c.latitude, c.longitude, c.ingestion_time,c.transformation_time, c.record_uuid, c.batch_id, c.source_file    
    from customer c
    left join locations_base l
    on c.city = l.city
    WHERE age BETWEEN 0 AND 100 
    AND c.first_name IS NOT NULL
    AND c.record_uuid IS NOT NULL
    ORDER BY c.email
)

-- Para resolver esta duplicidad cruzada, primero se deduplican los correos y 
-- posteriormente se aplica una segunda deduplicación por customer_id sobre ese subconjunto.

SELECT DISTINCT ON (customer_id) *
FROM temp_customer
ORDER BY customer_id
