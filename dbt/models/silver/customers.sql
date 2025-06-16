{{ config(schema='silver') }} 
with source as (
    select
        (raw_data ->> 'customer_id')::bigint as customer_id,
        lower(raw_data ->> 'first_name') as first_name,
        lower(raw_data ->> 'last_name') as last_name,
        case
            when raw_data ->> 'email' = '' then null
            else raw_data ->> 'email'
        end as email,
        case
            when raw_data ->> 'phone_number' ilike '(%' then
                regexp_replace(trim(raw_data ->> 'phone_number'), '[^0-9]', '', 'g')::bigint

            when raw_data ->> 'phone_number' ilike '+57%' then 
                regexp_replace(trim(raw_data ->> 'phone_number'), '[^0-9]', '', 'g')::bigint

            when trim(raw_data ->> 'phone_number') = '' then null 

            else
                regexp_replace(trim(raw_data ->> 'phone_number'), '[^0-9]', '', 'g')::bigint
        end as phone_number,
        -- Al mirar mas de cerca los registros de age, encontre que esta posee valores float y valores negativos
        -- Por tal motivo decidi primero formatearlos como numero, luego quitarles los decimales con floor()
        case
            when floor((raw_data ->> 'age')::numeric)::int < 0 then null
            else floor((raw_data ->> 'age')::numeric)::int
        end as age,
        -- Luego estandarice los nombres de los paises y las ciudades
        case
            when lower(raw_data ->> 'country') ilike 'co%' or lower(raw_data ->> 'country') ilike 'cl%' then 'colombia'
            when lower(raw_data ->> 'country') ilike 'p%' then 'peru'
            when lower(raw_data ->> 'country') ilike 'ch%' then 'chile'
            when lower(raw_data ->> 'country') ilike 'm%' then 'mexico'
            when lower(raw_data ->> 'country') ilike 'a%' then 'argentina'
            else lower(raw_data ->> 'country')
        end as country,
        
        case
            when lower(raw_data ->> 'city') ilike 'are%' then 'arequipa'
            when lower(raw_data ->> 'city') ilike 'bog%' then 'bogotá'
            when lower(raw_data ->> 'city') ilike 'cal%' then 'cali'
            when lower(raw_data ->> 'city') ilike 'cd%' or lower(raw_data ->> 'city') ilike 'ciudad%' then 'ciudad de méxico'
            when lower(raw_data ->> 'city') ilike 'con%' then 'concepción'
            when lower(raw_data ->> 'city') ilike 'cor%' then 'córdoba'
            when lower(raw_data ->> 'city') ilike 'gua%' then 'guadalajara'
            when lower(raw_data ->> 'city') ilike 'me%' then 'medellin'
            when lower(raw_data ->> 'city') ilike 'san%' then 'santiago de chile'
            when lower(raw_data ->> 'city') ilike 'val%'then 'valparaíso'
            else lower(raw_data ->> 'city')
        end as city,

        --lower(raw_data ->> 'country') as country,
        --lower(raw_data ->> 'city') as city,

        case
            when lower(raw_data ->> 'operator') ilike 'cla%' then 'claro'
            when lower(raw_data ->> 'operator') ilike 'mov%' then 'movistar'
            when lower(raw_data ->> 'operator') ilike 't%' then 'tigo'
            when lower(raw_data ->> 'operator') ilike 'w%' then 'wom'
            else lower(raw_data ->> 'operator')
        end as operator,
        --lower(raw_data ->> 'operator') as operator,
        case
            when lower(raw_data ->> 'plan_type') ilike 'pre%' then 'prepago'
            when lower(raw_data ->> 'plan_type') ilike 'pos%' then 'pospago'
            when lower(raw_data ->> 'plan_type') ilike 'c%' then 'control'
            else lower(raw_data ->> 'plan_type')
        end as plan_type,
        --lower(raw_data ->> 'plan_type') as plan_type,
        (raw_data ->> 'monthly_data_gb')::float as monthly_data_gb,
        (raw_data ->> 'monthly_bill_usd')::float as monthly_bill_usd,
        -- En cuanto a los formatos de fecha, los registros poseen principalmente dos variantes: el formato europeo 'DD/MM/YYYY' y 
        -- el formato ISO estándar 'YYYY-MM-DD'. Para asegurar una conversión correcta y uniforme, se detecta el formato mediante  
        -- expresiones regulares, comprobando el tipo (EU o ISO), y en caso de EU se hace un split y se reorganiza en el formato ISO.
        case
            when (raw_data ->> 'registration_date') ~ '^\d{2}/\d{2}/\d{4}' then
            to_date(
                split_part(raw_data ->> 'registration_date', '/', 3) || '-' ||  -- año
                split_part(raw_data ->> 'registration_date', '/', 2) || '-' ||  -- mes
                split_part(raw_data ->> 'registration_date', '/', 1),           -- dia
                'YYYY-MM-DD')
            when (raw_data ->> 'registration_date') ~ '^\d{4}-\d{2}-\d{2}$' then
                (raw_data ->> 'registration_date')::date
            else null
        end as registration_date,
        case
            when lower(raw_data ->> 'status') ilike 'a%' then 'active'
            when lower(raw_data ->> 'status') ilike 'ina%' then 'inactive'
            when lower(raw_data ->> 'status') ilike 'sus%' then 'suspended'
            when lower(raw_data ->> 'status') ilike 'v%' then 'valid'
            when lower(raw_data ->> 'status') = '' then null
            else lower(raw_data ->> 'status')
        end as status,
        -- lower(raw_data ->> 'status') as status,

        case
            when lower(raw_data ->> 'device_brand') ilike 'a%' then 'apple'
            when lower(raw_data ->> 'device_brand') ilike 'h%' then 'huawei'
            when lower(raw_data ->> 'device_brand') ilike 's%' then 'samsung'
            when lower(raw_data ->> 'device_brand') ilike 'x%' then 'xiaomi'
            else lower(raw_data ->> 'device_brand')
        end as device_brand,
        
        --raw_data ->> 'device_brand' as device_brand,
        case 
            when raw_data ->> 'device_model' is null 
                or trim(raw_data ->> 'device_model') = '' 
                or trim(lower(raw_data ->> 'status')) = '' then null
                -- de paso, aprendi sobre regexp y su uso.
            else regexp_replace(replace(lower(raw_data ->> 'device_model'), '-', ' '), '[^a-z0-9 ]','','g')
        end as device_model,
        --raw_data ->> 'device_model' as device_model, 
        case
            when (raw_data ->> 'last_payment_date') ~ '^\d{2}/\d{2}/\d{4}' then
            to_date(
                split_part(raw_data ->> 'last_payment_date', '/', 3) || '-' ||  
                split_part(raw_data ->> 'last_payment_date', '/', 2) || '-' ||  
                split_part(raw_data ->> 'last_payment_date', '/', 1),           
                'YYYY-MM-DD')
            when (raw_data ->> 'last_payment_date') ~ '^\d{4}-\d{2}-\d{2}$' then
                (raw_data ->> 'last_payment_date')::date
            else null
        end as last_payment_date,
        (raw_data ->> 'credit_limit')::float as credit_limit,
        (raw_data ->> 'data_usage_current_month')::float as data_usage_current_month,
        (raw_data ->> 'credit_score')::float as credit_score,
        (raw_data ->> 'latitude')::float as latitude,
        (raw_data ->> 'longitude')::float as longitude,
        ingestion_time,
        batch_id,
        source_file
    from {{ source('raw', 'raw_customers') }}

    where raw_data ->> 'customer_id' IS NOT NULL
)

select * from source