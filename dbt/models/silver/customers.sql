with source as (
    select
        raw_data ->> 'customer_id' as customer_id,
        raw_data ->> 'first_name' as first_name,
        raw_data ->> 'last_name' as last_name,
        raw_data ->> 'email' as email,
        raw_data ->> 'phone_number' as phone_number,
        -- Al mirar mas de cerca los registros de age, encontre que esta posee valores float
        -- Por tal motivo decidi primero formatearlos como numero, luego quitarles los decimales con floor()
        floor((raw_data ->> 'age')::numeric)::int as age,
        lower(raw_data ->> 'country') as country,
        lower(raw_data ->> 'city') as city,
        lower(raw_data ->> 'operator') as operator,
        lower(raw_data ->> 'plan_type') as plan_type,
        (raw_data ->> 'monthly_data_gb')::float as monthly_data_gb,
        (raw_data ->> 'monthly_bill_usd')::float as monthly_bill_usd,
        case
            when (raw_data ->> 'registration_date') ~ '^\d{2}/\d{2}/\d{4}' then
            to_date(
                split_part(raw_data ->> 'registration_date', '/', 3) || '-' ||  -- año
                split_part(raw_data ->> 'registration_date', '/', 2) || '-' ||  -- mes
                split_part(raw_data ->> 'registration_date', '/', 1),           -- día
                'YYYY-MM-DD')
            when (raw_data ->> 'registration_date') ~ '^\d{4}-\d{2}-\d{2}$' then
                (raw_data ->> 'registration_date')::date
            else null
        end as registration_date,
        lower(raw_data ->> 'status') as status,
        raw_data ->> 'device_brand' as device_brand,
        raw_data ->> 'device_model' as device_model, 
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
        ingestion_time,
        batch_id,
        source_file
    from {{ source('raw', 'raw_customers') }}
)

select * from source