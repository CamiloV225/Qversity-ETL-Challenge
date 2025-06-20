{{ config(schema='silver') }}  
with services as (
    SELECT
        (customer_id)::bigint,
        (contracted_service) AS service
    FROM {{ ref('mobile_customers_cleaned') }}
    WHERE jsonb_typeof(contracted_service) = 'array'
),

exploded_services AS (
    SELECT 
        customer_id,
        lower(jsonb_array_elements_text(service)) AS service
    FROM services
)

SELECT * FROM exploded_services