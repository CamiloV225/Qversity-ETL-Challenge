{{ config(schema='gold')}} 
WITH combined_services AS (
    SELECT
        customer_id,
        STRING_AGG(DISTINCT service, ' + ' ORDER BY service) AS service_combination
    FROM {{ ref('contracted_services') }}
    WHERE service IS NOT NULL
    GROUP BY customer_id
),

combination_counts AS (
    SELECT
        service_combination,
        COUNT(*) AS customer_count
    FROM combined_services
    GROUP BY service_combination
)

SELECT *
FROM combination_counts
ORDER BY customer_count DESC
