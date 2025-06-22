{{ config(schema='gold')}} 
WITH most_common_service AS (
    SELECT
        service,
        COUNT(*) AS times_contracted
    FROM {{ ref('contracted_services') }}
    WHERE service IS NOT NULL
    GROUP BY service
    ORDER BY times_contracted DESC

)

SELECT *
FROM most_common_service
