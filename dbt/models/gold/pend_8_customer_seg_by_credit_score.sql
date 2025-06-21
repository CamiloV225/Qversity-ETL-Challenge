{{ config(schema='gold')}} 
WITH credit_segmented AS (
    SELECT
        customer_id,
        CASE
            WHEN credit_score >= 550 THEN 'high'
            WHEN credit_score >= 250 THEN 'medium'
            WHEN credit_score >= 1 THEN 'low'
            ELSE 'unknown'
        END AS credit_segment
    FROM {{ ref('customers') }}
    WHERE credit_score IS NOT NULL
)

SELECT
    credit_segment,
    COUNT(*) AS customer_count
FROM credit_segmented
GROUP BY credit_segment
ORDER BY credit_segment
