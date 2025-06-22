{{ config(schema='gold')}} 
WITH credit_payment_bh AS (
    SELECT
        p.payment_status AS payment_status,
        ROUND(AVG(c.credit_score)::NUMERIC, 2) AS avg_credit_score,
        COUNT(DISTINCT c.customer_id) AS num_customers
    FROM {{ ref('payment_history') }} p
    JOIN {{ ref('customers') }} c
        ON p.customer_id = c.customer_id
    WHERE c.credit_score IS NOT NULL
    GROUP BY payment_status
    ORDER BY avg_credit_score
)

SELECT * FROM credit_payment_bh

