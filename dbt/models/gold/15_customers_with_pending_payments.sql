{{ config(schema='gold')}} 
WITH pending_payments AS (
    SELECT DISTINCT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.operator,
    c.plan_type,
    p.payment_date,
    p.payment_status,
    p.payment_amount
FROM {{ ref('payment_history') }} AS p
JOIN {{ ref('customers') }} AS c
    ON p.customer_id = c.customer_id
WHERE p.payment_status IN ('pending', 'failed')
)

SELECT * FROM pending_payments

