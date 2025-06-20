SELECT *
FROM {{ ref('payment_history') }}
WHERE payment_status NOT IN ('pending', 'late', 'failed', 'paid')