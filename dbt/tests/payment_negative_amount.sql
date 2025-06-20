-- Testear por valores negativos en el monto del historial de pagos

SELECT *
FROM {{ ref('payment_history') }}
WHERE payment_amount < 0