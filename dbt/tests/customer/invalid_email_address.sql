-- Para testear emails no validos (principalmente los que no posean dominio)
SELECT *
FROM {{ ref('customers') }}
WHERE email IS NOT NULL AND email NOT LIKE '%@%.%'