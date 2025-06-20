-- Test para mirar edades invalidas como edades negativas, o edades fuera el promedio como mas de 100 años
SELECT *
FROM {{ ref('customers') }}
WHERE age IS NOT NULL AND (age < 0 OR age > 100)