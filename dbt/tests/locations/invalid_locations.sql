SELECT *
FROM {{ ref('locations') }}
WHERE (lower(country), lower(city)) NOT IN (
    ('colombia', 'bogotá'),
    ('colombia', 'medellín'),
    ('colombia', 'barranquilla'),
    ('colombia', 'cali'),
    ('chile', 'santiago de chile'),
    ('chile', 'concepción'),
    ('chile', 'valparaíso'),
    ('peru', 'trujillo'),
    ('peru', 'lima'),
    ('peru', 'arequipa'),
    ('mexico', 'guadalajara'),
    ('mexico', 'ciudad de méxico'),
    ('mexico', 'monterrey'),
    ('argentina', 'buenos aires'),
    ('argentina', 'córdoba'),
    ('argentina', 'rosario')
)