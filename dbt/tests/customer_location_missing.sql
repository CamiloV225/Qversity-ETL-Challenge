-- Para testar cuando hay ubicaciones invalidas
SELECT *
FROM {{ ref('customers') }} c
LEFT JOIN {{ ref('locations') }} l
  ON c.location_id = l.id
WHERE l.id IS NULL