SELECT *
FROM {{ ref('contracted_services') }}  -- o el nombre final del modelo
WHERE service NOT IN ('international', 'voice', 'data', 'sms', 'roaming')