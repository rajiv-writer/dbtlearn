WITH raw_hosts AS (
    SELECT
        id as host_id,
        name,
        is_superhost,
        created_at,
        updated_at
    FROM
       {{ source('airbnb', 'hosts') }}
)

SELECT
    host_id, 
    name AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM raw_hosts