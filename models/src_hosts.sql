{{ 
  config(
    materialized='incremental',
    unique_key='host_id',  
    incremental_strategy='merge' )
}}

WITH raw_hosts AS (
    SELECT
        id,
        name,
        is_superhost,
        created_at,
        updated_at
    FROM
       {{ source('airbnb', 'hosts') }}
    
    {% if is_incremental() %}
    -- Filter for new or updated rows based on the created_at column
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    id AS host_id,
    name AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM raw_hosts