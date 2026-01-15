WITH raw AS (
    SELECT * FROM {{ source('raw_layer', 'raw_customers') }}
)

SELECT
    -- IDs
    CAST(customer_id AS VARCHAR) AS customer_id,
    
    -- Text
    CAST(name AS VARCHAR) AS name,
    CAST(email AS VARCHAR) AS email,
    CAST(region AS VARCHAR) AS region,
    
    -- Dates (Safe Cast)
    TRY_TO_DATE(signup_date) AS signup_date

FROM raw
