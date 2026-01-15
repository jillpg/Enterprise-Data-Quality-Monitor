WITH raw AS (
    SELECT * FROM {{ source('raw_layer', 'raw_orders') }}
)

SELECT
    -- IDs
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(product_id AS VARCHAR) AS product_id,
    
    -- Numerics
    TRY_CAST(quantity AS INTEGER) AS quantity,
    TRY_CAST(total_amount AS FLOAT) AS total_amount,
    
    -- Dates
    TRY_TO_DATE(order_date) AS order_date,
    
    -- Status (Standardization)
    UPPER(CAST(status AS VARCHAR)) AS status

FROM raw
