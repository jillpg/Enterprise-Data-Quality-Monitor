WITH raw AS (
    SELECT * FROM {{ source('raw_layer', 'raw_products') }}
)

SELECT
    -- IDs
    CAST(product_id AS VARCHAR) AS product_id,
    
    -- Details
    CAST(product_name AS VARCHAR) AS product_name,
    CAST(category AS VARCHAR) AS category,
    
    -- Numerics
    TRY_CAST(price AS FLOAT) AS price,
    TRY_CAST(stock_level AS INTEGER) AS stock_level

FROM raw
