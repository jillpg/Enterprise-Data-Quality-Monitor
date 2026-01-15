WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.status,
    o.quantity,
    o.total_amount,
    
    -- Denormalized Dimension Attributes (for easier analytics)
    c.name AS customer_name,
    c.region AS customer_region,
    p.product_name,
    p.category,
    p.price AS unit_price,
    
    -- Data Quality Flags (Business Logic)
    CASE 
        WHEN c.customer_id IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_orphan_order,
    
    CASE
        WHEN o.total_amount < 0 THEN TRUE
        ELSE FALSE
    END AS has_negative_amount,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.order_date) > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate,
    
    -- Chaos 2.0 Detectors
    CASE
        WHEN o.order_date > CURRENT_DATE() THEN TRUE
        ELSE FALSE
    END AS is_future_order,
    
    CASE
        WHEN ABS(o.total_amount - (o.quantity * p.price)) > 0.05 THEN TRUE
        ELSE FALSE
    END AS has_math_error,
    
    CASE
        WHEN o.status NOT IN ('COMPLETED', 'PENDING', 'SHIPPED', 'CANCELLED') THEN TRUE
        WHEN o.status IS NULL THEN TRUE
        ELSE FALSE
    END AS has_bad_status

FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
