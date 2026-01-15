-- 4. Load Data (Test)

USE DATABASE EDQM_DB;
USE SCHEMA RAW;
USE WAREHOUSE EDQM_WH;

-- Load Customers
COPY INTO raw_customers
FROM @s3_landing_stage/customers/
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE'; -- Resilient loading: Skip bad records, log errors

-- Load Products
COPY INTO raw_products
FROM @s3_landing_stage/products/
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE';

-- Load Orders
-- Loads all files matching pattern (daily partitions)
COPY INTO raw_orders
FROM @s3_landing_stage/orders/
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE';

-- Verify
SELECT 'Customers' as table_name, count(*) as cnt FROM raw_customers
UNION ALL
SELECT 'Products', count(*) FROM raw_products
UNION ALL
SELECT 'Orders', count(*) FROM raw_orders;
