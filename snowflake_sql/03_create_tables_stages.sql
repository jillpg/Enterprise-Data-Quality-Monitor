-- 3. Create Objects (Stages, Formats, Tables)

USE ROLE SYSADMIN;
USE DATABASE EDQM_DB;
USE SCHEMA RAW;

-- File Format (CSV)
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

-- External Stage (Points to S3)
-- Requires the integration created in step 02
CREATE OR REPLACE STAGE s3_landing_stage
  STORAGE_INTEGRATION = s3_edqm_integration
  URL = 's3://your-bucket-name/landing/' -- Load Customers
  FILE_FORMAT = csv_format;

COPY INTO raw_customers
FROM @s3_landing_stage/customers/
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE'; -- Resilient loading: Skip bad records, log errors

-- Tables (RAW Layer)
-- Ingestion Strategy: Full VARCHAR typing to decouple Load/Transform failures.
-- Type enforcement and cleansing are delegated to the dbt Transformation layer.

CREATE OR REPLACE TABLE raw_customers (
    customer_id STRING,
    name STRING,
    email STRING,
    signup_date STRING,
    region STRING
);

CREATE OR REPLACE TABLE raw_products (
    product_id STRING,
    category STRING,
    price STRING, -- Will cast to FLOAT in dbt
    stock_level STRING,
    product_name STRING
);

CREATE OR REPLACE TABLE raw_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity STRING,
    total_amount STRING,
    order_date STRING,
    status STRING
);
