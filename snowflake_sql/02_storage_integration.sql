-- 2. Storage Integration
-- MUST be run as ACCOUNTADMIN
-- REPLACE 'arn:aws:iam::123456789012:role/your_snowflake_role' with your actual AWS Role ARN

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_edqm_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/your_snowflake_role' -- Replace with your IAM Role ARN
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/'); -- Replace with your S3 Bucket URL

-- Retrieve IAM User and External ID for AWS Trust Policy configuration
DESC STORAGE INTEGRATION s3_edqm_integration;

-- Actions required in AWS Console after this step:
-- 1. Copy 'STORAGE_AWS_IAM_USER_ARN' from the result.
-- 2. Copy 'STORAGE_AWS_EXTERNAL_ID' from the result.
-- 3. Go to your AWS IAM Role -> Trust Relationships -> Edit Trust Policy.
-- 4. Paste the values there.
