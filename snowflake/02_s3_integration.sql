-- =====================================================================
-- 02_s3_integration.sql  -  run as ACCOUNTADMIN
-- Lets Snowflake READ your S3 bucket through an IAM role (no AWS keys
-- are stored in Snowflake). Replace the two placeholders first:
--   <aws-account-id>    12-digit AWS account number
--   <your-bucket-name>  the S3 bucket from Stage 6
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- IMPORTANT: use CREATE ... IF NOT EXISTS, never CREATE OR REPLACE, once the
-- AWS trust policy is set up - replacing the integration generates a new
-- external id and breaks the trust relationship.
CREATE STORAGE INTEGRATION IF NOT EXISTS EDR_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/edr-snowflake-s3-read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket-name>/dev/');

-- Copy two values from this output into the AWS role's trust policy:
--   STORAGE_AWS_IAM_USER_ARN   and   STORAGE_AWS_EXTERNAL_ID
DESC INTEGRATION EDR_S3_INT;

-- The application role creates the stage that uses this integration
GRANT USAGE ON INTEGRATION EDR_S3_INT TO ROLE EDR_APP_ROLE;
