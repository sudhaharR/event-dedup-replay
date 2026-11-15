-- =====================================================================
-- 01_setup.sql  -  run ONCE as ACCOUNTADMIN in a Snowflake worksheet
-- Creates the warehouse, database, schema, the application role,
-- and a SERVICE user for the Spring Boot service (key-pair login).
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- Small warehouse that suspends itself after 60 s of inactivity (keeps trial credits low)
CREATE WAREHOUSE IF NOT EXISTS EDR_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS EDR_DB;
CREATE SCHEMA IF NOT EXISTS EDR_DB.ANALYTICS;

-- Role that owns and uses all project objects
CREATE ROLE IF NOT EXISTS EDR_APP_ROLE;
GRANT USAGE ON WAREHOUSE EDR_WH TO ROLE EDR_APP_ROLE;
GRANT USAGE ON DATABASE EDR_DB TO ROLE EDR_APP_ROLE;
GRANT ALL ON SCHEMA EDR_DB.ANALYTICS TO ROLE EDR_APP_ROLE;

-- Let YOUR login use the role in worksheets (scripts 03-06 run with it)
SET MY_USER = CURRENT_USER();
GRANT ROLE EDR_APP_ROLE TO USER IDENTIFIER($MY_USER);

-- Service user for the application: no password, signs in with an RSA key pair
CREATE USER IF NOT EXISTS EDR_APP
  TYPE = SERVICE
  DEFAULT_ROLE = EDR_APP_ROLE
  DEFAULT_WAREHOUSE = EDR_WH
  DEFAULT_NAMESPACE = EDR_DB.ANALYTICS
  COMMENT = 'CAP-33 consumer-service (reconciliation reader)';
GRANT ROLE EDR_APP_ROLE TO USER EDR_APP;

-- After generating the key pair (see the Stage 8 guide), paste the PUBLIC key
-- (the text between the BEGIN/END lines, as one line) and run:
-- ALTER USER EDR_APP SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';

-- Check: RSA_PUBLIC_KEY_FP should be filled after the ALTER USER above
-- DESC USER EDR_APP;
