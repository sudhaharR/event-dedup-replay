-- =====================================================================
-- 03_stage_and_tables.sql  -  run as EDR_APP_ROLE (after the AWS trust
-- policy is updated). Replace <your-bucket-name>.
-- =====================================================================
USE ROLE EDR_APP_ROLE;
USE WAREHOUSE EDR_WH;
USE SCHEMA EDR_DB.ANALYTICS;

-- Each raw object is ONE message, possibly broken JSON. Load the whole file
-- as plain text so even malformed messages are kept; parse later with TRY_PARSE_JSON.
CREATE FILE FORMAT IF NOT EXISTS FF_WHOLE_FILE
  TYPE = CSV
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = NONE
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  ESCAPE = NONE
  ESCAPE_UNENCLOSED_FIELD = NONE
  ENCODING = 'UTF8';

-- Dead-letter records and replay snapshots are valid JSON (snapshots: one order per line)
CREATE FILE FORMAT IF NOT EXISTS FF_JSON_LINES
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

CREATE STAGE IF NOT EXISTS EDR_STAGE
  STORAGE_INTEGRATION = EDR_S3_INT
  URL = 's3://<your-bucket-name>/dev/';

-- Should list your raw/, dlq/ and snapshots/ objects. If this fails, the AWS role/trust is not right.
LIST @EDR_STAGE;

CREATE TABLE IF NOT EXISTS RAW_EVENTS (
  S3_KEY              STRING NOT NULL,      -- object path, e.g. raw/2026/11/14/193033367-000000000001-E101.json
  RAW_TEXT            STRING,               -- the message exactly as received
  FILE_LAST_MODIFIED  TIMESTAMP_NTZ,
  LOADED_AT           TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DEAD_LETTERS (
  S3_KEY     STRING NOT NULL,
  RECORD     VARIANT,                       -- {inbox_id, event_id, error_code, error_message, raw_payload, ...}
  LOADED_AT  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SNAPSHOTS (
  S3_KEY     STRING NOT NULL,               -- snapshots/2026/11/14/193257052-replay-000001.json
  LINE       VARIANT,                       -- one order of the rebuilt projection
  LOADED_AT  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
