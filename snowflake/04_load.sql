-- =====================================================================
-- 04_load.sql  -  loads NEW objects from S3 (run as EDR_APP_ROLE).
-- Safe to run any number of times: COPY INTO remembers which files it has
-- already loaded and skips them. The Spring Boot service runs these same
-- three statements on POST /reconciliation/refresh.
-- =====================================================================
USE ROLE EDR_APP_ROLE;
USE WAREHOUSE EDR_WH;
USE SCHEMA EDR_DB.ANALYTICS;

COPY INTO RAW_EVENTS (S3_KEY, RAW_TEXT, FILE_LAST_MODIFIED)
  FROM (SELECT METADATA$FILENAME, $1, METADATA$FILE_LAST_MODIFIED FROM @EDR_STAGE/raw/)
  FILE_FORMAT = (FORMAT_NAME = 'FF_WHOLE_FILE')
  ON_ERROR = 'CONTINUE';

COPY INTO DEAD_LETTERS (S3_KEY, RECORD)
  FROM (SELECT METADATA$FILENAME, $1 FROM @EDR_STAGE/dlq/)
  FILE_FORMAT = (FORMAT_NAME = 'FF_JSON_LINES')
  ON_ERROR = 'CONTINUE';

COPY INTO SNAPSHOTS (S3_KEY, LINE)
  FROM (SELECT METADATA$FILENAME, $1 FROM @EDR_STAGE/snapshots/)
  FILE_FORMAT = (FORMAT_NAME = 'FF_JSON_LINES')
  ON_ERROR = 'CONTINUE';

SELECT
  (SELECT COUNT(*) FROM RAW_EVENTS)   AS RAW_EVENTS,
  (SELECT COUNT(*) FROM DEAD_LETTERS) AS DEAD_LETTERS,
  (SELECT COUNT(*) FROM SNAPSHOTS)    AS SNAPSHOT_LINES;
