-- =====================================================================
-- 05_views.sql  -  independent reconciliation and analytics (EDR_APP_ROLE)
--
-- These views recompute the EXPECTED final state of every order directly
-- from the raw event log, using only SQL - independently of the Java service -
-- and compare it with the projection snapshot exported by the latest replay.
-- Safe to re-run (CREATE OR REPLACE VIEW).
-- =====================================================================
USE ROLE EDR_APP_ROLE;
USE WAREHOUSE EDR_WH;
USE SCHEMA EDR_DB.ANALYTICS;

-- ---------------------------------------------------------------------
-- 1. EVENTS_PARSED: every raw message, parsed and checked against the
--    event contract (same rules as the consumer's structural validation).
--    RAW_PATH ("raw/2026/11/14/193033367-000000000001-E101.json") sorts in
--    ARRIVAL ORDER; RECEIVED_AT is read back from it (UTC).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW EVENTS_PARSED AS
WITH R AS (
  SELECT
    S3_KEY,
    REGEXP_SUBSTR(S3_KEY, 'raw/.*') AS RAW_PATH,
    REGEXP_SUBSTR(S3_KEY, 'raw/([0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{9})', 1, 1, 'e', 1) AS T,
    RAW_TEXT,
    TRY_PARSE_JSON(RAW_TEXT) AS J
  FROM RAW_EVENTS
),
F AS (
  SELECT
    S3_KEY, RAW_PATH, RAW_TEXT, J,
    TRY_TO_TIMESTAMP_NTZ(SUBSTR(T, 1, 4) || '-' || SUBSTR(T, 6, 2) || '-' || SUBSTR(T, 9, 2) || ' '
                      || SUBSTR(T, 12, 2) || ':' || SUBSTR(T, 14, 2) || ':' || SUBSTR(T, 16, 2) || '.'
                      || SUBSTR(T, 18, 3))                                                      AS RECEIVED_AT,
    IFF(TYPEOF(J:event_id)   = 'VARCHAR', TRIM(J:event_id::STRING), NULL)                     AS EVENT_ID,
    IFF(TYPEOF(J:entity_id)  = 'VARCHAR', TRIM(J:entity_id::STRING), NULL)                    AS ENTITY_ID,
    IFF(TYPEOF(J:version)    = 'INTEGER', J:version::INT, NULL)                               AS VERSION,
    IFF(TYPEOF(J:event_type) = 'VARCHAR', J:event_type::STRING, NULL)                         AS EVENT_TYPE,
    IFF(TYPEOF(J:event_time) = 'VARCHAR'
        AND REGEXP_LIKE(J:event_time::STRING, '.*(Z|[+-][0-9]{2}:?[0-9]{2})'),
        TRY_TO_TIMESTAMP_TZ(J:event_time::STRING), NULL)                                      AS EVENT_TIME,
    J:payload                                                                                  AS PAYLOAD,
    IFF(TYPEOF(J:payload:amount) IN ('INTEGER', 'DECIMAL', 'DOUBLE'),
        J:payload:amount::NUMBER(12, 2), NULL)                                                AS AMOUNT,
    IFF(TYPEOF(J:payload:currency) = 'VARCHAR', J:payload:currency::STRING, NULL)             AS CURRENCY
  FROM R
)
SELECT
  F.*,
  -- COALESCE: a missing field makes the expression NULL - count that as invalid, not unknown
  COALESCE(TYPEOF(J) = 'OBJECT'
   AND EVENT_ID IS NOT NULL AND LENGTH(EVENT_ID) BETWEEN 1 AND 64
   AND ENTITY_ID IS NOT NULL AND LENGTH(ENTITY_ID) BETWEEN 1 AND 64
   AND EVENT_TYPE IN ('OrderCreated', 'PaymentCaptured', 'OrderShipped', 'OrderCancelled')
   AND VERSION IS NOT NULL
   AND EVENT_TIME IS NOT NULL
   AND TYPEOF(PAYLOAD) = 'OBJECT'
   AND ((EVENT_TYPE = 'OrderCreated' AND VERSION = 1) OR (EVENT_TYPE <> 'OrderCreated' AND VERSION >= 2))
   AND CASE EVENT_TYPE
         WHEN 'OrderCreated' THEN
              TYPEOF(PAYLOAD:customer_id) = 'VARCHAR' AND LENGTH(TRIM(PAYLOAD:customer_id::STRING)) BETWEEN 1 AND 64
              AND AMOUNT > 0 AND AMOUNT <= 9999999999.99
              AND REGEXP_LIKE(CURRENCY, '[A-Z]{3}')
         WHEN 'PaymentCaptured' THEN
              TYPEOF(PAYLOAD:payment_id) = 'VARCHAR' AND LENGTH(TRIM(PAYLOAD:payment_id::STRING)) BETWEEN 1 AND 64
              AND AMOUNT > 0 AND AMOUNT <= 9999999999.99
              AND REGEXP_LIKE(CURRENCY, '[A-Z]{3}')
         WHEN 'OrderShipped' THEN
              TYPEOF(PAYLOAD:tracking_id) = 'VARCHAR' AND LENGTH(TRIM(PAYLOAD:tracking_id::STRING)) BETWEEN 1 AND 64
         WHEN 'OrderCancelled' THEN
              TYPEOF(PAYLOAD:reason) = 'VARCHAR' AND LENGTH(TRIM(PAYLOAD:reason::STRING)) BETWEEN 1 AND 255
         ELSE FALSE
       END
  , FALSE) AS IS_VALID
FROM F;

-- ---------------------------------------------------------------------
-- 2. LATEST_SNAPSHOT: the most recent replay snapshot and the point in the
--    log where that replay started (so we recompute the same window).
--    Snapshot keys start with the time, so the alphabetically last is the latest.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW LATEST_SNAPSHOT AS
SELECT
  S3_KEY                                                        AS SNAPSHOT_KEY,
  ANY_VALUE(LINE:replay_run_id::INT)                            AS REPLAY_RUN_ID,
  ANY_VALUE(REGEXP_SUBSTR(LINE:start_key::STRING, 'raw/.*'))    AS START_RAW_PATH,
  COUNT(*)                                                      AS ORDERS_IN_SNAPSHOT,
  MAX(LOADED_AT)                                                AS LOADED_AT
FROM SNAPSHOTS
WHERE S3_KEY = (SELECT MAX(S3_KEY) FROM SNAPSHOTS)
GROUP BY S3_KEY;

-- ---------------------------------------------------------------------
-- 3. CANDIDATE_EVENTS: valid events inside the replay window, keeping the
--    FIRST arrival of each event_id (duplicates ignored), then the FIRST
--    arrival of each (order, version) (a retry with a new id is stale).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW CANDIDATE_EVENTS AS
SELECT *
FROM (
  SELECT *
  FROM EVENTS_PARSED
  WHERE IS_VALID
    AND RAW_PATH >= COALESCE((SELECT START_RAW_PATH FROM LATEST_SNAPSHOT), '')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY RAW_PATH) = 1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ENTITY_ID, VERSION ORDER BY RAW_PATH) = 1;

-- ---------------------------------------------------------------------
-- 4. EXPECTED_CHAIN: walk each order v1 -> v2 -> v3 ... with the state machine.
--    The chain stops at a missing version or at the first rule violation
--    (wrong transition, payment amount/currency not matching the order).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW EXPECTED_CHAIN AS
WITH RECURSIVE CHAIN (ORDER_ID, VERSION, EVENT_ID, EVENT_TYPE, STATUS, ORDER_AMOUNT, AMOUNT_PAID, CURRENCY) AS (
  SELECT ENTITY_ID, VERSION, EVENT_ID, EVENT_TYPE,
         'CREATED'::VARCHAR(20), AMOUNT, 0::NUMBER(12, 2), CURRENCY
  FROM CANDIDATE_EVENTS
  WHERE VERSION = 1 AND EVENT_TYPE = 'OrderCreated'

  UNION ALL

  SELECT C.ORDER_ID, E.VERSION, E.EVENT_ID, E.EVENT_TYPE,
         (CASE E.EVENT_TYPE WHEN 'PaymentCaptured' THEN 'PAID'
                            WHEN 'OrderShipped'    THEN 'SHIPPED'
                            ELSE 'CANCELLED' END)::VARCHAR(20),
         C.ORDER_AMOUNT,
         IFF(E.EVENT_TYPE = 'PaymentCaptured', E.AMOUNT, C.AMOUNT_PAID)::NUMBER(12, 2),
         C.CURRENCY
  FROM CHAIN C
  JOIN CANDIDATE_EVENTS E
    ON E.ENTITY_ID = C.ORDER_ID AND E.VERSION = C.VERSION + 1
  WHERE (E.EVENT_TYPE = 'PaymentCaptured' AND C.STATUS = 'CREATED'
         AND E.AMOUNT = C.ORDER_AMOUNT AND E.CURRENCY = C.CURRENCY)
     OR (E.EVENT_TYPE = 'OrderShipped'   AND C.STATUS = 'PAID')
     OR (E.EVENT_TYPE = 'OrderCancelled' AND C.STATUS IN ('CREATED', 'PAID'))
)
SELECT * FROM CHAIN;

-- 5. EXPECTED_STATE: the last step of each chain = the correct final state
CREATE OR REPLACE VIEW EXPECTED_STATE AS
SELECT ORDER_ID, STATUS, ORDER_AMOUNT, AMOUNT_PAID, CURRENCY,
       VERSION AS LAST_VERSION, EVENT_ID AS LAST_EVENT_ID
FROM EXPECTED_CHAIN
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY VERSION DESC) = 1;

-- 6. EXPECTED_CHARGES: payments that should have produced exactly one charge
CREATE OR REPLACE VIEW EXPECTED_CHARGES AS
SELECT EVENT_ID, ORDER_ID, AMOUNT_PAID AS AMOUNT
FROM EXPECTED_CHAIN
WHERE EVENT_TYPE = 'PaymentCaptured';

-- 7. SNAPSHOT_STATE: the projection the latest replay rebuilt
CREATE OR REPLACE VIEW SNAPSHOT_STATE AS
SELECT
  LINE:order_id::STRING                              AS ORDER_ID,
  LINE:status::STRING                                AS STATUS,
  TRY_TO_NUMBER(LINE:order_amount::STRING, 12, 2)    AS ORDER_AMOUNT,
  TRY_TO_NUMBER(LINE:amount_paid::STRING, 12, 2)     AS AMOUNT_PAID,
  TRY_TO_NUMBER(LINE:last_version::STRING)           AS LAST_VERSION,
  LINE:last_event_id::STRING                         AS LAST_EVENT_ID
FROM SNAPSHOTS
WHERE S3_KEY = (SELECT SNAPSHOT_KEY FROM LATEST_SNAPSHOT);

-- ---------------------------------------------------------------------
-- 8. RECONCILIATION: expected (from the raw log) vs snapshot (from the replay)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW RECONCILIATION AS
SELECT
  COALESCE(E.ORDER_ID, S.ORDER_ID) AS ORDER_ID,
  E.STATUS        AS EXPECTED_STATUS,        S.STATUS        AS SNAPSHOT_STATUS,
  E.AMOUNT_PAID   AS EXPECTED_AMOUNT_PAID,   S.AMOUNT_PAID   AS SNAPSHOT_AMOUNT_PAID,
  E.LAST_VERSION  AS EXPECTED_VERSION,       S.LAST_VERSION  AS SNAPSHOT_VERSION,
  E.LAST_EVENT_ID AS EXPECTED_LAST_EVENT,    S.LAST_EVENT_ID AS SNAPSHOT_LAST_EVENT,
  CASE
    WHEN S.ORDER_ID IS NULL THEN 'MISSING_IN_SNAPSHOT'
    WHEN E.ORDER_ID IS NULL THEN 'NOT_EXPECTED'
    WHEN E.STATUS = S.STATUS AND E.AMOUNT_PAID = S.AMOUNT_PAID
         AND E.LAST_VERSION = S.LAST_VERSION AND E.LAST_EVENT_ID = S.LAST_EVENT_ID THEN 'MATCH'
    ELSE 'MISMATCH'
  END AS RESULT
FROM EXPECTED_STATE E
FULL OUTER JOIN SNAPSHOT_STATE S ON E.ORDER_ID = S.ORDER_ID;

CREATE OR REPLACE VIEW RECONCILIATION_SUMMARY AS
SELECT
  (SELECT SNAPSHOT_KEY  FROM LATEST_SNAPSHOT)  AS SNAPSHOT_KEY,
  (SELECT REPLAY_RUN_ID FROM LATEST_SNAPSHOT)  AS REPLAY_RUN_ID,
  COUNT(*)                                     AS ORDERS_COMPARED,
  COUNT_IF(RESULT = 'MATCH')                   AS MATCHED,
  COUNT_IF(RESULT <> 'MATCH')                  AS DIFFERENT,
  CASE
    WHEN (SELECT COUNT(*) FROM LATEST_SNAPSHOT) = 0 THEN 'NO_SNAPSHOT'
    WHEN COUNT_IF(RESULT <> 'MATCH') = 0        THEN 'MATCH'
    ELSE 'MISMATCH'
  END                                          AS OVERALL,
  (SELECT COUNT(*) FROM EXPECTED_CHARGES)      AS EXPECTED_CHARGES,
  CURRENT_TIMESTAMP()                          AS CHECKED_AT
FROM RECONCILIATION;

-- ---------------------------------------------------------------------
-- 9. Analytics over the whole raw log
-- ---------------------------------------------------------------------

-- Per day: volume, poison messages, duplicate deliveries, delivery delay (event time -> received)
CREATE OR REPLACE VIEW DAILY_METRICS AS
SELECT
  TO_DATE(RECEIVED_AT)                                                         AS DAY,
  COUNT(*)                                                                     AS MESSAGES,
  COUNT_IF(NOT IS_VALID)                                                       AS INVALID_MESSAGES,
  COUNT_IF(IS_VALID)                                                           AS VALID_MESSAGES,
  COUNT(DISTINCT IFF(IS_VALID, EVENT_ID, NULL))                                AS DISTINCT_EVENTS,
  VALID_MESSAGES - DISTINCT_EVENTS                                             AS DUPLICATE_DELIVERIES,
  ROUND(100 * DUPLICATE_DELIVERIES / NULLIF(VALID_MESSAGES, 0), 2)             AS DUPLICATE_RATE_PCT,
  ROUND(AVG(IFF(IS_VALID, DATEDIFF('millisecond',
        CONVERT_TIMEZONE('UTC', EVENT_TIME)::TIMESTAMP_NTZ, RECEIVED_AT), NULL)))  AS AVG_DELIVERY_DELAY_MS,
  MAX(IFF(IS_VALID, DATEDIFF('millisecond',
        CONVERT_TIMEZONE('UTC', EVENT_TIME)::TIMESTAMP_NTZ, RECEIVED_AT), NULL))   AS MAX_DELIVERY_DELAY_MS
FROM EVENTS_PARSED
GROUP BY TO_DATE(RECEIVED_AT);

-- Events that arrived before their immediate predecessor version (or whose predecessor never arrived)
CREATE OR REPLACE VIEW EARLY_ARRIVALS AS
WITH FIRSTS AS (
  SELECT EVENT_ID, ENTITY_ID, VERSION, EVENT_TYPE, RAW_PATH, RECEIVED_AT
  FROM EVENTS_PARSED
  WHERE IS_VALID
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY RAW_PATH) = 1
)
SELECT A.*
FROM FIRSTS A
LEFT JOIN FIRSTS B
  ON B.ENTITY_ID = A.ENTITY_ID AND B.VERSION = A.VERSION - 1 AND B.RAW_PATH < A.RAW_PATH
WHERE A.VERSION > 1 AND B.EVENT_ID IS NULL;

-- Dead letters per day and error code
CREATE OR REPLACE VIEW DEAD_LETTER_TRENDS AS
SELECT
  TO_DATE(TRY_TO_TIMESTAMP_TZ(RECORD:dead_lettered_at::STRING)) AS DAY,
  RECORD:error_code::STRING                                    AS ERROR_CODE,
  COUNT(*)                                                     AS DEAD_LETTERS
FROM DEAD_LETTERS
GROUP BY 1, 2;
