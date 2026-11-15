    -- =====================================================================
    -- 06_checks.sql  -  queries to look at the results in a worksheet
    -- =====================================================================
    USE ROLE EDR_APP_ROLE;
    USE WAREHOUSE EDR_WH;
    USE SCHEMA EDR_DB.ANALYTICS;

    -- Is the reconciliation a MATCH?
    SELECT * FROM RECONCILIATION_SUMMARY;

    -- Order by order: expected (from the raw log) vs snapshot (from the replay)
    SELECT * FROM RECONCILIATION ORDER BY ORDER_ID;

    -- Which replay snapshot is being checked, and from which point in the log
    SELECT * FROM LATEST_SNAPSHOT;

    -- Every raw message with its validity (poison messages have IS_VALID = FALSE)
    SELECT RAW_PATH, EVENT_ID, ENTITY_ID, VERSION, EVENT_TYPE, IS_VALID
    FROM EVENTS_PARSED ORDER BY RAW_PATH DESC LIMIT 50;

    -- Expected final state and expected charges
    SELECT * FROM EXPECTED_STATE ORDER BY ORDER_ID;
    SELECT * FROM EXPECTED_CHARGES ORDER BY ORDER_ID;

    -- Analytics
    SELECT * FROM DAILY_METRICS ORDER BY DAY DESC;
    SELECT * FROM EARLY_ARRIVALS ORDER BY RAW_PATH DESC LIMIT 50;
    SELECT * FROM DEAD_LETTER_TRENDS ORDER BY DAY DESC, ERROR_CODE;
