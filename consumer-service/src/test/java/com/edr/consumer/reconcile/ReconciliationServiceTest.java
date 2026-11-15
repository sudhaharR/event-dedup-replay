package com.edr.consumer.reconcile;

import com.edr.consumer.AbstractMySqlTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Java side of the Snowflake reconciliation. Snowflake itself cannot run in a test, so MySQL stands in
 * for it: tables with the same names and columns as the Snowflake views, filled with known rows.
 * The Snowflake SQL (snowflake/05_views.sql) is verified on the real account.
 */
class ReconciliationServiceTest extends AbstractMySqlTest {

    private ReconciliationService service;

    @BeforeEach
    void standInForSnowflake() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS RECONCILIATION_SUMMARY (SNAPSHOT_KEY VARCHAR(200), REPLAY_RUN_ID INT,
                  ORDERS_COMPARED INT, MATCHED INT, DIFFERENT INT, OVERALL VARCHAR(20), EXPECTED_CHARGES INT)""");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS RECONCILIATION (ORDER_ID VARCHAR(64), EXPECTED_STATUS VARCHAR(20),
                  SNAPSHOT_STATUS VARCHAR(20), RESULT VARCHAR(30))""");
        jdbc.execute("CREATE TABLE IF NOT EXISTS EXPECTED_CHARGES (EVENT_ID VARCHAR(64), ORDER_ID VARCHAR(64), AMOUNT DECIMAL(12,2))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS DAILY_METRICS (DAY DATE, MESSAGES INT, DUPLICATE_DELIVERIES INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS DEAD_LETTER_TRENDS (DAY DATE, ERROR_CODE VARCHAR(40), DEAD_LETTERS INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS LOAD_LOG (N INT)");
        for (String t : List.of("RECONCILIATION_SUMMARY", "RECONCILIATION", "EXPECTED_CHARGES",
                "DAILY_METRICS", "DEAD_LETTER_TRENDS", "LOAD_LOG")) {
            jdbc.execute("DELETE FROM " + t);
        }
        // the Snowflake COPY INTO statements are replaced by a harmless statement we can count
        service = new ReconciliationService(new SnowflakeClient(jdbc, "test://stand-in", null), jdbc,
                List.of("INSERT INTO LOAD_LOG (N) VALUES (1)", "INSERT INTO LOAD_LOG (N) VALUES (2)"));
    }

    private void charge(String eventId, String orderId) {
        jdbc.update("INSERT INTO payment_ledger (event_id, order_id, payment_id, amount, currency, created_at) "
                + "VALUES (?, ?, ?, 100, 'INR', NOW(3))", eventId, orderId, "PAY-" + eventId);
    }

    @Test
    @SuppressWarnings("unchecked")
    void matchingStateAndChargesGiveMatch() {
        jdbc.update("INSERT INTO RECONCILIATION_SUMMARY VALUES ('snapshots/x.json', 1, 2, 2, 0, 'MATCH', 2)");
        jdbc.update("INSERT INTO RECONCILIATION VALUES ('ORD-1','SHIPPED','SHIPPED','MATCH'), ('ORD-2','SHIPPED','SHIPPED','MATCH')");
        jdbc.update("INSERT INTO EXPECTED_CHARGES VALUES ('E102','ORD-1',500), ('E202','ORD-2',300)");
        jdbc.update("INSERT INTO DAILY_METRICS VALUES ('2026-11-15', 8, 1)");
        charge("E102", "ORD-1");
        charge("E202", "ORD-2");
        charge("E999", "ORD-OTHER");            // a charge of an order outside the reconciled set is ignored

        Map<String, Object> r = service.refresh();

        assertEquals(2, jdbc.queryForObject("SELECT COUNT(*) FROM LOAD_LOG", Integer.class), "load statements ran");
        Map<String, Object> summary = (Map<String, Object>) r.get("summary");
        assertEquals("MATCH", summary.get("overall"), "keys are snake_case");
        assertEquals(List.of(), r.get("differences"));
        Map<String, Object> charges = (Map<String, Object>) r.get("charges");
        assertEquals("MATCH", charges.get("result"));
        assertEquals(2, charges.get("expected_charges"));
        assertEquals(2, charges.get("ledger_charges"));
        assertEquals(1, ((List<?>) r.get("daily_metrics")).size());
        assertTrue(r.containsKey("load_ms"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void differencesAndMissingOrExtraChargesAreReported() {
        jdbc.update("INSERT INTO RECONCILIATION_SUMMARY VALUES ('snapshots/x.json', 1, 2, 1, 1, 'MISMATCH', 2)");
        jdbc.update("INSERT INTO RECONCILIATION VALUES ('ORD-1','SHIPPED','SHIPPED','MATCH'), ('ORD-2','SHIPPED','CREATED','MISMATCH')");
        jdbc.update("INSERT INTO EXPECTED_CHARGES VALUES ('E102','ORD-1',500), ('E202','ORD-2',300)");
        charge("E102", "ORD-1");
        charge("E555", "ORD-2");                 // ledger has a charge Snowflake did not expect; E202 is missing

        Map<String, Object> r = service.result();

        List<Map<String, Object>> diffs = (List<Map<String, Object>>) r.get("differences");
        assertEquals(1, diffs.size());
        assertEquals("ORD-2", diffs.get(0).get("order_id"));
        Map<String, Object> charges = (Map<String, Object>) r.get("charges");
        assertEquals("MISMATCH", charges.get("result"));
        assertEquals(Set.of("E202"), charges.get("missing_charges"));
        assertEquals(Set.of("E555"), charges.get("unexpected_charges"));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM LOAD_LOG", Integer.class), "result() loads nothing");
    }

    @Test
    void jdbcUrlIsBuiltFromTheAccountIdentifier() {
        SnowflakeProperties p = new SnowflakeProperties(true, "abc12345.ap-south-1.snowflakecomputing.com",
                "EDR_APP", "k.p8", "EDR_WH", "EDR_DB", "ANALYTICS", "EDR_APP_ROLE");
        assertEquals("jdbc:snowflake://abc12345.ap-south-1.snowflakecomputing.com/"
                + "?warehouse=EDR_WH&db=EDR_DB&schema=ANALYTICS&role=EDR_APP_ROLE", p.jdbcUrl());
    }
}
