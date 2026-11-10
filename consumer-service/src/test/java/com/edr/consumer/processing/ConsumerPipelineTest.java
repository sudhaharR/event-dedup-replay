package com.edr.consumer.processing;

import com.edr.consumer.AbstractMySqlTest;
import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.ingest.IngestService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end scenarios of the consumer pipeline against a real (throwaway) MySQL:
 * duplicate payment, out-of-order status, poison events, partial side effects,
 * and the evaluator test (shuffled arrival order + repeated message).
 */
class ConsumerPipelineTest extends AbstractMySqlTest {

    @Autowired
    private IngestService ingestService;

    @Autowired
    private InboxConsumer consumer;

    @Autowired
    private FaultInjector faultInjector;

    @Autowired
    private JsonMapper jsonMapper;

    // ------------------------------------------------------------------ helpers

    private void send(String body) {
        ingestService.ingest(body, EventSource.LIVE);
    }

    private static String event(String id, String order, int version, String type, String time, String payload) {
        return """
                {"event_id":"%s","entity_id":"%s","version":%d,"event_type":"%s",
                 "event_time":"2026-11-10T%sZ","payload":%s}""".formatted(id, order, version, type, time, payload);
    }

    private static String created(String id, String order, int amount) {
        return event(id, order, 1, "OrderCreated", "10:00:00",
                "{\"customer_id\":\"CUST-1\",\"amount\":" + amount + ",\"currency\":\"INR\"}");
    }

    private static String paid(String id, String order, int version, String paymentId, int amount) {
        return event(id, order, version, "PaymentCaptured", "10:00:05",
                "{\"payment_id\":\"" + paymentId + "\",\"amount\":" + amount + ",\"currency\":\"INR\"}");
    }

    private static String shipped(String id, String order, int version) {
        return event(id, order, version, "OrderShipped", "10:01:00", "{\"tracking_id\":\"TRK-" + order + "\"}");
    }

    private Map<String, Object> order(String orderId) {
        return jdbc.queryForMap(
                "SELECT status, amount_paid, last_version FROM order_projection WHERE order_id = ?", orderId);
    }

    private void assertOrder(String orderId, String status, String paid, int version) {
        Map<String, Object> o = order(orderId);
        assertEquals(status, o.get("status"), orderId + " status");
        assertEquals(paid, o.get("amount_paid").toString(), orderId + " amount_paid");
        assertEquals(version, ((Number) o.get("last_version")).intValue(), orderId + " last_version");
    }

    private int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    private int inboxWithStatus(String status) {
        return count("SELECT COUNT(*) FROM inbox WHERE status = ?", status);
    }

    private List<String> sampleEvents() throws Exception {
        JsonNode array = jsonMapper.readTree(Files.readString(Path.of("../simulator/samples/sample_events.json")));
        List<String> events = new ArrayList<>();
        array.forEach(e -> events.add(e.toString()));
        return events;
    }

    // ------------------------------------------------------------------ scenarios

    @Test
    void sampleDatasetEndsInCorrectState() throws Exception {
        send(Files.readString(Path.of("../simulator/samples/sample_events.json")));

        consumer.drainAll();

        assertOrder("ORD-1", "SHIPPED", "500.00", 3);
        assertOrder("ORD-2", "SHIPPED", "300.00", 3);
        assertEquals(2, count("SELECT COUNT(*) FROM payment_ledger"), "exactly 2 charges");
        assertEquals(6, inboxWithStatus("APPLIED"));
        assertEquals(1, inboxWithStatus("DUPLICATE"));
        assertEquals(1, inboxWithStatus("DEAD_LETTERED"));
        assertEquals(1, count("SELECT COUNT(*) FROM inbox WHERE was_out_of_order = TRUE"));
        assertEquals(0, count("SELECT COUNT(*) FROM park_buffer"));
        assertEquals("MISSING_FIELD",
                jdbc.queryForObject("SELECT error_code FROM dead_letter WHERE event_id = 'E300'", String.class));
    }

    @Test
    void duplicatePaymentIsChargedOnlyOnce() {
        send("[" + created("E1", "ORD-1", 500) + "," + paid("E2", "ORD-1", 2, "PAY-1", 500) + ","
                + paid("E2", "ORD-1", 2, "PAY-1", 500) + "," + paid("E2", "ORD-1", 2, "PAY-1", 500) + "]");

        consumer.drainAll();

        assertOrder("ORD-1", "PAID", "500.00", 2);
        assertEquals(1, count("SELECT COUNT(*) FROM payment_ledger WHERE event_id = 'E2'"));
        assertEquals(2, inboxWithStatus("DUPLICATE"));
    }

    @Test
    void outOfOrderStatusIsParkedThenApplied() {
        // v3 arrives first, then v2 - both must wait for v1
        send(shipped("E3", "ORD-1", 3));
        consumer.drainAll();
        assertEquals(0, count("SELECT COUNT(*) FROM order_projection"));
        assertEquals(1, count("SELECT COUNT(*) FROM park_buffer"));

        send(paid("E2", "ORD-1", 2, "PAY-1", 500));
        consumer.drainAll();
        assertEquals(2, count("SELECT COUNT(*) FROM park_buffer"));

        send(created("E1", "ORD-1", 500));
        consumer.drainAll();

        assertOrder("ORD-1", "SHIPPED", "500.00", 3);
        assertEquals(0, count("SELECT COUNT(*) FROM park_buffer"));
        assertEquals(2, count("SELECT COUNT(*) FROM inbox WHERE was_out_of_order = TRUE"));
        assertEquals(3, inboxWithStatus("APPLIED"));
    }

    @Test
    void poisonEventsAreIsolatedAndDoNotBlockOthers() {
        send("{\"event_id\": \"E400\", ");                                                  // broken JSON
        send(event("E401", "ORD-1", 2, "PaymentRefunded", "10:00:01", "{}"));               // unknown type
        send(event("E402", "ORD-1", 2, "PaymentCaptured", "10:00:02",
                "{\"payment_id\":\"PAY-X\",\"amount\":\"abc\",\"currency\":\"INR\"}"));      // bad amount
        send(created("E1", "ORD-1", 500));                                                  // valid, after poison

        consumer.drainAll();

        assertOrder("ORD-1", "CREATED", "0.00", 1);
        assertEquals(3, count("SELECT COUNT(*) FROM dead_letter"));
        assertEquals(1, count("SELECT COUNT(*) FROM dead_letter WHERE error_code = 'MALFORMED_JSON'"));
        assertEquals(1, count("SELECT COUNT(*) FROM dead_letter WHERE error_code = 'UNKNOWN_EVENT_TYPE'"));
        assertEquals(1, count("SELECT COUNT(*) FROM dead_letter WHERE error_code = 'INVALID_PAYLOAD'"));
    }

    @Test
    void crashAfterSideEffectIsRolledBackAndRetried() {
        faultInjector.failOnceFor("E2");
        send("[" + created("E1", "ORD-1", 500) + "," + paid("E2", "ORD-1", 2, "PAY-1", 500) + "]");

        consumer.runOnce();   // E1 applied, E2 crashes after charging -> whole transaction rolled back

        assertOrder("ORD-1", "CREATED", "0.00", 1);
        assertEquals(0, count("SELECT COUNT(*) FROM payment_ledger"), "no partial charge");
        assertEquals(0, count("SELECT COUNT(*) FROM processed_events WHERE event_id = 'E2'"));
        assertEquals(1, count("SELECT attempts FROM inbox WHERE event_id = 'E2'"));
        assertEquals(1, inboxWithStatus("PENDING"));

        consumer.drainAll();  // retry succeeds

        assertOrder("ORD-1", "PAID", "500.00", 2);
        assertEquals(1, count("SELECT COUNT(*) FROM payment_ledger"));
    }

    @Test
    void repeatedFailuresEndInDeadLetter() {
        send("[" + created("E1", "ORD-1", 500) + "," + paid("E2", "ORD-1", 2, "PAY-1", 500) + "]");
        for (int i = 0; i < 3; i++) {
            faultInjector.failOnceFor("E2");
            consumer.runOnce();
        }

        assertEquals("PROCESSING_ERROR",
                jdbc.queryForObject("SELECT error_code FROM dead_letter WHERE event_id = 'E2'", String.class));
        assertEquals(0, count("SELECT COUNT(*) FROM payment_ledger"));
        assertOrder("ORD-1", "CREATED", "0.00", 1);
    }

    @Test
    void businessRuleViolationsAreDeadLettered() {
        send("[" + created("E1", "ORD-1", 500) + ","
                + shipped("E2", "ORD-1", 2) + ","                         // ship before pay
                + created("E10", "ORD-2", 300) + ","
                + paid("E11", "ORD-2", 2, "PAY-2", 999) + "]");           // wrong amount

        consumer.drainAll();

        assertEquals("INVALID_TRANSITION",
                jdbc.queryForObject("SELECT error_code FROM dead_letter WHERE event_id = 'E2'", String.class));
        assertEquals("AMOUNT_MISMATCH",
                jdbc.queryForObject("SELECT error_code FROM dead_letter WHERE event_id = 'E11'", String.class));
        assertOrder("ORD-1", "CREATED", "0.00", 1);
        assertOrder("ORD-2", "CREATED", "0.00", 1);
        assertEquals(0, count("SELECT COUNT(*) FROM payment_ledger"));
    }

    @Test
    void sameEventIdWithDifferentContentIsConflict() {
        send("[" + created("E1", "ORD-1", 500) + "," + created("E1", "ORD-1", 999) + "]");

        consumer.drainAll();

        assertOrder("ORD-1", "CREATED", "0.00", 1);
        assertEquals("EVENT_ID_CONFLICT",
                jdbc.queryForObject("SELECT error_code FROM dead_letter WHERE event_id = 'E1'", String.class));
    }

    @Test
    void sameVersionWithNewEventIdIsStale() {
        // producer retried v2 with a NEW event_id: idempotency misses it, the version check catches it
        send("[" + created("E1", "ORD-1", 500) + "," + paid("E2", "ORD-1", 2, "PAY-1", 500) + ","
                + paid("E2-RETRY", "ORD-1", 2, "PAY-1", 500) + "]");

        consumer.drainAll();

        assertEquals("STALE", jdbc.queryForObject("SELECT status FROM inbox WHERE event_id = 'E2-RETRY'", String.class));
        assertEquals(1, count("SELECT COUNT(*) FROM payment_ledger"));
    }

    @Test
    void evaluatorShufflesAndRepeatsFinalStateIsAlwaysTheSame() throws Exception {
        List<String> events = sampleEvents();
        events.add(events.get(0));              // repeat E101
        events.add(events.get(6));              // repeat E202 (payment)

        for (int seed = 1; seed <= 25; seed++) {
            cleanTables();
            List<String> shuffled = new ArrayList<>(events);
            Collections.shuffle(shuffled, new Random(seed));

            for (String e : shuffled) {           // one message per request, like a real stream
                send(e);
                consumer.runOnce();
            }
            consumer.drainAll();

            assertOrder("ORD-1", "SHIPPED", "500.00", 3);
            assertOrder("ORD-2", "SHIPPED", "300.00", 3);
            assertEquals(2, count("SELECT COUNT(*) FROM payment_ledger"), "seed " + seed);
            assertEquals(1, count("SELECT COUNT(*) FROM dead_letter"), "seed " + seed);
            assertEquals(0, count("SELECT COUNT(*) FROM park_buffer"), "seed " + seed);
            assertEquals(3, inboxWithStatus("DUPLICATE"), "seed " + seed);
        }
    }
}
