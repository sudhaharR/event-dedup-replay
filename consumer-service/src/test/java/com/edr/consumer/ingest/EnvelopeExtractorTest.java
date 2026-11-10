package com.edr.consumer.ingest;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Pure unit test - no Spring, no database. */
class EnvelopeExtractorTest {

    private final EnvelopeExtractor extractor = new EnvelopeExtractor(JsonMapper.builder().build());

    @Test
    void singleValidEventIsFullyExtracted() {
        String body = """
                {"event_id":"E101","entity_id":"ORD-1","version":1,"event_type":"OrderCreated",
                 "event_time":"2026-11-10T10:00:00Z",
                 "payload":{"customer_id":"CUST-1","amount":500.00,"currency":"INR"}}
                """;

        List<ParsedMessage> result = extractor.split(body);

        assertEquals(1, result.size());
        ParsedMessage m = result.get(0);
        assertEquals("E101", m.eventId());
        assertEquals("ORD-1", m.entityId());
        assertEquals(1, m.version());
        assertEquals("OrderCreated", m.eventType());
        assertEquals(Instant.parse("2026-11-10T10:00:00Z"), m.eventTime());
    }

    @Test
    void arrayIsSplitIntoOneMessagePerElementInOrder() {
        String body = """
                [{"event_id":"E101","entity_id":"ORD-1","version":1},
                 {"event_id":"E102","entity_id":"ORD-1","version":2},
                 {"event_id":"E102","entity_id":"ORD-1","version":2}]
                """;

        List<ParsedMessage> result = extractor.split(body);

        assertEquals(3, result.size());
        assertEquals("E101", result.get(0).eventId());
        assertEquals("E102", result.get(1).eventId());
        assertEquals("E102", result.get(2).eventId());   // duplicate kept - consumer decides
    }

    @Test
    void brokenJsonIsKeptAsOneUnreadableMessage() {
        String body = "{\"event_id\": \"E400\", ";

        List<ParsedMessage> result = extractor.split(body);

        assertEquals(1, result.size());
        assertEquals(body, result.get(0).rawPayload());
        assertNull(result.get(0).eventId());
    }

    @Test
    void wrongTypesAreReadAsNullButMessageIsKept() {
        String body = """
                {"event_id":"E300","version":"two","event_type":"PaymentCaptured",
                 "event_time":"not-a-date","payload":{"amount":"abc"}}
                """;

        ParsedMessage m = extractor.split(body).get(0);

        assertEquals("E300", m.eventId());
        assertNull(m.entityId());     // missing
        assertNull(m.version());      // "two" is not a number
        assertNull(m.eventTime());    // not a valid timestamp
        assertEquals("PaymentCaptured", m.eventType());
    }

    @Test
    void tooLongIdIsReadAsNull() {
        String longId = "X".repeat(65);
        ParsedMessage m = extractor.split("{\"event_id\":\"" + longId + "\"}").get(0);
        assertNull(m.eventId());
    }
}
