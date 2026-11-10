package com.edr.consumer.processing;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Phase A validation rules - pure unit test, no Spring, no database. */
class EventValidatorTest {

    private final EventValidator validator = new EventValidator(JsonMapper.builder().build());

    private static final String PAYMENT = """
            {"event_id":"E102","entity_id":"ORD-1","version":2,"event_type":"PaymentCaptured",
             "event_time":"2026-11-10T10:00:05Z",
             "payload":{"payment_id":"PAY-1","amount":500.00,"currency":"INR"}}
            """;

    private ErrorCode rejectCode(String raw) {
        return assertThrows(RejectedEventException.class, () -> validator.validate(raw)).getCode();
    }

    @Test
    void validEventPasses() {
        ValidatedEvent e = validator.validate(PAYMENT);
        assertEquals("E102", e.eventId());
        assertEquals("ORD-1", e.entityId());
        assertEquals(2, e.version());
        assertEquals(EventType.PAYMENT_CAPTURED, e.type());
        assertEquals("500.00", e.amount().toPlainString());
    }

    @Test
    void brokenJsonIsMalformed() {
        assertEquals(ErrorCode.MALFORMED_JSON, rejectCode("{\"event_id\": \"E400\", "));
        assertEquals(ErrorCode.MALFORMED_JSON, rejectCode("[1,2,3]"));
    }

    @Test
    void missingEntityIdIsMissingField() {
        assertEquals(ErrorCode.MISSING_FIELD, rejectCode(PAYMENT.replace("\"entity_id\":\"ORD-1\",", "")));
    }

    @Test
    void textVersionIsInvalidFieldType() {
        assertEquals(ErrorCode.INVALID_FIELD_TYPE, rejectCode(PAYMENT.replace("\"version\":2", "\"version\":\"two\"")));
    }

    @Test
    void badTimestampIsInvalidFieldType() {
        assertEquals(ErrorCode.INVALID_FIELD_TYPE,
                rejectCode(PAYMENT.replace("2026-11-10T10:00:05Z", "yesterday")));
    }

    @Test
    void unknownTypeIsRejected() {
        assertEquals(ErrorCode.UNKNOWN_EVENT_TYPE, rejectCode(PAYMENT.replace("PaymentCaptured", "PaymentRefunded")));
    }

    @Test
    void orderCreatedMustBeVersionOne() {
        String created = """
                {"event_id":"E1","entity_id":"ORD-9","version":2,"event_type":"OrderCreated",
                 "event_time":"2026-11-10T10:00:00Z","payload":{"customer_id":"C","amount":10,"currency":"INR"}}
                """;
        assertEquals(ErrorCode.INVALID_VERSION, rejectCode(created));
    }

    @Test
    void textAmountIsInvalidPayload() {
        assertEquals(ErrorCode.INVALID_PAYLOAD, rejectCode(PAYMENT.replace("500.00", "\"abc\"")));
    }

    @Test
    void lowercaseCurrencyIsInvalidPayload() {
        assertEquals(ErrorCode.INVALID_PAYLOAD, rejectCode(PAYMENT.replace("\"INR\"", "\"inr\"")));
    }

    @Test
    void sameContentGivesSameHashDifferentContentDoesNot() {
        String h1 = validator.validate(PAYMENT).payloadHash();
        String h2 = validator.validate(PAYMENT).payloadHash();
        String h3 = validator.validate(PAYMENT.replace("500.00", "499.00")).payloadHash();
        assertEquals(h1, h2);
        assertNotEquals(h1, h3);
    }
}
