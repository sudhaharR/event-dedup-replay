package com.edr.consumer.processing;

import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Instant;

/** An event that passed structural validation. payloadHash identifies its content. */
public record ValidatedEvent(
        String eventId,
        String entityId,
        int version,
        EventType type,
        Instant eventTime,
        JsonNode payload,
        String payloadHash) {

    public String text(String field) {
        return payload.get(field).stringValue();
    }

    public BigDecimal amount() {
        return payload.get("amount").decimalValue().setScale(2, java.math.RoundingMode.HALF_UP);
    }
}
