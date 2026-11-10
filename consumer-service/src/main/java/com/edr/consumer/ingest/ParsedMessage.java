package com.edr.consumer.ingest;

import java.time.Instant;

/**
 * One message split out of a request body.
 * rawPayload is always set; the other fields are null when they could not be read.
 */
public record ParsedMessage(
        String rawPayload,
        String eventId,
        String entityId,
        Integer version,
        String eventType,
        Instant eventTime) {

    static ParsedMessage unreadable(String raw) {
        return new ParsedMessage(raw, null, null, null, null, null);
    }
}
