package com.edr.consumer.archive;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Object names in the bucket.
 *
 *   <prefix>raw/2026/11/13/091502123-000000000042-E102.json
 *   <prefix>dlq/2026/11/13/091502987-000000000046-E300.json
 *
 * - Partitioned by UTC day (cheap listing per day, Snowflake-friendly).
 * - Time + zero-padded inbox id: sorting keys alphabetically gives ARRIVAL ORDER,
 *   which is what replay needs. Every delivery gets its own object, so duplicates are kept.
 * - The time part keeps keys unique even after the inbox ids restart (e.g. after a dev reset).
 */
public final class S3Keys {

    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HHmmssSSS").withZone(ZoneOffset.UTC);

    private S3Keys() {
    }

    public static String raw(String prefix, long inboxId, String eventId, Instant receivedAt) {
        return key(prefix, "raw", inboxId, eventId, receivedAt);
    }

    public static String deadLetter(String prefix, long inboxId, String eventId, Instant receivedAt) {
        return key(prefix, "dlq", inboxId, eventId, receivedAt);
    }

    private static String key(String prefix, String area, long inboxId, String eventId, Instant at) {
        return prefix + area + "/" + DAY.format(at) + "/" + TIME.format(at) + "-"
                + String.format("%012d", inboxId) + "-" + safe(eventId) + ".json";
    }

    /** event ids come from producers - keep only characters that are safe in a key. */
    static String safe(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            return "unparsed";
        }
        String s = eventId.replaceAll("[^A-Za-z0-9._-]", "_");
        return s.length() > 64 ? s.substring(0, 64) : s;
    }
}
