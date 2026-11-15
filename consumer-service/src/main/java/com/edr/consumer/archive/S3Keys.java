package com.edr.consumer.archive;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * Projection snapshot written after a replay: <prefix>snapshots/2026/11/13/091502123-replay-000007.json
     * Time first, so the alphabetically last key is always the latest snapshot (run ids restart after a reset).
     */
    public static String snapshot(String prefix, long replayRunId, Instant at) {
        return prefix + "snapshots/" + DAY.format(at) + "/" + TIME.format(at)
                + "-replay-" + String.format("%06d", replayRunId) + ".json";
    }

    /** Reads the receive time back from a raw/dlq key; empty if the key is not in that format. */
    public static Optional<Instant> receivedAt(String key) {
        Matcher m = KEY_TIME.matcher(key);
        if (!m.find()) {
            return Optional.empty();
        }
        try {
            LocalDate day = LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                    Integer.parseInt(m.group(3)));
            LocalTime time = LocalTime.parse(m.group(4), DateTimeFormatter.ofPattern("HHmmssSSS"));
            return Optional.of(LocalDateTime.of(day, time).toInstant(ZoneOffset.UTC));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static final Pattern KEY_TIME =
            Pattern.compile("(?:raw|dlq)/(\\d{4})/(\\d{2})/(\\d{2})/(\\d{9})-\\d{12}-");

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
