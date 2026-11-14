package com.edr.consumer.archive;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Object naming - pure unit test. */
class S3KeysTest {

    private static final Instant T = Instant.parse("2026-11-13T09:15:02.123Z");

    @Test
    void rawKeyIsPartitionedByDayAndSortableByArrival() {
        assertEquals("dev/raw/2026/11/13/091502123-000000000042-E102.json", S3Keys.raw("dev/", 42, "E102", T));
        assertEquals("dlq/2026/11/13/091502123-000000000046-E300.json", S3Keys.deadLetter("", 46, "E300", T));
    }

    @Test
    void unreadableOrUnsafeEventIdsAreMadeSafe() {
        assertEquals("unparsed", S3Keys.safe(null));
        assertEquals("unparsed", S3Keys.safe("  "));
        assertEquals("E_1_x", S3Keys.safe("E/1 x"));
        assertEquals(64, S3Keys.safe("X".repeat(100)).length());
    }

    @Test
    void alphabeticalOrderOfKeysIsArrivalOrder() {
        List<String> keys = new ArrayList<>();
        for (long id = 1; id <= 12; id++) {
            keys.add(S3Keys.raw("", id, "E", T.plusMillis(id)));
        }
        List<String> sorted = new ArrayList<>(keys);
        sorted.sort(String::compareTo);
        assertEquals(keys, sorted);
    }

    @Test
    void prefixIsNormalised() {
        assertEquals("", new S3Properties(true, "b", "r", null, null, null, null, 1000).normalizedPrefix());
        assertEquals("dev/", new S3Properties(true, "b", "r", "dev", null, null, null, 1000).normalizedPrefix());
        assertEquals("dev/", new S3Properties(true, "b", "r", "/dev/", null, null, null, 1000).normalizedPrefix());
    }
}
