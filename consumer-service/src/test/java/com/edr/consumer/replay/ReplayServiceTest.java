package com.edr.consumer.replay;

import com.edr.consumer.AbstractMySqlTest;
import com.edr.consumer.archive.FakeRawEventStore;
import com.edr.consumer.archive.S3Archiver;
import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.ingest.IngestService;
import com.edr.consumer.processing.InboxConsumer;
import com.edr.consumer.query.QueryController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replay rebuilds the projection from the raw event log without repeating side effects,
 * repairs a damaged projection, refuses to run on a busy system, and never inflates live metrics.
 */
@TestPropertySource(properties = {"s3.enabled=true", "s3.bucket=test-bucket", "s3.region=ap-south-1", "s3.prefix=dev/"})
class ReplayServiceTest extends AbstractMySqlTest {

    @TestConfiguration
    static class FakeStoreConfig {
        @Bean
        @Primary
        FakeRawEventStore fakeStore() {
            return new FakeRawEventStore();
        }
    }

    @Autowired private FakeRawEventStore store;
    @Autowired private ReplayService replay;
    @Autowired private S3Archiver archiver;
    @Autowired private IngestService ingestService;
    @Autowired private InboxConsumer consumer;
    @Autowired private QueryController queryController;
    @Autowired private JsonMapper jsonMapper;

    private String sample;

    @BeforeEach
    void setUp() throws Exception {
        store.reset();
        sample = Files.readString(Path.of("../simulator/samples/sample_events.json"));
    }

    /** Live traffic: ingest, process, archive to the (fake) S3 log. */
    private void live(String body) {
        ingestService.ingest(body, EventSource.LIVE);
        consumer.drainAll();
        archiver.archiveOnce();
    }

    private String status(String orderId) {
        return jdbc.queryForObject("SELECT status FROM order_projection WHERE order_id = ?", String.class, orderId);
    }

    private int count(String sql) {
        return jdbc.queryForObject(sql, Integer.class);
    }

    @Test
    void replayRebuildsTheSameStateWithoutNewCharges() throws Exception {
        live(sample);

        ReplayReport r = replay.replay(null);

        assertEquals("MATCH", r.reconciliation(), String.valueOf(r.differences()));
        assertEquals(8, r.eventsRead());
        assertEquals(6, r.applied());
        assertEquals(1, r.duplicates());
        assertEquals(1, r.deadLettered());
        assertEquals(2, r.ledgerBefore());
        assertEquals(2, r.ledgerAfter());
        assertEquals(0, r.newCharges(), "replay must not charge again");
        assertEquals(2, r.sideEffectsSkipped());
        assertEquals("SHIPPED", status("ORD-1"));
        assertEquals("SHIPPED", status("ORD-2"));

        // snapshot exported for Snowflake: one JSON line per order
        String[] lines = store.objects.get(r.snapshotKey()).strip().split("\n");
        assertEquals(2, lines.length);
        JsonNode first = jsonMapper.readTree(lines[0]);
        assertEquals("ORD-1", first.get("order_id").stringValue());
        assertEquals(r.startKey(), first.get("start_key").stringValue(), "Snowflake needs the replay window");
        assertTrue(r.snapshotKey().startsWith("dev/snapshots/"));

        Map<String, Object> run = jdbc.queryForMap("SELECT * FROM replay_runs WHERE id = ?", r.runId());
        assertEquals("COMPLETED", run.get("status"));
        assertEquals(8, ((Number) run.get("events_read")).intValue());
        assertEquals(2, ((Number) run.get("side_effects_skipped")).intValue());
    }

    @Test
    void replayRepairsADamagedProjection() {
        live(sample);
        jdbc.update("DELETE FROM order_projection WHERE order_id = 'ORD-1'");
        jdbc.update("UPDATE order_projection SET status = 'CREATED', amount_paid = 0 WHERE order_id = 'ORD-2'");

        ReplayReport r = replay.replay(null);

        assertEquals("MISMATCH", r.reconciliation(), "the damage is reported");
        assertTrue(r.differences().stream().anyMatch(d -> d.startsWith("ORD-1: missing before replay")));
        assertTrue(r.differences().stream().anyMatch(d -> d.startsWith("ORD-2:") && d.contains("status CREATED -> SHIPPED")));
        assertEquals("SHIPPED", status("ORD-1"), "and repaired");
        assertEquals("SHIPPED", status("ORD-2"));
        assertEquals(0, r.newCharges());
    }

    @Test
    void replayIsRefusedWhenTheSystemIsNotQuiet() {
        ingestService.ingest(sample, EventSource.LIVE);         // not processed, not archived

        ReplayRejectedException e = assertThrows(ReplayRejectedException.class, () -> replay.replay(null));

        assertTrue(e.getMessage().contains("PENDING"));
        assertTrue(e.getMessage().contains("not yet archived"));
        assertEquals(0, count("SELECT COUNT(*) FROM replay_runs"), "nothing was touched");
    }

    @Test
    void shuffledStreamWithRepeatsReplaysToTheSameState() throws Exception {
        List<String> events = new ArrayList<>();
        jsonMapper.readTree(sample).forEach(e -> events.add(e.toString()));
        events.add(events.get(0));
        events.add(events.get(6));
        Collections.shuffle(events, new Random(7));
        for (String e : events) {
            ingestService.ingest(e, EventSource.LIVE);
            consumer.runOnce();
        }
        consumer.drainAll();
        archiver.archiveOnce();

        ReplayReport r = replay.replay(null);

        assertEquals("MATCH", r.reconciliation(), String.valueOf(r.differences()));
        assertEquals(10, r.eventsRead());
        assertEquals(0, r.newCharges());
    }

    @Test
    void replayingTwiceGivesTheSameResultWithoutPilingUpRows() {
        live(sample);

        replay.replay(null);
        ReplayReport second = replay.replay(null);

        assertEquals("MATCH", second.reconciliation());
        assertEquals(8, count("SELECT COUNT(*) FROM inbox WHERE source = 'REPLAY'"), "previous replay rows replaced");
        assertEquals(1, count("SELECT COUNT(*) FROM dead_letter d JOIN inbox i ON i.id = d.inbox_id WHERE i.source = 'REPLAY'"));
        assertEquals(2, count("SELECT COUNT(*) FROM payment_ledger"));
    }

    @Test
    void liveMetricsAreNotInflatedAndReplayCopiesAreNotReArchived() {
        live(sample);
        Map<String, Object> before = queryController.metrics();
        int objectsBefore = store.objects.size();

        replay.replay(null);
        archiver.archiveOnce();
        Map<String, Object> after = queryController.metrics();

        for (String k : List.of("total_received", "applied", "duplicates", "dead_lettered", "total_charges")) {
            assertEquals(before.get(k), after.get(k), k);
        }
        assertEquals(8L, ((Number) after.get("replayed_messages")).longValue());
        assertEquals("COMPLETED", after.get("last_replay_status"));
        assertEquals(objectsBefore + 1, store.objects.size(), "only the snapshot was added - no raw or dlq copies");
    }

    @Test
    void defaultWindowIsTheCurrentDatasetAndFromWidensIt() {
        store.objects.put("dev/raw/2020/01/01/000000000-000000000001-OLD1.json", """
                {"event_id":"OLD1","entity_id":"ORD-OLD","version":1,"event_type":"OrderCreated",
                 "event_time":"2020-01-01T00:00:00Z","payload":{"customer_id":"C","amount":10,"currency":"INR"}}""");
        live(sample);

        ReplayReport current = replay.replay(null);
        assertEquals(8, current.eventsRead(), "older objects from earlier runs are ignored by default");

        ReplayReport all = replay.replay(Instant.parse("2020-01-01T00:00:00Z"));
        assertEquals(9, all.eventsRead());
        assertEquals("CREATED", status("ORD-OLD"));
        assertTrue(all.differences().stream().anyMatch(d -> d.startsWith("ORD-OLD: missing before replay")));
    }
}
