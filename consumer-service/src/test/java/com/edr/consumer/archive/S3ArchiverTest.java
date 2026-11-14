package com.edr.consumer.archive;

import com.edr.consumer.AbstractMySqlTest;
import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.inbox.InboxStatus;
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
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The S3 archiver against an in-memory store that can be switched "down".
 * Proves: every message is archived in arrival order, an S3 outage never blocks ingestion and is
 * recovered from, dead letters are archived with their reason, and the consumer never erases s3_key.
 */
@TestPropertySource(properties = {"s3.enabled=true", "s3.bucket=test-bucket", "s3.region=ap-south-1", "s3.prefix=dev/"})
class S3ArchiverTest extends AbstractMySqlTest {

    static class FakeStore implements RawEventStore {
        final Map<String, String> objects = new ConcurrentSkipListMap<>();   // sorted by key
        volatile boolean down;

        @Override
        public void put(String key, String body, Map<String, String> metadata) {
            if (down) {
                throw new IllegalStateException("S3 unreachable (simulated outage)");
            }
            objects.put(key, body);
        }

        @Override
        public String location() {
            return "fake://test-bucket";
        }
    }

    @TestConfiguration
    static class FakeStoreConfig {
        @Bean
        @Primary
        FakeStore fakeStore() {
            return new FakeStore();
        }
    }

    @Autowired private FakeStore store;
    @Autowired private S3Archiver archiver;
    @Autowired private IngestService ingestService;
    @Autowired private InboxConsumer consumer;
    @Autowired private InboxRepository inboxRepository;
    @Autowired private QueryController queryController;
    @Autowired private TransactionTemplate tx;
    @Autowired private JsonMapper jsonMapper;

    private String sample;

    @BeforeEach
    void setUp() throws Exception {
        store.objects.clear();
        store.down = false;
        sample = Files.readString(Path.of("../simulator/samples/sample_events.json"));
    }

    private List<String> keys(String area) {
        return store.objects.keySet().stream().filter(k -> k.startsWith("dev/" + area + "/")).toList();
    }

    @Test
    void everyMessageIsArchivedInArrivalOrderIncludingDuplicatesAndPoison() {
        ingestService.ingest(sample, EventSource.LIVE);
        ingestService.ingest("{\"event_id\": \"E400\", ", EventSource.LIVE);

        archiver.archiveOnce();

        List<InboxEvent> rows = inboxRepository.findAll().stream()
                .sorted((a, b) -> Long.compare(a.getId(), b.getId())).toList();
        List<String> raw = keys("raw");
        assertEquals(9, raw.size(), "8 sample messages (incl. duplicate E102) + broken JSON");
        for (int i = 0; i < rows.size(); i++) {
            InboxEvent row = rows.get(i);
            assertEquals(raw.get(i), row.getS3Key(), "key order = arrival order");
            assertEquals(row.getRawPayload(), store.objects.get(row.getS3Key()), "stored exactly as received");
        }
        assertTrue(raw.get(8).endsWith("-unparsed.json"), "broken JSON has no event_id");
        assertEquals(0, inboxRepository.countByS3KeyIsNull());
    }

    @Test
    void s3OutageDoesNotBlockIngestionAndIsRecovered() {
        store.down = true;
        assertEquals(8, ingestService.ingest(sample, EventSource.LIVE).accepted(), "ingestion still works");

        archiver.archiveOnce();

        Map<String, Object> metrics = queryController.metrics();
        assertEquals(8L, metrics.get("s3_archive_backlog"));
        assertTrue(String.valueOf(metrics.get("s3_last_error")).contains("simulated outage"));
        assertEquals(0, store.objects.size());

        store.down = false;            // S3 is back
        archiver.archiveOnce();

        assertEquals(8, keys("raw").size());
        assertEquals(0L, queryController.metrics().get("s3_archive_backlog"));
    }

    @Test
    void deadLettersAreArchivedWithTheirReason() throws Exception {
        ingestService.ingest(sample, EventSource.LIVE);
        consumer.drainAll();

        archiver.archiveOnce();

        List<String> dlq = keys("dlq");
        assertEquals(1, dlq.size());
        JsonNode envelope = jsonMapper.readTree(store.objects.get(dlq.get(0)));
        assertEquals("E300", envelope.get("event_id").stringValue());
        assertEquals("MISSING_FIELD", envelope.get("error_code").stringValue());
        assertTrue(envelope.get("raw_payload").stringValue().contains("\"amount\":\"abc\""));
        assertEquals(0L, queryController.metrics().get("s3_dlq_backlog"));
    }

    @Test
    void consumerNeverErasesAnS3KeyWrittenWhileItWasProcessing() {
        long id = ingestService.ingest(sample, EventSource.LIVE).inboxIds().get(0);

        // The consumer loads the row (s3_key still NULL) ... the archiver records the key meanwhile
        // on another connection ... then the consumer commits its own change to the same row.
        tx.executeWithoutResult(status -> {
            InboxEvent row = inboxRepository.findById(id).orElseThrow();
            CompletableFuture.runAsync(() ->
                    jdbc.update("UPDATE inbox SET s3_key = 'dev/raw/x.json' WHERE id = ?", id)).join();
            row.setStatus(InboxStatus.APPLIED);
        });

        assertEquals("dev/raw/x.json",
                jdbc.queryForObject("SELECT s3_key FROM inbox WHERE id = ?", String.class, id));
        assertEquals("APPLIED", jdbc.queryForObject("SELECT status FROM inbox WHERE id = ?", String.class, id));
    }
}
