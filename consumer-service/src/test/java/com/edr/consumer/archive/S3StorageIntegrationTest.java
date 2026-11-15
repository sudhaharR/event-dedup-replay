package com.edr.consumer.archive;

import com.edr.consumer.AbstractMySqlTest;
import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.ingest.IngestService;
import com.edr.consumer.processing.InboxConsumer;
import com.edr.consumer.replay.ReplayReport;
import com.edr.consumer.replay.ReplayService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end through the real AWS SDK against S3-compatible storage (Adobe S3Mock in a container),
 * so the S3 client, credentials, keys and metadata are exercised without an AWS account.
 */
class S3StorageIntegrationTest extends AbstractMySqlTest {

    @SuppressWarnings("resource")
    static final GenericContainer<?> S3_MOCK = new GenericContainer<>("adobe/s3mock:5.2.3")
            .withExposedPorts(9090)
            .waitingFor(Wait.forListeningPort());

    static {
        S3_MOCK.start();
    }

    @DynamicPropertySource
    static void s3(DynamicPropertyRegistry r) {
        r.add("s3.enabled", () -> "true");
        r.add("s3.bucket", () -> "edr-test");
        r.add("s3.region", () -> "us-east-1");
        r.add("s3.prefix", () -> "it/");
        r.add("s3.endpoint", () -> "http://" + S3_MOCK.getHost() + ":" + S3_MOCK.getMappedPort(9090));
        r.add("s3.access-key-id", () -> "test-access-key");
        r.add("s3.secret-access-key", () -> "test-secret-key");
    }

    @Autowired private S3Client s3;
    @Autowired private S3Archiver archiver;
    @Autowired private IngestService ingestService;
    @Autowired private InboxRepository inboxRepository;
    @Autowired private InboxConsumer consumer;
    @Autowired private ReplayService replayService;

    @Test
    void rawEventsAreWrittenToTheBucketExactlyAsReceived() throws Exception {
        if (s3.listBuckets().buckets().stream().noneMatch(b -> b.name().equals("edr-test"))) {
            s3.createBucket(b -> b.bucket("edr-test"));
        }
        ingestService.ingest(Files.readString(Path.of("../simulator/samples/sample_events.json")), EventSource.LIVE);

        archiver.archiveOnce();

        List<S3Object> objects = s3.listObjectsV2(b -> b.bucket("edr-test").prefix("it/raw/")).contents();
        assertEquals(8, objects.size());

        InboxEvent first = inboxRepository.findAll().stream().min((a, b) -> Long.compare(a.getId(), b.getId())).orElseThrow();
        String body = s3.getObjectAsBytes(b -> b.bucket("edr-test").key(first.getS3Key())).asUtf8String();
        assertEquals(first.getRawPayload(), body);

        HeadObjectResponse head = s3.headObject(b -> b.bucket("edr-test").key(first.getS3Key()));
        assertEquals(String.valueOf(first.getId()), head.metadata().get("inbox-id"));
        assertEquals("E101", head.metadata().get("event-id"));
        assertEquals("application/json", head.contentType());

        // full replay through the real SDK: read the log back, rebuild, reconcile, export the snapshot
        consumer.drainAll();
        archiver.archiveOnce();
        ReplayReport report = replayService.replay(null);
        assertEquals("MATCH", report.reconciliation(), String.valueOf(report.differences()));
        assertEquals(8, report.eventsRead());
        assertEquals(0, report.newCharges());
        String snapshot = s3.getObjectAsBytes(b -> b.bucket("edr-test").key(report.snapshotKey())).asUtf8String();
        assertEquals(2, snapshot.strip().lines().count());
    }
}
