package com.edr.consumer.archive;

import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.store.DeadLetter;
import com.edr.consumer.store.DeadLetterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Transactional-outbox archiver: copies every inbox message (and every dead letter) to S3.
 *
 * The Ingest API never calls S3 itself - it only writes the inbox row. This archiver uploads
 * rows whose s3_key is still NULL, then records the key. So:
 *  - an S3 outage never blocks or slows ingestion,
 *  - no message is lost: it stays in the backlog until the upload succeeds,
 *  - the backlog size and last error are visible in GET /metrics.
 * Uploading the same key twice (e.g. a crash between upload and recording the key) is harmless.
 */
@Service
@ConditionalOnProperty(name = "s3.enabled", havingValue = "true")
public class S3Archiver {

    private static final Logger log = LoggerFactory.getLogger(S3Archiver.class);

    private final InboxRepository inboxRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final RawEventStore store;
    private final ArchiveStatus status;
    private final JdbcTemplate jdbc;
    private final JsonMapper jsonMapper;
    private final String prefix;

    public S3Archiver(InboxRepository inboxRepository, DeadLetterRepository deadLetterRepository,
                      RawEventStore store, ArchiveStatus status, JdbcTemplate jdbc,
                      JsonMapper jsonMapper, S3Properties props) {
        this.inboxRepository = inboxRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.store = store;
        this.status = status;
        this.jdbc = jdbc;
        this.jsonMapper = jsonMapper;
        this.prefix = props.normalizedPrefix();
    }

    /** One archiving pass. Returns the number of objects uploaded. */
    public synchronized int archiveOnce() {
        int raw = archiveInbox();
        int dlq = archiveDeadLetters();
        if (raw + dlq > 0) {
            log.info("Archived to {}: {} raw message(s), {} dead letter(s)", store.location(), raw, dlq);
        }
        return raw + dlq;
    }

    private int archiveInbox() {
        List<InboxEvent> pending = inboxRepository.findTop200ByS3KeyIsNullOrderByIdAsc();
        int done = 0;
        for (InboxEvent row : pending) {
            String key = S3Keys.raw(prefix, row.getId(), row.getEventId(), row.getReceivedAt());
            try {
                store.put(key, row.getRawPayload(), metadata(row.getId(), row.getEventId(), row.getReceivedAt()));
            } catch (Exception e) {
                return failed(done, "raw message " + row.getId(), e);
            }
            jdbc.update("UPDATE inbox SET s3_key = ? WHERE id = ? AND s3_key IS NULL", key, row.getId());
            done++;
        }
        if (done > 0) {
            status.success(done);
        }
        return done;
    }

    private int archiveDeadLetters() {
        List<DeadLetter> pending = deadLetterRepository.findArchivePending();
        int done = 0;
        for (DeadLetter d : pending) {
            String key = S3Keys.deadLetter(prefix, d.getInboxId(), d.getEventId(), d.getReceivedAt());
            try {
                store.put(key, envelope(d), metadata(d.getInboxId(), d.getEventId(), d.getReceivedAt()));
            } catch (Exception e) {
                return failed(done, "dead letter " + d.getId(), e);
            }
            jdbc.update("UPDATE dead_letter SET s3_key = ? WHERE id = ? AND s3_key IS NULL", key, d.getId());
            done++;
        }
        if (done > 0) {
            status.success(done);
        }
        return done;
    }

    /** S3 is probably unreachable: stop this pass, keep the rest in the backlog, retry next pass. */
    private int failed(int doneSoFar, String what, Exception e) {
        status.failure(e);
        log.warn("S3 upload failed for {} - will retry on the next pass: {}", what, e.getMessage());
        if (doneSoFar > 0) {
            status.success(doneSoFar);
        }
        return doneSoFar;
    }

    /** Dead letters are archived with the reason, so the archive is self-explanatory. */
    private String envelope(DeadLetter d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("inbox_id", d.getInboxId());
        m.put("event_id", d.getEventId());
        m.put("entity_id", d.getEntityId());
        m.put("error_code", d.getErrorCode());
        m.put("error_message", d.getErrorMessage());
        m.put("received_at", String.valueOf(d.getReceivedAt()));
        m.put("dead_lettered_at", String.valueOf(d.getDeadLetteredAt()));
        m.put("raw_payload", d.getRawPayload());
        return jsonMapper.writeValueAsString(m);
    }

    private static Map<String, String> metadata(Long inboxId, String eventId, java.time.Instant receivedAt) {
        return Map.of(
                "inbox-id", String.valueOf(inboxId),
                "event-id", S3Keys.safe(eventId),
                "received-at", String.valueOf(receivedAt));
    }
}
