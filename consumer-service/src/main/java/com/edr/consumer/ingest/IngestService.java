package com.edr.consumer.ingest;

import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class IngestService {

    private final EnvelopeExtractor extractor;
    private final InboxRepository inboxRepository;
    private final int maxBatchSize;

    public IngestService(EnvelopeExtractor extractor,
                         InboxRepository inboxRepository,
                         @Value("${ingest.max-batch-size:1000}") int maxBatchSize) {
        this.extractor = extractor;
        this.inboxRepository = inboxRepository;
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Stores every message in the body in the inbox, in arrival order, with status PENDING.
     * All messages of one request are stored together or not at all.
     */
    @Transactional
    public IngestResponse ingest(String body, EventSource source) {
        if (body == null || body.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Request body is empty");
        }

        List<ParsedMessage> messages = extractor.split(body);
        if (messages.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No messages in request");
        }
        if (messages.size() > maxBatchSize) {
            throw new ResponseStatusException(HttpStatus.CONTENT_TOO_LARGE,
                    "Batch of " + messages.size() + " exceeds limit of " + maxBatchSize);
        }

        Instant receivedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        List<InboxEvent> rows = messages.stream()
                .map(m -> new InboxEvent(m.rawPayload(), m.eventId(), m.entityId(), m.version(),
                        m.eventType(), m.eventTime(), source, receivedAt))
                .toList();

        List<Long> ids = inboxRepository.saveAll(rows).stream().map(InboxEvent::getId).toList();

        // Stage 6: each raw message will also be written to S3 here.
        return new IngestResponse(ids.size(), ids);
    }
}
