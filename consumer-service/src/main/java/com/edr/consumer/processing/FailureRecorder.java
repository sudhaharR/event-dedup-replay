package com.edr.consumer.processing;

import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.inbox.InboxStatus;
import com.edr.consumer.store.DeadLetter;
import com.edr.consumer.store.DeadLetterRepository;
import com.edr.consumer.store.ParkedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Records an unexpected processing failure, AFTER the failed transaction has been rolled back.
 * The message stays where it was (PENDING or PARKED) and is retried on the next poll;
 * after max-attempts it is dead-lettered as PROCESSING_ERROR so it cannot block the consumer forever.
 */
@Service
public class FailureRecorder {

    private static final Logger log = LoggerFactory.getLogger(FailureRecorder.class);

    private final InboxRepository inboxRepository;
    private final ParkedEventRepository parkedRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final int maxAttempts;

    public FailureRecorder(InboxRepository inboxRepository,
                           ParkedEventRepository parkedRepository,
                           DeadLetterRepository deadLetterRepository,
                           @Value("${consumer.max-attempts:3}") int maxAttempts) {
        this.inboxRepository = inboxRepository;
        this.parkedRepository = parkedRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.maxAttempts = maxAttempts;
    }

    @Transactional
    public void recordFailure(long inboxId, Exception error) {
        InboxEvent inbox = inboxRepository.findById(inboxId).orElseThrow();
        int attempts = inbox.getAttempts() + 1;
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();
        if (message.length() > 450) {
            message = message.substring(0, 450);
        }
        inbox.setAttempts(attempts);
        inbox.setLastError("attempt " + attempts + " failed - " + message);
        log.warn("[inbox {}] attempt {}/{} failed, transaction rolled back: {}", inboxId, attempts, maxAttempts, message);

        if (attempts >= maxAttempts) {
            parkedRepository.findByInboxId(inboxId).ifPresent(parkedRepository::delete);
            deadLetterRepository.save(new DeadLetter(inboxId, inbox.getEventId(), inbox.getEntityId(),
                    inbox.getRawPayload(), ErrorCode.PROCESSING_ERROR.name(),
                    "Failed after " + attempts + " attempts - " + message, inbox.getReceivedAt()));
            inbox.setStatus(InboxStatus.DEAD_LETTERED);
            inbox.setProcessedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
            log.warn("[inbox {}] -> DEAD_LETTERED PROCESSING_ERROR after {} attempts", inboxId, attempts);
        }
    }
}
