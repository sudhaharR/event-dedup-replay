package com.edr.consumer.processing;

import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.inbox.InboxStatus;
import com.edr.consumer.store.DeadLetter;
import com.edr.consumer.store.DeadLetterRepository;
import com.edr.consumer.store.LedgerEntry;
import com.edr.consumer.store.LedgerRepository;
import com.edr.consumer.store.OrderProjection;
import com.edr.consumer.store.OrderProjectionRepository;
import com.edr.consumer.store.OrderStatus;
import com.edr.consumer.store.ParkedEvent;
import com.edr.consumer.store.ParkedEventRepository;
import com.edr.consumer.store.ProcessedEvent;
import com.edr.consumer.store.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static com.edr.consumer.inbox.InboxStatus.APPLIED;
import static com.edr.consumer.inbox.InboxStatus.DEAD_LETTERED;
import static com.edr.consumer.inbox.InboxStatus.DUPLICATE;
import static com.edr.consumer.inbox.InboxStatus.PARKED;
import static com.edr.consumer.inbox.InboxStatus.PENDING;
import static com.edr.consumer.inbox.InboxStatus.STALE;

/**
 * The consumer pipeline for ONE message. Each public method is one database transaction:
 * either every change it makes is committed, or none is (no partial side effects).
 *
 *   1. Parse + structural validation      -> fail: DEAD_LETTERED
 *   2. Idempotency check on event_id      -> seen: DUPLICATE (or EVENT_ID_CONFLICT if content differs)
 *   3. Ordering check against last_version
 *        version <= current               -> STALE
 *        version >  current + 1           -> PARKED (waits for the missing version)
 *        version == current + 1           -> continue
 *   4. Business validation                -> fail: DEAD_LETTERED
 *   5. Atomic apply: projection + ledger + processed_events + inbox status
 */
@Service
public class EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);
    private static final int MAX_MESSAGE = 500;

    private final InboxRepository inboxRepository;
    private final OrderProjectionRepository projectionRepository;
    private final ProcessedEventRepository processedRepository;
    private final ParkedEventRepository parkedRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final LedgerRepository ledgerRepository;
    private final EventValidator validator;
    private final BusinessRules businessRules;
    private final FaultInjector faultInjector;

    public EventProcessor(InboxRepository inboxRepository,
                          OrderProjectionRepository projectionRepository,
                          ProcessedEventRepository processedRepository,
                          ParkedEventRepository parkedRepository,
                          DeadLetterRepository deadLetterRepository,
                          LedgerRepository ledgerRepository,
                          EventValidator validator,
                          BusinessRules businessRules,
                          FaultInjector faultInjector) {
        this.inboxRepository = inboxRepository;
        this.projectionRepository = projectionRepository;
        this.processedRepository = processedRepository;
        this.parkedRepository = parkedRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.ledgerRepository = ledgerRepository;
        this.validator = validator;
        this.businessRules = businessRules;
        this.faultInjector = faultInjector;
    }

    /** Handles one PENDING inbox message. */
    @Transactional
    public Outcome process(long inboxId) {
        InboxEvent inbox = inboxRepository.findById(inboxId).orElseThrow();
        if (inbox.getStatus() != PENDING) {
            return new Outcome(inbox.getStatus(), inbox.getEntityId());
        }

        // 1. Structural validation
        ValidatedEvent event;
        try {
            event = validator.validate(inbox.getRawPayload());
        } catch (RejectedEventException e) {
            return deadLetter(inbox, inbox.getEventId(), inbox.getEntityId(), e);
        }

        // 2. Idempotency
        Optional<ProcessedEvent> processed = processedRepository.findById(event.eventId());
        if (processed.isPresent()) {
            if (!processed.get().getPayloadHash().equals(event.payloadHash())) {
                return deadLetter(inbox, event.eventId(), event.entityId(), new RejectedEventException(
                        ErrorCode.EVENT_ID_CONFLICT, "event_id " + event.eventId()
                        + " was already processed with different content"));
            }
            return finish(inbox, event.entityId(), DUPLICATE, "event_id already processed");
        }
        if (parkedRepository.existsByEventId(event.eventId())) {
            return finish(inbox, event.entityId(), DUPLICATE, "event_id already waiting in park buffer");
        }

        // 3. Ordering (per entity)
        OrderProjection order = projectionRepository.findById(event.entityId()).orElse(null);
        int current = order == null ? 0 : order.getLastVersion();

        if (event.version() <= current) {
            return finish(inbox, event.entityId(), STALE,
                    "version " + event.version() + " <= current version " + current);
        }
        if (event.version() > current + 1) {
            if (parkedRepository.existsByEntityIdAndVersion(event.entityId(), event.version())) {
                return finish(inbox, event.entityId(), DUPLICATE,
                        "version " + event.version() + " already waiting in park buffer");
            }
            return park(inbox, event, current);
        }

        // 4 + 5. Business validation and atomic apply
        return applyOrReject(inbox, event, order);
    }

    /**
     * Applies a parked event if it is now next in line for its order.
     * Returns null if it is not ready (or no longer exists).
     */
    @Transactional
    public Outcome applyParked(long parkedId) {
        ParkedEvent parked = parkedRepository.findById(parkedId).orElse(null);
        if (parked == null) {
            return null;
        }
        OrderProjection order = projectionRepository.findById(parked.getEntityId()).orElse(null);
        int current = order == null ? 0 : order.getLastVersion();
        if (parked.getVersion() > current + 1) {
            return null;                                   // still waiting
        }

        InboxEvent inbox = inboxRepository.findById(parked.getInboxId()).orElseThrow();
        parkedRepository.delete(parked);

        if (parked.getVersion() <= current) {             // another event with this version won
            return finish(inbox, parked.getEntityId(), STALE,
                    "version " + parked.getVersion() + " <= current version " + current);
        }
        ValidatedEvent event = validator.validate(parked.getRawPayload());   // was valid when parked
        log.info("[inbox {}] {} {} v{} released from park buffer", inbox.getId(),
                event.eventId(), event.entityId(), event.version());
        return applyOrReject(inbox, event, order);
    }

    // ------------------------------------------------------------------

    private Outcome park(InboxEvent inbox, ValidatedEvent event, int current) {
        parkedRepository.save(new ParkedEvent(inbox.getId(), event.eventId(), event.entityId(), event.version(),
                event.type().wireName(), inbox.getRawPayload(), current + 1, now()));
        inbox.setWasOutOfOrder(true);
        inbox.setStatus(PARKED);
        inbox.setLastError("waiting for version " + (current + 1));
        log.info("[inbox {}] {} {} v{} -> PARKED (waiting for v{})", inbox.getId(),
                event.eventId(), event.entityId(), event.version(), current + 1);
        return new Outcome(PARKED, event.entityId());
    }

    private Outcome applyOrReject(InboxEvent inbox, ValidatedEvent event, OrderProjection order) {
        try {
            businessRules.check(order, event);
        } catch (RejectedEventException e) {
            return deadLetter(inbox, event.eventId(), event.entityId(), e);
        }
        apply(event, order);
        return finish(inbox, event.entityId(), APPLIED, null);
    }

    /** State change + side effect + idempotency record - all in the caller's transaction. */
    private void apply(ValidatedEvent event, OrderProjection order) {
        switch (event.type()) {
            case ORDER_CREATED -> order = new OrderProjection(event.entityId(), event.text("customer_id"),
                    event.amount(), event.text("currency"));
            case PAYMENT_CAPTURED -> {
                order.setStatus(OrderStatus.PAID);
                order.setAmountPaid(event.amount());
                chargeOnce(event);
            }
            case ORDER_SHIPPED -> {
                order.setStatus(OrderStatus.SHIPPED);
                order.setTrackingId(event.text("tracking_id"));
            }
            case ORDER_CANCELLED -> {
                order.setStatus(OrderStatus.CANCELLED);
                order.setCancelReason(event.text("reason"));
            }
        }
        order.advanceTo(event.version(), event.eventId(), event.eventTime());
        projectionRepository.save(order);

        faultInjector.afterSideEffect(event.eventId());   // demo crash point (normally does nothing)

        processedRepository.save(new ProcessedEvent(event.eventId(), event.entityId(), event.version(),
                event.type().wireName(), event.payloadHash(), event.eventTime(), now()));
    }

    /** The side effect. The ledger is keyed by event_id, so a charge can never happen twice. */
    private void chargeOnce(ValidatedEvent event) {
        if (ledgerRepository.existsByEventId(event.eventId())) {
            log.info("Charge for {} already in ledger - side effect skipped", event.eventId());
            return;
        }
        ledgerRepository.save(new LedgerEntry(event.eventId(), event.entityId(), event.text("payment_id"),
                event.amount(), event.text("currency"), now()));
    }

    private Outcome deadLetter(InboxEvent inbox, String eventId, String entityId, RejectedEventException e) {
        String message = trim(e.getMessage());
        deadLetterRepository.save(new DeadLetter(inbox.getId(), eventId, entityId, inbox.getRawPayload(),
                e.getCode().name(), message, inbox.getReceivedAt()));
        log.warn("[inbox {}] {} -> DEAD_LETTERED {}: {}", inbox.getId(), eventId, e.getCode(), message);
        return finish(inbox, entityId, DEAD_LETTERED, e.getCode() + ": " + message);
    }

    private Outcome finish(InboxEvent inbox, String entityId, InboxStatus status, String reason) {
        inbox.setStatus(status);
        inbox.setProcessedAt(now());
        inbox.setLastError(reason == null ? null : trim(reason));
        if (status != DEAD_LETTERED) {
            log.info("[inbox {}] {} {} v{} -> {}{}", inbox.getId(), inbox.getEventId(), entityId,
                    inbox.getVersion(), status, reason == null ? "" : " (" + reason + ")");
        }
        return new Outcome(status, entityId);
    }

    private static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static String trim(String s) {
        return s.length() > MAX_MESSAGE ? s.substring(0, MAX_MESSAGE) : s;
    }
}
