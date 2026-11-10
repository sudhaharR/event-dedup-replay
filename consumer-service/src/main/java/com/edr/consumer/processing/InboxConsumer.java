package com.edr.consumer.processing;

import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.inbox.InboxStatus;
import com.edr.consumer.store.OrderProjection;
import com.edr.consumer.store.OrderProjectionRepository;
import com.edr.consumer.store.ParkedEvent;
import com.edr.consumer.store.ParkedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Reads the inbox in arrival order and hands each message to the EventProcessor.
 * Runs on a single thread (see ConsumerScheduler), so messages are handled one at a time.
 *
 * After an event is APPLIED, any parked successors of that order are released immediately.
 * A final sweep also releases parked events left over from a crash or a failed attempt.
 */
@Service
public class InboxConsumer {

    private static final Logger log = LoggerFactory.getLogger(InboxConsumer.class);
    private static final int MAX_DRAIN_ROUNDS = 50;

    private final InboxRepository inboxRepository;
    private final OrderProjectionRepository projectionRepository;
    private final ParkedEventRepository parkedRepository;
    private final EventProcessor processor;
    private final FailureRecorder failureRecorder;

    public InboxConsumer(InboxRepository inboxRepository,
                         OrderProjectionRepository projectionRepository,
                         ParkedEventRepository parkedRepository,
                         EventProcessor processor,
                         FailureRecorder failureRecorder) {
        this.inboxRepository = inboxRepository;
        this.projectionRepository = projectionRepository;
        this.parkedRepository = parkedRepository;
        this.processor = processor;
        this.failureRecorder = failureRecorder;
    }

    /** One polling cycle: up to 100 pending messages, then a park-buffer sweep. Returns messages handled. */
    public synchronized int runOnce() {
        int handled = 0;
        List<InboxEvent> pending = inboxRepository.findTop100ByStatusOrderByIdAsc(InboxStatus.PENDING);
        for (InboxEvent row : pending) {
            try {
                Outcome outcome = processor.process(row.getId());
                handled++;
                if (outcome.status() == InboxStatus.APPLIED) {
                    handled += drainEntity(outcome.entityId());
                }
            } catch (Exception e) {
                failureRecorder.recordFailure(row.getId(), e);
            }
        }
        handled += sweepParked();
        return handled;
    }

    /** Runs cycles until nothing is left to do. Used by tests and on demand. */
    public int drainAll() {
        int total = 0;
        for (int round = 0; round < MAX_DRAIN_ROUNDS; round++) {
            int handled = runOnce();
            total += handled;
            if (handled == 0 && inboxRepository.countByStatus(InboxStatus.PENDING) == 0) {
                break;
            }
        }
        return total;
    }

    /** Releases parked events of one order for as long as the next version is waiting. */
    private int drainEntity(String entityId) {
        int released = 0;
        while (true) {
            int current = projectionRepository.findById(entityId).map(OrderProjection::getLastVersion).orElse(0);
            Optional<ParkedEvent> next = parkedRepository.findByEntityIdAndVersion(entityId, current + 1);
            if (next.isEmpty() || !applyParked(next.get())) {
                return released;
            }
            released++;
        }
    }

    /** Releases every parked event that is now next in line (recovery after crashes or failures). */
    private int sweepParked() {
        int released = 0;
        Set<Long> failed = new HashSet<>();
        for (int round = 0; round < MAX_DRAIN_ROUNDS; round++) {
            List<Long> ready = parkedRepository.findReadyIds().stream().filter(id -> !failed.contains(id)).toList();
            if (ready.isEmpty()) {
                break;
            }
            for (Long id : ready) {
                Optional<ParkedEvent> parked = parkedRepository.findById(id);
                if (parked.isEmpty()) {
                    continue;
                }
                if (applyParked(parked.get())) {
                    released++;
                } else {
                    failed.add(id);
                }
            }
        }
        return released;
    }

    private boolean applyParked(ParkedEvent parked) {
        try {
            Outcome outcome = processor.applyParked(parked.getId());
            return outcome != null && outcome.status() == InboxStatus.APPLIED;
        } catch (Exception e) {
            log.warn("Releasing parked event {} failed", parked.getEventId());
            failureRecorder.recordFailure(parked.getInboxId(), e);
            return false;
        }
    }
}
