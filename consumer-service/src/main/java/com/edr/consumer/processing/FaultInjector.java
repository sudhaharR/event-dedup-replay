package com.edr.consumer.processing;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simulates a crash in the middle of processing, for the "partial side effect" demo.
 *
 * For each configured event_id, the FIRST time it is applied the consumer throws right after
 * the projection update and the ledger insert, before the transaction commits.
 * Expected result: everything is rolled back (no charge, no state change), and the retry succeeds.
 *
 * Configure with: consumer.fault-injection.fail-once-event-ids=E102,E202
 */
@Component
public class FaultInjector {

    private final Set<String> failOnce = ConcurrentHashMap.newKeySet();

    public FaultInjector(@Value("${consumer.fault-injection.fail-once-event-ids:}") List<String> eventIds) {
        eventIds.stream().map(String::trim).filter(s -> !s.isEmpty()).forEach(failOnce::add);
    }

    public void failOnceFor(String eventId) {
        failOnce.add(eventId);
    }

    void afterSideEffect(String eventId) {
        if (failOnce.remove(eventId)) {
            throw new SimulatedCrashException("Simulated crash after side effect for " + eventId);
        }
    }

    public static class SimulatedCrashException extends RuntimeException {
        public SimulatedCrashException(String message) {
            super(message);
        }
    }
}
