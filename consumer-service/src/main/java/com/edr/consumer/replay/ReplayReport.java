package com.edr.consumer.replay;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Outcome of one replay run.
 *
 * reconciliation = MATCH when the rebuilt projection is identical to the projection before the replay;
 * otherwise MISMATCH with the differences (expected if the projection had been damaged - replay repairs it).
 */
public record ReplayReport(
        long runId,
        String startKey,
        int eventsRead,
        int applied,
        int duplicates,
        int stale,
        int parked,
        int deadLettered,
        int ordersBefore,
        int ordersAfter,
        int ledgerBefore,
        int ledgerAfter,
        int sideEffectsSkipped,
        String reconciliation,
        List<String> differences,
        String snapshotKey,
        long durationMs) {

    public int newCharges() {
        return ledgerAfter - ledgerBefore;
    }

    /** JSON body for POST /replay (snake_case, like the rest of the API). */
    public Map<String, Object> toResponse() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("run_id", runId);
        m.put("status", "COMPLETED");
        m.put("start_key", startKey);
        m.put("events_read", eventsRead);
        m.put("applied", applied);
        m.put("duplicates", duplicates);
        m.put("stale", stale);
        m.put("parked", parked);
        m.put("dead_lettered", deadLettered);
        m.put("orders_before", ordersBefore);
        m.put("orders_after", ordersAfter);
        m.put("ledger_before", ledgerBefore);
        m.put("ledger_after", ledgerAfter);
        m.put("new_charges", newCharges());
        m.put("side_effects_skipped", sideEffectsSkipped);
        m.put("reconciliation", reconciliation);
        m.put("differences", differences);
        m.put("snapshot_s3_key", snapshotKey);
        m.put("duration_ms", durationMs);
        return m;
    }
}
