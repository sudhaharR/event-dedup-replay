package com.edr.consumer.replay;

import com.edr.consumer.archive.RawEventStore;
import com.edr.consumer.archive.S3Keys;
import com.edr.consumer.archive.S3Properties;
import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.ingest.EnvelopeExtractor;
import com.edr.consumer.ingest.ParsedMessage;
import com.edr.consumer.processing.InboxConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Rebuilds the order projection from the S3 raw event log, without repeating side effects.
 *
 *  1. Check the system is quiet: nothing PENDING, nothing PARKED, archive backlog empty
 *     (so S3 holds every message and nothing is in flight).
 *  2. Snapshot the current projection and the ledger.
 *  3. Reset the DERIVED state only: order_projection, processed_events, park_buffer, and the rows of
 *     earlier replays. The payment_ledger (real side effects) and the LIVE inbox history are kept.
 *  4. Read the raw objects from S3 in key order (= arrival order) and add them to the inbox as REPLAY.
 *  5. Run the normal consumer pipeline over them. Payments find their charge already in the ledger,
 *     so the side effect is skipped.
 *  6. Compare the rebuilt projection with the snapshot (reconciliation), export a snapshot to S3,
 *     and record the run in replay_runs.
 */
@Service
@ConditionalOnProperty(name = "s3.enabled", havingValue = "true")
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);
    private static final int INSERT_BATCH = 500;
    private static final List<String> COMPARED_COLUMNS = List.of("status", "customer_id", "order_amount",
            "amount_paid", "currency", "tracking_id", "cancel_reason", "last_version", "last_event_id");

    private final JdbcTemplate jdbc;
    private final RawEventStore store;
    private final EnvelopeExtractor extractor;
    private final InboxRepository inboxRepository;
    private final InboxConsumer consumer;
    private final TransactionTemplate tx;
    private final JsonMapper jsonMapper;
    private final String prefix;
    private final ReentrantLock lock = new ReentrantLock();

    public ReplayService(JdbcTemplate jdbc, RawEventStore store, EnvelopeExtractor extractor,
                         InboxRepository inboxRepository, InboxConsumer consumer, TransactionTemplate tx,
                         JsonMapper jsonMapper, S3Properties props) {
        this.jdbc = jdbc;
        this.store = store;
        this.extractor = extractor;
        this.inboxRepository = inboxRepository;
        this.consumer = consumer;
        this.tx = tx;
        this.jsonMapper = jsonMapper;
        this.prefix = props.normalizedPrefix();
    }

    /**
     * @param from replay only raw objects received at or after this time; null = everything since the
     *             first LIVE message currently in the inbox (or the whole log if the inbox is empty)
     */
    public ReplayReport replay(Instant from) {
        if (!lock.tryLock()) {
            throw new ReplayRejectedException("A replay is already running");
        }
        try {
            return run(from);
        } finally {
            lock.unlock();
        }
    }

    private ReplayReport run(Instant from) {
        checkSystemIsQuiet();
        long started = System.currentTimeMillis();
        long runId = startRun();
        try {
            List<String> keys = keysToReplay(from);
            String startKey = keys.isEmpty() ? null : keys.get(0);
            log.info("REPLAY {}: {} raw object(s) from {} starting at {}", runId, keys.size(), store.location(), startKey);

            Map<String, Map<String, String>> before = projection();
            Set<String> chargesBefore = new HashSet<>(jdbc.queryForList("SELECT event_id FROM payment_ledger", String.class));

            synchronized (consumer) {          // live consumption pauses while the projection is rebuilt
                tx.executeWithoutResult(s -> resetDerivedState());
                insertReplayMessages(keys);
                consumer.drainAll(keys.size() / 50 + 50);
            }

            Map<String, Map<String, String>> after = projection();
            List<String> differences = compare(before, after);
            int ledgerAfter = count("SELECT COUNT(*) FROM payment_ledger");
            int skipped = sideEffectsSkipped(chargesBefore);
            String snapshotKey = exportSnapshot(runId, startKey, after);

            ReplayReport report = new ReplayReport(runId, startKey, keys.size(),
                    replayCount("APPLIED"), replayCount("DUPLICATE"), replayCount("STALE"),
                    replayCount("PARKED"), replayCount("DEAD_LETTERED"),
                    before.size(), after.size(), chargesBefore.size(), ledgerAfter, skipped,
                    differences.isEmpty() ? "MATCH" : "MISMATCH",
                    differences.size() > 50 ? differences.subList(0, 50) : differences,
                    snapshotKey, System.currentTimeMillis() - started);
            finishRun(report);
            log.info("REPLAY {} done: read={} applied={} new_charges={} side_effects_skipped={} reconciliation={}",
                    runId, report.eventsRead(), report.applied(), report.newCharges(), skipped, report.reconciliation());
            return report;
        } catch (RuntimeException e) {
            failRun(runId, e);
            throw e;
        }
    }

    // ---------------------------------------------------------------- steps

    private void checkSystemIsQuiet() {
        int pending = count("SELECT COUNT(*) FROM inbox WHERE status = 'PENDING'");
        int parked = count("SELECT COUNT(*) FROM park_buffer");
        int backlog = count("SELECT COUNT(*) FROM inbox WHERE s3_key IS NULL");
        List<String> problems = new ArrayList<>();
        if (pending > 0) {
            problems.add(pending + " message(s) still PENDING - wait for the consumer to finish");
        }
        if (parked > 0) {
            problems.add(parked + " event(s) PARKED waiting for a missing version");
        }
        if (backlog > 0) {
            problems.add(backlog + " message(s) not yet archived to S3 - wait for the archiver (or fix S3)");
        }
        if (!problems.isEmpty()) {
            throw new ReplayRejectedException("Replay needs a quiet system: " + String.join("; ", problems));
        }
    }

    /** Raw keys in arrival order, limited to the requested window. */
    private List<String> keysToReplay(Instant from) {
        List<String> all = store.listKeys(prefix + "raw/");
        if (from != null) {
            return all.stream().filter(k -> S3Keys.receivedAt(k).map(t -> !t.isBefore(from)).orElse(false)).toList();
        }
        // default window: from the first LIVE message currently in the inbox (keys sort in arrival order)
        String firstKey = inboxRepository.findFirstBySourceOrderByIdAsc(EventSource.LIVE)
                .map(InboxEvent::getS3Key).orElse(null);
        return firstKey == null ? all : all.stream().filter(k -> k.compareTo(firstKey) >= 0).toList();
    }

    private void resetDerivedState() {
        jdbc.update("DELETE FROM order_projection");
        jdbc.update("DELETE FROM processed_events");
        jdbc.update("DELETE FROM park_buffer");
        jdbc.update("DELETE d FROM dead_letter d JOIN inbox i ON i.id = d.inbox_id WHERE i.source = 'REPLAY'");
        jdbc.update("DELETE FROM inbox WHERE source = 'REPLAY'");
        // payment_ledger is NOT touched: it records real-world side effects
    }

    private void insertReplayMessages(List<String> keys) {
        List<InboxEvent> batch = new ArrayList<>();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        for (String key : keys) {
            String body = store.get(key);
            List<ParsedMessage> messages = extractor.split(body);
            for (ParsedMessage m : messages) {
                String raw = messages.size() == 1 ? body : m.rawPayload();   // keep the exact bytes from S3
                InboxEvent row = new InboxEvent(raw, m.eventId(), m.entityId(), m.version(), m.eventType(),
                        m.eventTime(), EventSource.REPLAY, now);
                row.setS3Key(key);             // already in S3 - the archiver must not upload it again
                batch.add(row);
            }
            if (batch.size() >= INSERT_BATCH) {
                saveBatch(batch);
            }
        }
        saveBatch(batch);
    }

    private void saveBatch(List<InboxEvent> batch) {
        if (!batch.isEmpty()) {
            List<InboxEvent> copy = List.copyOf(batch);
            tx.executeWithoutResult(s -> inboxRepository.saveAll(copy));
            batch.clear();
        }
    }

    /** Payments applied during the replay whose charge was already in the ledger before it started. */
    private int sideEffectsSkipped(Set<String> chargesBefore) {
        List<String> replayedPayments = jdbc.queryForList("""
                SELECT event_id FROM inbox
                WHERE source = 'REPLAY' AND status = 'APPLIED' AND event_type = 'PaymentCaptured'""", String.class);
        return (int) replayedPayments.stream().filter(chargesBefore::contains).count();
    }

    // ---------------------------------------------------------------- reconciliation

    /** order_id -> comparable column values (amounts normalised so 500.0 equals 500.00). */
    private Map<String, Map<String, String>> projection() {
        Map<String, Map<String, String>> result = new TreeMap<>();
        for (Map<String, Object> row : jdbc.queryForList("SELECT * FROM order_projection")) {
            Map<String, String> values = new LinkedHashMap<>();
            for (String col : COMPARED_COLUMNS) {
                Object v = row.get(col);
                values.put(col, v instanceof BigDecimal d ? d.stripTrailingZeros().toPlainString() : String.valueOf(v));
            }
            result.put(String.valueOf(row.get("order_id")), values);
        }
        return result;
    }

    private static List<String> compare(Map<String, Map<String, String>> before, Map<String, Map<String, String>> after) {
        List<String> differences = new ArrayList<>();
        Set<String> ids = new TreeSet<>(before.keySet());
        ids.addAll(after.keySet());
        for (String id : ids) {
            Map<String, String> b = before.get(id);
            Map<String, String> a = after.get(id);
            if (b == null) {
                differences.add(id + ": missing before replay, rebuilt as " + a.get("status"));
            } else if (a == null) {
                differences.add(id + ": existed before replay (" + b.get("status") + ") but has no events in the log");
            } else {
                String changed = COMPARED_COLUMNS.stream().filter(c -> !b.get(c).equals(a.get(c)))
                        .map(c -> c + " " + b.get(c) + " -> " + a.get(c)).collect(Collectors.joining(", "));
                if (!changed.isEmpty()) {
                    differences.add(id + ": " + changed);
                }
            }
        }
        return differences;
    }

    /**
     * Rebuilt projection as JSON Lines in S3 (one order per line). Snowflake reconciles against it;
     * start_key tells Snowflake which part of the raw log this replay covered.
     */
    private String exportSnapshot(long runId, String startKey, Map<String, Map<String, String>> after) {
        String key = S3Keys.snapshot(prefix, runId, Instant.now());
        StringBuilder lines = new StringBuilder();
        after.forEach((orderId, values) -> {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("replay_run_id", runId);
            line.put("start_key", startKey);
            line.put("order_id", orderId);
            values.forEach((k, v) -> line.put(k, "null".equals(v) ? null : v));
            lines.append(jsonMapper.writeValueAsString(line)).append('\n');
        });
        store.put(key, lines.toString(), Map.of("replay-run-id", String.valueOf(runId)));
        return key;
    }

    // ---------------------------------------------------------------- replay_runs bookkeeping

    private long startRun() {
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(c -> {
            PreparedStatement ps = c.prepareStatement("INSERT INTO replay_runs (status) VALUES ('RUNNING')",
                    Statement.RETURN_GENERATED_KEYS);
            return ps;
        }, keyHolder);
        return keyHolder.getKey().longValue();
    }

    private void finishRun(ReplayReport r) {
        jdbc.update("""
                UPDATE replay_runs SET status = 'COMPLETED', events_read = ?, events_applied = ?,
                       side_effects_skipped = ?, snapshot_s3_key = ?, finished_at = CURRENT_TIMESTAMP(3)
                WHERE id = ?""", r.eventsRead(), r.applied(), r.sideEffectsSkipped(), r.snapshotKey(), r.runId());
    }

    private void failRun(long runId, RuntimeException e) {
        String msg = (e.getClass().getSimpleName() + ": " + e.getMessage());
        jdbc.update("UPDATE replay_runs SET status = 'FAILED', error_message = ?, finished_at = CURRENT_TIMESTAMP(3) WHERE id = ?",
                msg.length() > 500 ? msg.substring(0, 500) : msg, runId);
        log.error("REPLAY {} failed", runId, e);
    }

    private int replayCount(String status) {
        return count("SELECT COUNT(*) FROM inbox WHERE source = 'REPLAY' AND status = '" + status + "'");
    }

    private int count(String sql) {
        Integer n = jdbc.queryForObject(sql, Integer.class);
        return n == null ? 0 : n;
    }
}
