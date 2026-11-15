package com.edr.consumer.query;

import com.edr.consumer.archive.ArchiveStatus;
import com.edr.consumer.archive.S3Properties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only API over the processing tables. Used by the Python simulator (Stage 5)
 * to check results, and by the React dashboard (Stages 9-10).
 * Field names are the database column names (snake_case), like the event contract.
 */
@RestController
public class QueryController {

    private static final int MAX_LIMIT = 1000;

    private final JdbcTemplate jdbc;
    private final S3Properties s3;
    private final ObjectProvider<ArchiveStatus> archiveStatus;

    public QueryController(JdbcTemplate jdbc, S3Properties s3, ObjectProvider<ArchiveStatus> archiveStatus) {
        this.jdbc = jdbc;
        this.s3 = s3;
        this.archiveStatus = archiveStatus;
    }

    @GetMapping("/orders")
    public List<Map<String, Object>> orders() {
        return jdbc.queryForList("""
                SELECT order_id, status, customer_id, order_amount, amount_paid, currency, tracking_id,
                       cancel_reason, last_version, last_event_id, last_event_time, updated_at
                FROM order_projection ORDER BY order_id""");
    }

    @GetMapping("/orders/{orderId}")
    public Map<String, Object> order(@PathVariable String orderId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT order_id, status, customer_id, order_amount, amount_paid, currency, tracking_id,
                       cancel_reason, last_version, last_event_id, last_event_time, updated_at
                FROM order_projection WHERE order_id = ?""", orderId);
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + orderId + " not found");
        }
        return rows.get(0);
    }

    /** v_metrics plus the health of the S3 raw event log (archive backlog and last error). */
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>(jdbc.queryForMap("SELECT * FROM v_metrics"));
        m.put("s3_enabled", s3.enabled());
        if (s3.enabled()) {
            m.put("s3_archive_backlog",
                    jdbc.queryForObject("SELECT COUNT(*) FROM inbox WHERE s3_key IS NULL", Long.class));
            m.put("s3_dlq_backlog",
                    jdbc.queryForObject("""
                            SELECT COUNT(*) FROM dead_letter d JOIN inbox i ON i.id = d.inbox_id
                            WHERE d.s3_key IS NULL AND i.source = 'LIVE'""", Long.class));
            ArchiveStatus status = archiveStatus.getIfAvailable();
            if (status != null) {
                m.put("s3_uploaded", status.uploaded());
                m.put("s3_failures", status.failures());
                m.put("s3_last_success_at", status.lastSuccessAt());
                m.put("s3_last_failure_at", status.lastFailureAt());   // compare with last success: is the error current?
                m.put("s3_last_error", status.lastError());
            }
        }
        List<Map<String, Object>> lastReplay = jdbc.queryForList(
                "SELECT id, status, finished_at FROM replay_runs ORDER BY id DESC LIMIT 1");
        m.put("last_replay_id", lastReplay.isEmpty() ? null : lastReplay.get(0).get("id"));
        m.put("last_replay_status", lastReplay.isEmpty() ? null : lastReplay.get(0).get("status"));
        m.put("last_replay_finished_at", lastReplay.isEmpty() ? null : lastReplay.get(0).get("finished_at"));
        return m;
    }

    /** Dead letters of live traffic (like the metrics); include_replay=true also shows replay copies. */
    @GetMapping("/dead-letters")
    public List<Map<String, Object>> deadLetters(@RequestParam(defaultValue = "200") int limit,
                                                 @RequestParam(name = "include_replay", defaultValue = "false")
                                                 boolean includeReplay) {
        return jdbc.queryForList("""
                SELECT d.id, d.inbox_id, d.event_id, d.entity_id, d.error_code, d.error_message, d.status,
                       d.raw_payload, d.received_at, d.dead_lettered_at, i.source
                FROM dead_letter d JOIN inbox i ON i.id = d.inbox_id
                WHERE i.source = 'LIVE' OR ?
                ORDER BY d.id DESC LIMIT ?""", includeReplay, clamp(limit));
    }

    @GetMapping("/ledger")
    public List<Map<String, Object>> ledger(@RequestParam(name = "order_id", required = false) String orderId) {
        if (orderId != null) {
            return jdbc.queryForList("""
                    SELECT id, event_id, order_id, payment_id, amount, currency, created_at
                    FROM payment_ledger WHERE order_id = ? ORDER BY id""", orderId);
        }
        return jdbc.queryForList("""
                SELECT id, event_id, order_id, payment_id, amount, currency, created_at
                FROM payment_ledger ORDER BY id""");
    }

    @GetMapping("/inbox")
    public List<Map<String, Object>> inbox(@RequestParam(name = "event_id", required = false) String eventId,
                                           @RequestParam(required = false) String status,
                                           @RequestParam(defaultValue = "200") int limit) {
        StringBuilder sql = new StringBuilder("""
                SELECT id, event_id, entity_id, version, event_type, event_time, source, status,
                       was_out_of_order, attempts, last_error, received_at, processed_at
                FROM inbox WHERE 1 = 1""");
        List<Object> args = new ArrayList<>();
        if (eventId != null) {
            sql.append(" AND event_id = ?");
            args.add(eventId);
        }
        if (status != null) {
            sql.append(" AND status = ?");
            args.add(status);
        }
        sql.append(" ORDER BY id DESC LIMIT ?");
        args.add(clamp(limit));
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    private static int clamp(int limit) {
        return Math.max(1, Math.min(limit, MAX_LIMIT));
    }
}
