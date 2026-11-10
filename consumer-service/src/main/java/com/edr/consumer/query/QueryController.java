package com.edr.consumer.query;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
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

    public QueryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
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

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return jdbc.queryForMap("SELECT * FROM v_metrics");
    }

    @GetMapping("/dead-letters")
    public List<Map<String, Object>> deadLetters(@RequestParam(defaultValue = "200") int limit) {
        return jdbc.queryForList("""
                SELECT id, inbox_id, event_id, entity_id, error_code, error_message, status,
                       raw_payload, received_at, dead_lettered_at
                FROM dead_letter ORDER BY id DESC LIMIT ?""", clamp(limit));
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
