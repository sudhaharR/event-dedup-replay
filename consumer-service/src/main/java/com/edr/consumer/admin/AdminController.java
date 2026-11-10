package com.edr.consumer.admin;

import com.edr.consumer.processing.FaultInjector;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * DEVELOPMENT / DEMO ONLY. Lets the simulator start each scenario from an empty database
 * and arm the crash-after-side-effect demo. Switched off unless admin.enabled=true.
 */
@RestController
@RequestMapping("/admin")
@ConditionalOnProperty(name = "admin.enabled", havingValue = "true")
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class);
    private static final List<String> TABLES = List.of("inbox", "processed_events", "order_projection",
            "park_buffer", "dead_letter", "payment_ledger", "replay_runs");

    private final JdbcTemplate jdbc;
    private final FaultInjector faultInjector;

    public AdminController(JdbcTemplate jdbc, FaultInjector faultInjector) {
        this.jdbc = jdbc;
        this.faultInjector = faultInjector;
    }

    /** Empties every processing table (including the ledger). */
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        TABLES.forEach(t -> jdbc.execute("TRUNCATE TABLE " + t));
        log.warn("ADMIN: all processing tables truncated");
        return Map.of("reset", true, "tables", TABLES);
    }

    /** Makes the consumer crash once, after the side effect, for each given event id. */
    @PostMapping("/faults/fail-once")
    public Map<String, Object> failOnce(@RequestBody FailOnceRequest request) {
        request.eventIds().forEach(faultInjector::failOnceFor);
        log.warn("ADMIN: armed crash-after-side-effect for {}", request.eventIds());
        return Map.of("armed", request.eventIds());
    }

    /** Body: {"event_ids": ["E102"]} */
    public record FailOnceRequest(@JsonProperty("event_ids") List<String> eventIds) {
    }
}
