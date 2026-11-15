package com.edr.consumer.replay;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * POST /replay              rebuild the projection from the S3 raw event log (default window: current dataset)
 * POST /replay?from=...     only raw events received at/after a time (2026-11-13T10:00:00Z) or a day (2026-11-13)
 * GET  /replay-runs         history of replay runs
 */
@RestController
public class ReplayController {

    private final ObjectProvider<ReplayService> replayService;
    private final JdbcTemplate jdbc;

    public ReplayController(ObjectProvider<ReplayService> replayService, JdbcTemplate jdbc) {
        this.replayService = replayService;
        this.jdbc = jdbc;
    }

    @PostMapping("/replay")
    public Map<String, Object> replay(@RequestParam(required = false) String from) {
        ReplayService service = replayService.getIfAvailable();
        if (service == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Replay reads the S3 raw event log - set S3_ENABLED=true in .env and restart");
        }
        try {
            return service.replay(parse(from)).toResponse();
        } catch (ReplayRejectedException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage());
        }
    }

    @GetMapping("/replay-runs")
    public List<Map<String, Object>> runs() {
        return jdbc.queryForList("""
                SELECT id, status, events_read, events_applied, side_effects_skipped, snapshot_s3_key,
                       error_message, started_at, finished_at
                FROM replay_runs ORDER BY id DESC LIMIT 50""");
    }

    private static Instant parse(String from) {
        if (from == null || from.isBlank()) {
            return null;
        }
        try {
            return from.length() == 10
                    ? LocalDate.parse(from).atStartOfDay().toInstant(ZoneOffset.UTC)
                    : OffsetDateTime.parse(from).toInstant();
        } catch (DateTimeParseException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "'from' must be a day (2026-11-13) or a UTC time (2026-11-13T10:00:00Z)");
        }
    }
}
