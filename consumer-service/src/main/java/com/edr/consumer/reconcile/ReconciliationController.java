package com.edr.consumer.reconcile;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.function.Function;

/**
 * POST /reconciliation/refresh   load new S3 files into Snowflake, then return the reconciliation
 * GET  /reconciliation           return the reconciliation from what is already loaded
 */
@RestController
public class ReconciliationController {

    private final ObjectProvider<SnowflakeClient> snowflake;
    private final JdbcTemplate mysql;

    public ReconciliationController(ObjectProvider<SnowflakeClient> snowflake, JdbcTemplate mysql) {
        this.snowflake = snowflake;
        this.mysql = mysql;
    }

    @GetMapping("/reconciliation")
    public Map<String, Object> result() {
        return call(ReconciliationService::result);
    }

    @PostMapping("/reconciliation/refresh")
    public Map<String, Object> refresh() {
        return call(ReconciliationService::refresh);
    }

    private Map<String, Object> call(Function<ReconciliationService, Map<String, Object>> action) {
        SnowflakeClient client = snowflake.getIfAvailable();
        if (client == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Snowflake reconciliation is not enabled - set SNOWFLAKE_ENABLED=true in .env and restart");
        }
        try {
            return action.apply(new ReconciliationService(client, mysql));
        } catch (DataAccessException e) {
            Throwable root = e.getMostSpecificCause();
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Snowflake error: " + root.getMessage(), e);
        }
    }
}
