package com.edr.consumer.reconcile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Independent reconciliation in Snowflake (see snowflake/05_views.sql).
 *
 *  refresh(): COPY INTO loads the new raw events, dead letters and replay snapshots from S3.
 *  result():  reads RECONCILIATION_SUMMARY / RECONCILIATION (expected state recomputed in SQL from the raw
 *             log vs the latest replay snapshot), and compares Snowflake's EXPECTED_CHARGES with the
 *             real payment_ledger in MySQL - so the money is checked independently too.
 */
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    /** The same three statements as snowflake/04_load.sql. */
    public static final List<String> LOAD_STATEMENTS = List.of(
            """
            COPY INTO RAW_EVENTS (S3_KEY, RAW_TEXT, FILE_LAST_MODIFIED)
              FROM (SELECT METADATA$FILENAME, $1, METADATA$FILE_LAST_MODIFIED FROM @EDR_STAGE/raw/)
              FILE_FORMAT = (FORMAT_NAME = 'FF_WHOLE_FILE') ON_ERROR = 'CONTINUE'""",
            """
            COPY INTO DEAD_LETTERS (S3_KEY, RECORD)
              FROM (SELECT METADATA$FILENAME, $1 FROM @EDR_STAGE/dlq/)
              FILE_FORMAT = (FORMAT_NAME = 'FF_JSON_LINES') ON_ERROR = 'CONTINUE'""",
            """
            COPY INTO SNAPSHOTS (S3_KEY, LINE)
              FROM (SELECT METADATA$FILENAME, $1 FROM @EDR_STAGE/snapshots/)
              FILE_FORMAT = (FORMAT_NAME = 'FF_JSON_LINES') ON_ERROR = 'CONTINUE'""");

    private final SnowflakeClient snowflake;
    private final JdbcTemplate mysql;
    private final List<String> loadStatements;

    public ReconciliationService(SnowflakeClient snowflake, JdbcTemplate mysql) {
        this(snowflake, mysql, LOAD_STATEMENTS);
    }

    /** loadStatements can be replaced in tests (they are Snowflake-specific). */
    public ReconciliationService(SnowflakeClient snowflake, JdbcTemplate mysql, List<String> loadStatements) {
        this.snowflake = snowflake;
        this.mysql = mysql;
        this.loadStatements = loadStatements;
    }

    /** Loads new files from S3 into Snowflake, then returns the reconciliation result. */
    public Map<String, Object> refresh() {
        long start = System.currentTimeMillis();
        for (String sql : loadStatements) {
            snowflake.jdbc().execute(sql);
        }
        long loadMs = System.currentTimeMillis() - start;
        log.info("Snowflake load from S3 finished in {} ms", loadMs);
        Map<String, Object> result = result();
        result.put("load_ms", loadMs);
        return result;
    }

    public Map<String, Object> result() {
        JdbcTemplate sf = snowflake.jdbc();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("checked_by", snowflake.location());

        Map<String, Object> summary = lower(sf.queryForMap("SELECT * FROM RECONCILIATION_SUMMARY"));
        out.put("summary", summary);

        List<Map<String, Object>> rows = sf.queryForList("SELECT * FROM RECONCILIATION ORDER BY ORDER_ID");
        out.put("differences", rows.stream()
                .filter(r -> !"MATCH".equals(value(r, "RESULT")))
                .limit(100).map(ReconciliationService::lower).toList());

        out.put("charges", checkCharges(sf, rows));

        out.put("daily_metrics", sf.queryForList("SELECT * FROM DAILY_METRICS ORDER BY DAY DESC LIMIT 7")
                .stream().map(ReconciliationService::lower).toList());
        out.put("dead_letter_trends", sf.queryForList(
                        "SELECT * FROM DEAD_LETTER_TRENDS ORDER BY DAY DESC, ERROR_CODE LIMIT 50")
                .stream().map(ReconciliationService::lower).toList());
        return out;
    }

    /** Snowflake's expected charges vs the real ledger in MySQL, for the orders being reconciled. */
    private Map<String, Object> checkCharges(JdbcTemplate sf, List<Map<String, Object>> reconciled) {
        Set<String> orders = reconciled.stream().map(r -> String.valueOf(value(r, "ORDER_ID")))
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> expected = sf.queryForList("SELECT EVENT_ID, ORDER_ID FROM EXPECTED_CHARGES").stream()
                .filter(r -> orders.contains(String.valueOf(value(r, "ORDER_ID"))))
                .map(r -> String.valueOf(value(r, "EVENT_ID")))
                .collect(Collectors.toCollection(TreeSet::new));

        Set<String> actual = new TreeSet<>();
        List<String> orderList = new ArrayList<>(orders);
        for (int i = 0; i < orderList.size(); i += 500) {           // IN (...) in chunks
            List<String> chunk = orderList.subList(i, Math.min(i + 500, orderList.size()));
            String marks = String.join(",", chunk.stream().map(o -> "?").toList());
            actual.addAll(mysql.queryForList(
                    "SELECT event_id FROM payment_ledger WHERE order_id IN (" + marks + ")",
                    String.class, chunk.toArray()));
        }

        Set<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        Set<String> unexpected = new TreeSet<>(actual);
        unexpected.removeAll(expected);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("expected_charges", expected.size());
        m.put("ledger_charges", actual.size());
        m.put("missing_charges", missing);
        m.put("unexpected_charges", unexpected);
        m.put("result", missing.isEmpty() && unexpected.isEmpty() ? "MATCH" : "MISMATCH");
        return m;
    }

    /** Snowflake returns UPPER_CASE column names; the API uses snake_case like everywhere else. */
    private static Map<String, Object> lower(Map<String, Object> row) {
        Map<String, Object> m = new LinkedHashMap<>();
        row.forEach((k, v) -> m.put(k.toLowerCase(Locale.ROOT), v));
        return m;
    }

    private static Object value(Map<String, Object> row, String column) {
        Object v = row.get(column);
        return v != null ? v : row.get(column.toLowerCase(Locale.ROOT));
    }
}
