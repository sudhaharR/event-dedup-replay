package com.edr.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.MountableFile;

import java.util.List;

/**
 * Base class for tests that need MySQL.
 *
 * Starts a throwaway MySQL 8.4 container (Testcontainers) loaded with ../db/init/*.sql,
 * so tests never touch your development database on port 3307. Needs Docker Desktop running.
 * The scheduler is switched off: tests call InboxConsumer directly, so results are deterministic.
 *
 * The application also reads the developer's ../.env file. The values below override it, so tests
 * behave the same on every machine: S3 and Snowflake are OFF by default and real AWS keys or
 * Snowflake keys are never visible to a test.
 * Tests that need S3 switch it on themselves (s3.enabled=true) with a fake store or S3Mock.
 */
@SpringBootTest(properties = {
        "consumer.scheduler.enabled=false",
        "S3_ENABLED=false",
        "S3_BUCKET=",
        "S3_PREFIX=",
        "S3_ENDPOINT=",
        "AWS_ACCESS_KEY_ID=",
        "AWS_SECRET_ACCESS_KEY=",
        "SNOWFLAKE_ENABLED=false",
        "SNOWFLAKE_ACCOUNT=",
        "SNOWFLAKE_PRIVATE_KEY_FILE=",
        "consumer.fault-injection.fail-once-event-ids="
})
public abstract class AbstractMySqlTest {

    @ServiceConnection
    static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4")
            .withDatabaseName("event_db")
            .withCopyFileToContainer(MountableFile.forHostPath("../db/init"), "/docker-entrypoint-initdb.d");

    static {
        MYSQL.start();
    }

    private static final List<String> TABLES = List.of("inbox", "processed_events", "order_projection",
            "park_buffer", "dead_letter", "payment_ledger", "replay_runs");

    @Autowired
    protected JdbcTemplate jdbc;

    @BeforeEach
    protected void cleanTables() {
        TABLES.forEach(t -> jdbc.execute("TRUNCATE TABLE " + t));
    }
}
