package com.edr.consumer.reconcile;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Path;

/** Creates the Snowflake client - only when snowflake.enabled=true. */
@Configuration
@ConditionalOnProperty(name = "snowflake.enabled", havingValue = "true")
public class SnowflakeConfig {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeConfig.class);

    @Bean(destroyMethod = "close")
    public SnowflakeClient snowflakeClient(SnowflakeProperties props) {
        require(props.account(), "SNOWFLAKE_ACCOUNT");
        require(props.user(), "SNOWFLAKE_USER");
        require(props.privateKeyFile(), "SNOWFLAKE_PRIVATE_KEY_FILE");
        Path key = Path.of(props.privateKeyFile());
        if (!Files.isReadable(key)) {
            log.warn("Snowflake private key file not found: {} (relative to {})",
                    key, Path.of("").toAbsolutePath());
        }

        // The pool connects lazily on first use, so the service starts even if Snowflake is unreachable.
        HikariDataSource ds = new HikariDataSource();
        ds.setPoolName("snowflake");
        ds.setJdbcUrl(props.jdbcUrl());
        ds.setUsername(props.user());
        ds.addDataSourceProperty("authenticator", "SNOWFLAKE_JWT");            // key-pair login, no password
        ds.addDataSourceProperty("private_key_file", key.toAbsolutePath().toString());
        ds.addDataSourceProperty("JDBC_QUERY_RESULT_FORMAT", "JSON");           // avoids Arrow module issues on Java 17+
        ds.setMaximumPoolSize(2);
        ds.setMinimumIdle(0);
        ds.setConnectionTimeout(60_000);
        log.info("Snowflake reconciliation enabled: {}", props.location());
        return new SnowflakeClient(new JdbcTemplate(ds), props.location(), ds);
    }

    private static void require(String value, String envName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("SNOWFLAKE_ENABLED=true but " + envName + " is not set in .env");
        }
    }
}
