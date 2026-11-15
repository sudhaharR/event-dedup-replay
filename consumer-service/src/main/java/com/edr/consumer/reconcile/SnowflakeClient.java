package com.edr.consumer.reconcile;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Access to the Snowflake analytics schema.
 * Deliberately NOT a DataSource bean, so Spring's main MySQL DataSource is never affected.
 */
public class SnowflakeClient implements AutoCloseable {

    private final JdbcTemplate jdbc;
    private final String location;
    private final AutoCloseable resource;

    public SnowflakeClient(JdbcTemplate jdbc, String location, AutoCloseable resource) {
        this.jdbc = jdbc;
        this.location = location;
        this.resource = resource;
    }

    public JdbcTemplate jdbc() {
        return jdbc;
    }

    public String location() {
        return location;
    }

    @Override
    public void close() throws Exception {
        if (resource != null) {
            resource.close();
        }
    }
}
