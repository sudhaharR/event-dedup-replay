package com.edr.consumer.reconcile;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Snowflake settings ("snowflake.*" in application.properties, values from the project .env file).
 *
 * @param enabled        turn the Snowflake reconciliation on/off
 * @param account        account identifier, e.g. "abc12345.ap-south-1" or "myorg-myaccount"
 * @param user           service user (key-pair login, no password), e.g. EDR_APP
 * @param privateKeyFile path to the unencrypted PKCS#8 private key (.p8)
 */
@ConfigurationProperties(prefix = "snowflake")
public record SnowflakeProperties(
        boolean enabled,
        String account,
        String user,
        String privateKeyFile,
        String warehouse,
        String database,
        String schema,
        String role) {

    /** jdbc:snowflake://<account>.snowflakecomputing.com/?warehouse=..&db=..&schema=..&role=.. */
    public String jdbcUrl() {
        String host = account.trim().replaceAll("(?i)\\.snowflakecomputing\\.com$", "");
        return "jdbc:snowflake://" + host + ".snowflakecomputing.com/"
                + "?warehouse=" + warehouse + "&db=" + database + "&schema=" + schema + "&role=" + role;
    }

    public String location() {
        return "snowflake://" + account + "/" + database + "." + schema;
    }
}
