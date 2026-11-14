package com.edr.consumer.archive;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * S3 settings (application.properties "s3.*", values come from the project .env file).
 *
 * @param enabled           turn the S3 raw event log on/off
 * @param bucket            bucket name
 * @param region            AWS region, e.g. ap-south-1
 * @param prefix            optional folder prefix for every key, e.g. "dev/"
 * @param endpoint          optional custom endpoint (S3-compatible storage for tests); empty = real AWS
 * @param accessKeyId       IAM access key; empty = use the default AWS credential chain
 * @param secretAccessKey   IAM secret key
 * @param archiveIntervalMs how often the archiver uploads pending messages
 */
@ConfigurationProperties(prefix = "s3")
public record S3Properties(
        boolean enabled,
        String bucket,
        String region,
        String prefix,
        String endpoint,
        String accessKeyId,
        String secretAccessKey,
        long archiveIntervalMs) {

    /** Prefix normalised to "" or "something/". */
    public String normalizedPrefix() {
        if (prefix == null || prefix.isBlank()) {
            return "";
        }
        String p = prefix.trim().replaceAll("^/+", "");
        return p.endsWith("/") ? p : p + "/";
    }

    public boolean hasStaticCredentials() {
        return accessKeyId != null && !accessKeyId.isBlank()
                && secretAccessKey != null && !secretAccessKey.isBlank();
    }

    public boolean hasCustomEndpoint() {
        return endpoint != null && !endpoint.isBlank();
    }
}
