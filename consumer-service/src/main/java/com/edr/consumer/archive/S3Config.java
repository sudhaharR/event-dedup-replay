package com.edr.consumer.archive;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

/** Creates the S3 client - only when s3.enabled=true. */
@Configuration
@ConditionalOnProperty(name = "s3.enabled", havingValue = "true")
public class S3Config {

    @Bean(destroyMethod = "close")
    public S3Client s3Client(S3Properties props) {
        if (props.bucket() == null || props.bucket().isBlank()) {
            throw new IllegalStateException("s3.enabled=true but S3_BUCKET is not set in .env");
        }
        AwsCredentialsProvider credentials = props.hasStaticCredentials()
                ? StaticCredentialsProvider.create(AwsBasicCredentials.create(props.accessKeyId(), props.secretAccessKey()))
                : DefaultCredentialsProvider.builder().build();

        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(credentials);
        if (props.hasCustomEndpoint()) {
            // S3-compatible storage (e.g. MinIO in tests) uses path-style URLs
            builder.endpointOverride(URI.create(props.endpoint())).forcePathStyle(true);
        }
        return builder.build();
    }

    @Bean
    public RawEventStore rawEventStore(S3Client s3Client, S3Properties props) {
        return new S3RawEventStore(s3Client, props.bucket());
    }

    @Bean
    public ArchiveStatus archiveStatus() {
        return new ArchiveStatus();
    }
}
