package com.edr.consumer.archive;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Writes objects to an S3 bucket with the AWS SDK v2. */
public class S3RawEventStore implements RawEventStore {

    private final S3Client s3;
    private final String bucket;

    public S3RawEventStore(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void put(String key, String body, Map<String, String> metadata) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .metadata(metadata)          // encryption at rest: S3 default (SSE-S3) on the bucket
                .build();
        s3.putObject(request, RequestBody.fromString(body, StandardCharsets.UTF_8));
    }

    @Override
    public String location() {
        return "s3://" + bucket;
    }
}
