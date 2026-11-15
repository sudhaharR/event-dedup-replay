package com.edr.consumer.archive;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Reads and writes objects in an S3 bucket with the AWS SDK v2. */
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
    public List<String> listKeys(String prefix) {
        // the paginator follows continuation tokens, so buckets with more than 1,000 objects work too
        return s3.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(prefix))
                .contents().stream()
                .map(S3Object::key)
                .sorted()
                .toList();
    }

    @Override
    public String get(String key) {
        return s3.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asUtf8String();
    }

    @Override
    public String location() {
        return "s3://" + bucket;
    }
}
