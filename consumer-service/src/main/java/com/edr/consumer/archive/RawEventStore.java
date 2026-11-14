package com.edr.consumer.archive;

import java.util.Map;

/** Durable object storage for raw events and dead letters (Amazon S3 in this project). */
public interface RawEventStore {

    /** Stores the body under the key. Overwriting the same key with the same body is harmless. */
    void put(String key, String body, Map<String, String> metadata);

    /** Short description for logs, e.g. "s3://my-bucket". */
    String location();
}
