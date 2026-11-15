package com.edr.consumer.archive;

import java.util.List;
import java.util.Map;

/** Durable object storage for raw events, dead letters and snapshots (Amazon S3 in this project). */
public interface RawEventStore {

    /** Stores the body under the key. Overwriting the same key with the same body is harmless. */
    void put(String key, String body, Map<String, String> metadata);

    /** All keys that start with the prefix, in alphabetical order. */
    List<String> listKeys(String prefix);

    /** The body stored under the key. */
    String get(String key);

    /** Short description for logs, e.g. "s3://my-bucket". */
    String location();
}
