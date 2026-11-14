package com.edr.consumer.archive;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/** Health of the S3 archiver, exposed in GET /metrics so S3 problems are observable. */
public class ArchiveStatus {

    private final AtomicLong uploaded = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private volatile Instant lastSuccessAt;
    private volatile Instant lastFailureAt;
    private volatile String lastError;

    void success(int count) {
        uploaded.addAndGet(count);
        lastSuccessAt = Instant.now();
    }

    void failure(Exception e) {
        failures.incrementAndGet();
        lastFailureAt = Instant.now();
        String msg = e.getClass().getSimpleName() + ": " + e.getMessage();
        lastError = msg.length() > 300 ? msg.substring(0, 300) : msg;
    }

    public long uploaded() { return uploaded.get(); }
    public long failures() { return failures.get(); }
    public Instant lastSuccessAt() { return lastSuccessAt; }
    public Instant lastFailureAt() { return lastFailureAt; }
    public String lastError() { return lastError; }
}
