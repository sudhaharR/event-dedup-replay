package com.edr.consumer.replay;

/** Replay cannot start now (system not quiet, or another replay is running). Mapped to HTTP 409. */
public class ReplayRejectedException extends RuntimeException {

    public ReplayRejectedException(String message) {
        super(message);
    }
}
