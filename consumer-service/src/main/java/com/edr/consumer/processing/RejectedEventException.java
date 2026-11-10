package com.edr.consumer.processing;

/** Thrown by validation when a message must go to the dead-letter table. */
public class RejectedEventException extends RuntimeException {

    private final ErrorCode code;

    public RejectedEventException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
