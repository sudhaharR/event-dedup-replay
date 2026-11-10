package com.edr.consumer.processing;

/** Why a message was dead-lettered. Phase A = structural, Phase B = business, plus processing failures. */
public enum ErrorCode {
    // Phase A - structural validation
    MALFORMED_JSON,
    MISSING_FIELD,
    INVALID_FIELD_TYPE,
    UNKNOWN_EVENT_TYPE,
    INVALID_VERSION,
    INVALID_PAYLOAD,

    // Phase B - business validation (checked when the event is next in line)
    INVALID_TRANSITION,
    AMOUNT_MISMATCH,
    DUPLICATE_PAYMENT,
    EVENT_ID_CONFLICT,

    // Unexpected error that kept failing after the maximum number of retries
    PROCESSING_ERROR
}
