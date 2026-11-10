package com.edr.consumer.processing;

import com.edr.consumer.inbox.InboxStatus;

/** Result of handling one message: the new inbox status and the order it belongs to (may be null). */
public record Outcome(InboxStatus status, String entityId) {
}
