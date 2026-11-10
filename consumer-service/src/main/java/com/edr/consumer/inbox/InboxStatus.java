package com.edr.consumer.inbox;

/** Lifecycle of a message in the inbox. Matches the MySQL ENUM in inbox.status. */
public enum InboxStatus {
    PENDING,
    APPLIED,
    DUPLICATE,
    STALE,
    PARKED,
    DEAD_LETTERED
}
