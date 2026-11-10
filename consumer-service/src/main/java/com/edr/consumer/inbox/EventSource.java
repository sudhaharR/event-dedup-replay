package com.edr.consumer.inbox;

/** Where a message came from: the live producer or a replay run. */
public enum EventSource {
    LIVE,
    REPLAY
}
