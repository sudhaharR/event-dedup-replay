package com.edr.consumer.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/** An event that arrived before the version it depends on. Waits here until it is next in line. */
@Entity
@Table(name = "park_buffer")
public class ParkedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "inbox_id", nullable = false)
    private Long inboxId;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "entity_id", nullable = false)
    private String entityId;

    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "raw_payload", nullable = false)
    private String rawPayload;

    @Column(name = "expected_version", nullable = false)
    private int expectedVersion;

    @Column(name = "parked_at", nullable = false)
    private Instant parkedAt;

    protected ParkedEvent() {
    }

    public ParkedEvent(Long inboxId, String eventId, String entityId, int version, String eventType,
                       String rawPayload, int expectedVersion, Instant parkedAt) {
        this.inboxId = inboxId;
        this.eventId = eventId;
        this.entityId = entityId;
        this.version = version;
        this.eventType = eventType;
        this.rawPayload = rawPayload;
        this.expectedVersion = expectedVersion;
        this.parkedAt = parkedAt;
    }

    public Long getId() { return id; }
    public Long getInboxId() { return inboxId; }
    public String getEventId() { return eventId; }
    public String getEntityId() { return entityId; }
    public int getVersion() { return version; }
    public String getEventType() { return eventType; }
    public String getRawPayload() { return rawPayload; }
    public int getExpectedVersion() { return expectedVersion; }
    public Instant getParkedAt() { return parkedAt; }
}
