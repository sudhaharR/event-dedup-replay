package com.edr.consumer.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.Instant;

/**
 * Idempotency record: one row per event that has been applied.
 * Implements Persistable so save() always INSERTs (never a silent UPDATE) -
 * a second insert of the same event_id fails on the primary key.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent implements Persistable<String> {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "entity_id", nullable = false)
    private String entityId;

    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "payload_hash", nullable = false)
    private String payloadHash;

    @Column(name = "event_time", nullable = false)
    private Instant eventTime;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String entityId, int version, String eventType,
                          String payloadHash, Instant eventTime, Instant processedAt) {
        this.eventId = eventId;
        this.entityId = entityId;
        this.version = version;
        this.eventType = eventType;
        this.payloadHash = payloadHash;
        this.eventTime = eventTime;
        this.processedAt = processedAt;
    }

    @Override
    public String getId() { return eventId; }

    @Override
    public boolean isNew() { return true; }

    public String getEventId() { return eventId; }
    public String getEntityId() { return entityId; }
    public int getVersion() { return version; }
    public String getEventType() { return eventType; }
    public String getPayloadHash() { return payloadHash; }
    public Instant getEventTime() { return eventTime; }
    public Instant getProcessedAt() { return processedAt; }
}
