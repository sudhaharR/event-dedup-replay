package com.edr.consumer.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * One received message, stored exactly as it arrived (raw_payload).
 * The other event fields are read best-effort and may be null for malformed messages;
 * the consumer re-validates from raw_payload in Stage 4.
 */
@Entity
@Table(name = "inbox")
public class InboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id")
    private String eventId;

    @Column(name = "entity_id")
    private String entityId;

    @Column(name = "version")
    private Integer version;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "event_time")
    private Instant eventTime;

    @Column(name = "raw_payload", nullable = false)
    private String rawPayload;

    @Enumerated(EnumType.STRING)
    @Column(name = "source", nullable = false)
    private EventSource source = EventSource.LIVE;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private InboxStatus status = InboxStatus.PENDING;

    @Column(name = "was_out_of_order", nullable = false)
    private boolean wasOutOfOrder = false;

    @Column(name = "attempts", nullable = false)
    private int attempts = 0;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "s3_key")
    private String s3Key;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    protected InboxEvent() {
        // for JPA
    }

    public InboxEvent(String rawPayload, String eventId, String entityId, Integer version,
                      String eventType, Instant eventTime, EventSource source, Instant receivedAt) {
        this.rawPayload = rawPayload;
        this.eventId = eventId;
        this.entityId = entityId;
        this.version = version;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.source = source;
        this.receivedAt = receivedAt;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getEntityId() { return entityId; }
    public Integer getVersion() { return version; }
    public String getEventType() { return eventType; }
    public Instant getEventTime() { return eventTime; }
    public String getRawPayload() { return rawPayload; }
    public EventSource getSource() { return source; }
    public InboxStatus getStatus() { return status; }
    public boolean isWasOutOfOrder() { return wasOutOfOrder; }
    public int getAttempts() { return attempts; }
    public String getLastError() { return lastError; }
    public String getS3Key() { return s3Key; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getProcessedAt() { return processedAt; }

    public void setStatus(InboxStatus status) { this.status = status; }
    public void setWasOutOfOrder(boolean wasOutOfOrder) { this.wasOutOfOrder = wasOutOfOrder; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public void setS3Key(String s3Key) { this.s3Key = s3Key; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
}
