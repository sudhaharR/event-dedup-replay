package com.edr.consumer.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/** A rejected (poison) message, isolated with the reason so it never blocks other events. */
@Entity
@Table(name = "dead_letter")
public class DeadLetter {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "inbox_id", nullable = false)
    private Long inboxId;

    @Column(name = "event_id")
    private String eventId;

    @Column(name = "entity_id")
    private String entityId;

    @Column(name = "raw_payload", nullable = false)
    private String rawPayload;

    @Column(name = "error_code", nullable = false)
    private String errorCode;

    @Column(name = "error_message", nullable = false)
    private String errorMessage;

    // status ('NEW') and dead_lettered_at are filled by MySQL defaults
    @Column(name = "status", insertable = false, updatable = false)
    private String status;

    @Column(name = "s3_key")
    private String s3Key;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "dead_lettered_at", insertable = false, updatable = false)
    private Instant deadLetteredAt;

    protected DeadLetter() {
    }

    public DeadLetter(Long inboxId, String eventId, String entityId, String rawPayload,
                      String errorCode, String errorMessage, Instant receivedAt) {
        this.inboxId = inboxId;
        this.eventId = eventId;
        this.entityId = entityId;
        this.rawPayload = rawPayload;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.receivedAt = receivedAt;
    }

    public Long getId() { return id; }
    public Long getInboxId() { return inboxId; }
    public String getEventId() { return eventId; }
    public String getEntityId() { return entityId; }
    public String getRawPayload() { return rawPayload; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public String getStatus() { return status; }
    public String getS3Key() { return s3Key; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getDeadLetteredAt() { return deadLetteredAt; }

    public void setS3Key(String s3Key) { this.s3Key = s3Key; }
}
