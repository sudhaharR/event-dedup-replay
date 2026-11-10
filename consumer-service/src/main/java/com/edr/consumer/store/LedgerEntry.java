package com.edr.consumer.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A side effect: one "charge" in the payment ledger.
 * UNIQUE(event_id) in MySQL guarantees one charge per event, even after a replay.
 */
@Entity
@Table(name = "payment_ledger")
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "payment_id", nullable = false)
    private String paymentId;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false)
    private String currency;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected LedgerEntry() {
    }

    public LedgerEntry(String eventId, String orderId, String paymentId, BigDecimal amount,
                       String currency, Instant createdAt) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amount = amount;
        this.currency = currency;
        this.createdAt = createdAt;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getOrderId() { return orderId; }
    public String getPaymentId() { return paymentId; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public Instant getCreatedAt() { return createdAt; }
}
