package com.edr.consumer.store;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/** Current state of one order, built only from applied events. */
@Entity
@Table(name = "order_projection")
public class OrderProjection {

    @Id
    @Column(name = "order_id")
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OrderStatus status;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(name = "order_amount", nullable = false)
    private BigDecimal orderAmount;

    @Column(name = "amount_paid", nullable = false)
    private BigDecimal amountPaid = BigDecimal.ZERO;

    @Column(name = "currency", nullable = false)
    private String currency;

    @Column(name = "tracking_id")
    private String trackingId;

    @Column(name = "cancel_reason")
    private String cancelReason;

    @Column(name = "last_version", nullable = false)
    private int lastVersion;

    @Column(name = "last_event_id", nullable = false)
    private String lastEventId;

    @Column(name = "last_event_time", nullable = false)
    private Instant lastEventTime;

    // created_at / updated_at are filled by MySQL defaults
    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", insertable = false, updatable = false)
    private Instant updatedAt;

    protected OrderProjection() {
    }

    public OrderProjection(String orderId, String customerId, BigDecimal orderAmount, String currency) {
        this.orderId = orderId;
        this.status = OrderStatus.CREATED;
        this.customerId = customerId;
        this.orderAmount = orderAmount;
        this.currency = currency;
    }

    /** Records which event produced the current state. */
    public void advanceTo(int version, String eventId, Instant eventTime) {
        this.lastVersion = version;
        this.lastEventId = eventId;
        this.lastEventTime = eventTime;
    }

    public String getOrderId() { return orderId; }
    public OrderStatus getStatus() { return status; }
    public String getCustomerId() { return customerId; }
    public BigDecimal getOrderAmount() { return orderAmount; }
    public BigDecimal getAmountPaid() { return amountPaid; }
    public String getCurrency() { return currency; }
    public String getTrackingId() { return trackingId; }
    public String getCancelReason() { return cancelReason; }
    public int getLastVersion() { return lastVersion; }
    public String getLastEventId() { return lastEventId; }
    public Instant getLastEventTime() { return lastEventTime; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void setStatus(OrderStatus status) { this.status = status; }
    public void setAmountPaid(BigDecimal amountPaid) { this.amountPaid = amountPaid; }
    public void setTrackingId(String trackingId) { this.trackingId = trackingId; }
    public void setCancelReason(String cancelReason) { this.cancelReason = cancelReason; }
}
