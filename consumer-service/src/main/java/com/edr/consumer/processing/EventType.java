package com.edr.consumer.processing;

import java.util.Arrays;
import java.util.Optional;

/** The four event types of the contract, with their names on the wire. */
public enum EventType {
    ORDER_CREATED("OrderCreated"),
    PAYMENT_CAPTURED("PaymentCaptured"),
    ORDER_SHIPPED("OrderShipped"),
    ORDER_CANCELLED("OrderCancelled");

    private final String wireName;

    EventType(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static Optional<EventType> fromWire(String name) {
        return Arrays.stream(values()).filter(t -> t.wireName.equals(name)).findFirst();
    }
}
