package com.edr.consumer.store;

/** Order status in the projection. Matches the MySQL ENUM in order_projection.status. */
public enum OrderStatus {
    CREATED,
    PAID,
    SHIPPED,
    CANCELLED
}
