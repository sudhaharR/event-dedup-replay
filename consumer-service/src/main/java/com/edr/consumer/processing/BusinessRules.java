package com.edr.consumer.processing;

import com.edr.consumer.store.LedgerRepository;
import com.edr.consumer.store.OrderProjection;
import com.edr.consumer.store.OrderStatus;
import org.springframework.stereotype.Component;

import static com.edr.consumer.processing.ErrorCode.AMOUNT_MISMATCH;
import static com.edr.consumer.processing.ErrorCode.DUPLICATE_PAYMENT;
import static com.edr.consumer.processing.ErrorCode.INVALID_TRANSITION;

/**
 * Phase B - business validation. Runs only when the event is the next version for its order,
 * so "order" is the state just before this event.
 *
 *   (none) -OrderCreated-> CREATED -PaymentCaptured-> PAID -OrderShipped-> SHIPPED
 *                          CREATED / PAID -OrderCancelled-> CANCELLED
 */
@Component
public class BusinessRules {

    private final LedgerRepository ledgerRepository;

    public BusinessRules(LedgerRepository ledgerRepository) {
        this.ledgerRepository = ledgerRepository;
    }

    public void check(OrderProjection order, ValidatedEvent event) {
        switch (event.type()) {
            case ORDER_CREATED -> {
                if (order != null) {
                    throw new RejectedEventException(INVALID_TRANSITION, "Order " + event.entityId() + " already exists");
                }
            }
            case PAYMENT_CAPTURED -> {
                requireStatus(order, event, OrderStatus.CREATED);
                if (order.getOrderAmount().compareTo(event.amount()) != 0
                        || !order.getCurrency().equals(event.text("currency"))) {
                    throw new RejectedEventException(AMOUNT_MISMATCH, "Payment " + event.amount() + " "
                            + event.text("currency") + " does not match order " + order.getOrderAmount() + " "
                            + order.getCurrency());
                }
                String paymentId = event.text("payment_id");
                if (ledgerRepository.existsByPaymentId(paymentId) && !ledgerRepository.existsByEventId(event.eventId())) {
                    throw new RejectedEventException(DUPLICATE_PAYMENT,
                            "payment_id " + paymentId + " was already charged by another event");
                }
            }
            case ORDER_SHIPPED -> requireStatus(order, event, OrderStatus.PAID);
            case ORDER_CANCELLED -> requireStatus(order, event, OrderStatus.CREATED, OrderStatus.PAID);
        }
    }

    private static void requireStatus(OrderProjection order, ValidatedEvent event, OrderStatus... allowed) {
        if (order == null) {
            throw new RejectedEventException(INVALID_TRANSITION, "Order " + event.entityId() + " does not exist");
        }
        for (OrderStatus s : allowed) {
            if (order.getStatus() == s) {
                return;
            }
        }
        throw new RejectedEventException(INVALID_TRANSITION,
                event.type().wireName() + " is not allowed when order is " + order.getStatus());
    }
}
