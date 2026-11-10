package com.edr.consumer.store;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LedgerRepository extends JpaRepository<LedgerEntry, Long> {

    boolean existsByEventId(String eventId);

    boolean existsByPaymentId(String paymentId);

    List<LedgerEntry> findByOrderId(String orderId);
}
