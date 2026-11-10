package com.edr.consumer.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface InboxRepository extends JpaRepository<InboxEvent, Long> {

    /** Pending messages in arrival order (used by the consumer in Stage 4). */
    List<InboxEvent> findTop100ByStatusOrderByIdAsc(InboxStatus status);

    long countByStatus(InboxStatus status);
}
