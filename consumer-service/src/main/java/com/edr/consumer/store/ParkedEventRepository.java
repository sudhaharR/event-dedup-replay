package com.edr.consumer.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface ParkedEventRepository extends JpaRepository<ParkedEvent, Long> {

    boolean existsByEventId(String eventId);

    boolean existsByEntityIdAndVersion(String entityId, int version);

    Optional<ParkedEvent> findByEntityIdAndVersion(String entityId, int version);

    Optional<ParkedEvent> findByInboxId(Long inboxId);

    /** Parked events whose predecessor has now been applied (version = last_version + 1). */
    @Query(value = """
            SELECT p.id FROM park_buffer p
            JOIN order_projection o ON o.order_id = p.entity_id AND p.version = o.last_version + 1
            ORDER BY p.id
            """, nativeQuery = true)
    List<Long> findReadyIds();
}
