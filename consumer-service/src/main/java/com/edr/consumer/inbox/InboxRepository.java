package com.edr.consumer.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface InboxRepository extends JpaRepository<InboxEvent, Long> {

    /** Pending messages in arrival order (used by the consumer in Stage 4). */
    List<InboxEvent> findTop100ByStatusOrderByIdAsc(InboxStatus status);

    long countByStatus(InboxStatus status);

    /** Messages not yet copied to S3 (the archive backlog), oldest first. */
    List<InboxEvent> findTop200ByS3KeyIsNullOrderByIdAsc();

    long countByS3KeyIsNull();

    /** First message of a source, e.g. the oldest LIVE message (start of replay's default window). */
    Optional<InboxEvent> findFirstBySourceOrderByIdAsc(EventSource source);
}
