package com.edr.consumer.store;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface DeadLetterRepository extends JpaRepository<DeadLetter, Long> {

    Optional<DeadLetter> findByInboxId(Long inboxId);

    /** Dead letters of LIVE messages not yet archived to S3, oldest first (replay copies are not re-archived). */
    @Query(value = """
            SELECT d.* FROM dead_letter d JOIN inbox i ON i.id = d.inbox_id
            WHERE d.s3_key IS NULL AND i.source = 'LIVE'
            ORDER BY d.id LIMIT 200""", nativeQuery = true)
    List<DeadLetter> findArchivePending();
}
