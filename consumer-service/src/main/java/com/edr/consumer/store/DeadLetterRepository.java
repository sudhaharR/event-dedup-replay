package com.edr.consumer.store;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface DeadLetterRepository extends JpaRepository<DeadLetter, Long> {

    Optional<DeadLetter> findByInboxId(Long inboxId);

    /** Dead letters not yet archived to S3, oldest first. */
    List<DeadLetter> findTop200ByS3KeyIsNullOrderByIdAsc();

    long countByS3KeyIsNull();
}
