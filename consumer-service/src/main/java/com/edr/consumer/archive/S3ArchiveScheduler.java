package com.edr.consumer.archive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/** Runs the S3 archiver on a fixed delay (only when S3 and the schedulers are enabled). */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = {"s3.enabled", "consumer.scheduler.enabled"}, havingValue = "true")
public class S3ArchiveScheduler {

    private static final Logger log = LoggerFactory.getLogger(S3ArchiveScheduler.class);

    private final S3Archiver archiver;

    public S3ArchiveScheduler(S3Archiver archiver) {
        this.archiver = archiver;
    }

    @Scheduled(fixedDelayString = "${s3.archive-interval-ms:1000}", initialDelayString = "3000")
    public void archive() {
        try {
            archiver.archiveOnce();
        } catch (Exception e) {
            log.error("Archive pass failed - will retry", e);
        }
    }
}
