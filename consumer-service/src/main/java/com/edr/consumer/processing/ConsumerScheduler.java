package com.edr.consumer.processing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Polls the inbox on a fixed delay - this is what makes the inbox table behave like a stream.
 * Disable with consumer.scheduler.enabled=false (the tests do this and call InboxConsumer directly).
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "consumer.scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class ConsumerScheduler {

    private static final Logger log = LoggerFactory.getLogger(ConsumerScheduler.class);

    private final InboxConsumer consumer;

    public ConsumerScheduler(InboxConsumer consumer) {
        this.consumer = consumer;
    }

    @Scheduled(fixedDelayString = "${consumer.poll-interval-ms:500}",
               initialDelayString = "${consumer.initial-delay-ms:2000}")
    public void poll() {
        try {
            int handled = consumer.runOnce();
            if (handled > 0) {
                log.info("Poll cycle handled {} message(s)", handled);
            }
        } catch (Exception e) {
            log.error("Poll cycle failed - will retry on next cycle", e);
        }
    }
}
