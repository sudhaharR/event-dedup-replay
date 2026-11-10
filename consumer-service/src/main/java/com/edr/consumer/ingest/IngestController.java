package com.edr.consumer.ingest;

import com.edr.consumer.inbox.EventSource;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /**
     * Accepts one event (JSON object), a batch (JSON array) or even a broken message.
     * The body is read as a raw String on purpose, so malformed JSON is stored instead of
     * being rejected by Spring. The consumer will dead-letter it with a reason in Stage 4.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingest(@RequestBody(required = false) String body) {
        return ingestService.ingest(body, EventSource.LIVE);
    }
}
