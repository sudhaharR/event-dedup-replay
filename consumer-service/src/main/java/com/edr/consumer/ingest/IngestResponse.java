package com.edr.consumer.ingest;

import java.util.List;

/** Returned by POST /events: how many messages were stored and their inbox ids (arrival order). */
public record IngestResponse(int accepted, List<Long> inboxIds) {
}
