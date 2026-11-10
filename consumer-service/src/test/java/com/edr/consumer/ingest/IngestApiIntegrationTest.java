package com.edr.consumer.ingest;

import com.edr.consumer.AbstractMySqlTest;
import com.edr.consumer.inbox.InboxEvent;
import com.edr.consumer.inbox.InboxRepository;
import com.edr.consumer.inbox.InboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration test of POST /events against a throwaway MySQL container (see AbstractMySqlTest).
 * Each test also runs in a transaction that is rolled back.
 */
@AutoConfigureMockMvc
@Transactional
class IngestApiIntegrationTest extends AbstractMySqlTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private InboxRepository inboxRepository;

    @Autowired
    private JsonMapper jsonMapper;

    private Long firstInboxId(MvcResult result) throws Exception {
        return jsonMapper.readTree(result.getResponse().getContentAsString()).get("inboxIds").get(0).asLong();
    }

    @Test
    void singleEventIsStoredAsPending() throws Exception {
        String event = """
                {"event_id":"E101","entity_id":"ORD-1","version":1,"event_type":"OrderCreated",
                 "event_time":"2026-11-10T10:00:00Z",
                 "payload":{"customer_id":"CUST-1","amount":500.00,"currency":"INR"}}
                """;

        MvcResult result = mockMvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(event))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(1))
                .andReturn();

        Long id = firstInboxId(result);
        InboxEvent row = inboxRepository.findById(id).orElseThrow();
        assertEquals("E101", row.getEventId());
        assertEquals("ORD-1", row.getEntityId());
        assertEquals(1, row.getVersion());
        assertEquals(InboxStatus.PENDING, row.getStatus());
    }

    @Test
    void sampleDatasetStoresAllEightMessagesIncludingDuplicateAndPoison() throws Exception {
        String sample = Files.readString(Path.of("../simulator/samples/sample_events.json"));
        long before = inboxRepository.count();

        mockMvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(sample))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(8))
                .andExpect(jsonPath("$.inboxIds.length()").value(8));

        assertEquals(before + 8, inboxRepository.count());
    }

    @Test
    void malformedJsonIsAcceptedAndStoredRaw() throws Exception {
        String broken = "{\"event_id\": \"E400\", ";

        MvcResult result = mockMvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(broken))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(1))
                .andReturn();

        Long id = firstInboxId(result);
        InboxEvent row = inboxRepository.findById(id).orElseThrow();
        assertEquals(broken, row.getRawPayload());
        assertNull(row.getEventId());
        assertEquals(InboxStatus.PENDING, row.getStatus());
    }

    @Test
    void emptyBodyIsRejected() throws Exception {
        mockMvc.perform(post("/events").contentType(MediaType.APPLICATION_JSON).content(""))
                .andExpect(status().isBadRequest());
    }
}
