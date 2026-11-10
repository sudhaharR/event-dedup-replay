package com.edr.consumer.query;

import com.edr.consumer.AbstractMySqlTest;
import com.edr.consumer.inbox.EventSource;
import com.edr.consumer.ingest.IngestService;
import com.edr.consumer.processing.InboxConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The read API used by the simulator, and the dev-only admin endpoints. */
@AutoConfigureMockMvc
class QueryAndAdminApiTest extends AbstractMySqlTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private IngestService ingestService;

    @Autowired
    private InboxConsumer consumer;

    @BeforeEach
    void loadSample() throws Exception {
        ingestService.ingest(Files.readString(Path.of("../simulator/samples/sample_events.json")), EventSource.LIVE);
        consumer.drainAll();
    }

    @Test
    void ordersAndSingleOrder() throws Exception {
        mockMvc.perform(get("/orders")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].order_id").value("ORD-1"));
        mockMvc.perform(get("/orders/ORD-2")).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SHIPPED"))
                .andExpect(jsonPath("$.amount_paid").value(300.00))
                .andExpect(jsonPath("$.last_version").value(3));
        mockMvc.perform(get("/orders/NOPE")).andExpect(status().isNotFound());
    }

    @Test
    void metricsLedgerInboxDeadLetters() throws Exception {
        mockMvc.perform(get("/metrics")).andExpect(status().isOk())
                .andExpect(jsonPath("$.applied").value(6))
                .andExpect(jsonPath("$.duplicates").value(1))
                .andExpect(jsonPath("$.total_charges").value(2));
        mockMvc.perform(get("/ledger").param("order_id", "ORD-1"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].event_id").value("E102"));
        mockMvc.perform(get("/inbox").param("event_id", "E102"))
                .andExpect(jsonPath("$.length()").value(2));
        mockMvc.perform(get("/dead-letters"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].error_code").value("MISSING_FIELD"));
    }

    @Test
    void adminArmsFaultAndResets() throws Exception {
        mockMvc.perform(post("/admin/faults/fail-once").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"event_ids\":[\"E-NEVER-SENT\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.armed[0]").value("E-NEVER-SENT"));

        mockMvc.perform(post("/admin/reset")).andExpect(status().isOk());

        mockMvc.perform(get("/orders")).andExpect(jsonPath("$.length()").value(0));
        mockMvc.perform(get("/metrics")).andExpect(jsonPath("$.total_received").value(0));
    }
}
