package com.edr.consumer.web;

import com.edr.consumer.AbstractMySqlTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The dashboard origin may call the API; other origins may not. */
@AutoConfigureMockMvc
class CorsConfigTest extends AbstractMySqlTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void dashboardOriginIsAllowed() throws Exception {
        mockMvc.perform(options("/replay").header("Origin", "http://localhost:5173")
                        .header("Access-Control-Request-Method", "POST"))
                .andExpect(status().isOk())
                .andExpect(header().string("Access-Control-Allow-Origin", "http://localhost:5173"));
        mockMvc.perform(get("/metrics").header("Origin", "http://localhost:5173"))
                .andExpect(header().string("Access-Control-Allow-Origin", "http://localhost:5173"));
    }

    @Test
    void otherOriginsAreRejected() throws Exception {
        mockMvc.perform(get("/metrics").header("Origin", "http://evil.example"))
                .andExpect(status().isForbidden());
    }
}
