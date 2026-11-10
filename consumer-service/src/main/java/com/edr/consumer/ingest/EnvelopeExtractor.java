package com.edr.consumer.ingest;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a request body into individual messages and reads the envelope fields best-effort.
 *
 * Rules:
 *  - JSON array  -> one message per element
 *  - JSON object -> one message
 *  - anything else (broken JSON, plain text, numbers) -> one message, all fields null
 *
 * It never rejects a message: validation happens in the consumer (Stage 4),
 * so even poison messages are stored and can be dead-lettered with a reason.
 */
@Component
public class EnvelopeExtractor {

    // Must match the column sizes in db/init/01_schema.sql
    private static final int MAX_ID_LENGTH = 64;
    private static final int MAX_TYPE_LENGTH = 40;

    private final JsonMapper jsonMapper;

    public EnvelopeExtractor(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public List<ParsedMessage> split(String body) {
        JsonNode root;
        try {
            root = jsonMapper.readTree(body);
        } catch (JacksonException e) {
            return List.of(ParsedMessage.unreadable(body));
        }

        List<ParsedMessage> messages = new ArrayList<>();
        if (root != null && root.isArray()) {
            for (JsonNode element : root) {
                messages.add(extract(element));
            }
        } else if (root != null && root.isObject()) {
            messages.add(extract(root));
        } else {
            messages.add(ParsedMessage.unreadable(body));
        }
        return messages;
    }

    private ParsedMessage extract(JsonNode node) {
        String raw = node.toString();
        if (!node.isObject()) {
            return ParsedMessage.unreadable(raw);
        }
        return new ParsedMessage(
                raw,
                text(node, "event_id", MAX_ID_LENGTH),
                text(node, "entity_id", MAX_ID_LENGTH),
                integer(node, "version"),
                text(node, "event_type", MAX_TYPE_LENGTH),
                timestamp(node, "event_time"));
    }

    private static String text(JsonNode node, String field, int maxLength) {
        JsonNode value = node.get(field);
        if (value == null || !value.isString()) {
            return null;
        }
        String s = value.stringValue().trim();
        return (s.isEmpty() || s.length() > maxLength) ? null : s;
    }

    private static Integer integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
            return null;
        }
        return value.intValue();
    }

    private static Instant timestamp(JsonNode node, String field) {
        String s = text(node, field, 64);
        if (s == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(s).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
