package com.edr.consumer.processing;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;

import static com.edr.consumer.processing.ErrorCode.INVALID_FIELD_TYPE;
import static com.edr.consumer.processing.ErrorCode.INVALID_PAYLOAD;
import static com.edr.consumer.processing.ErrorCode.INVALID_VERSION;
import static com.edr.consumer.processing.ErrorCode.MALFORMED_JSON;
import static com.edr.consumer.processing.ErrorCode.MISSING_FIELD;
import static com.edr.consumer.processing.ErrorCode.UNKNOWN_EVENT_TYPE;

/**
 * Phase A - structural validation of a raw message against the event contract
 * (docs/event-contract.md). Returns a ValidatedEvent or throws RejectedEventException.
 * The first problem found is reported.
 */
@Component
public class EventValidator {

    private static final List<String> REQUIRED =
            List.of("event_id", "entity_id", "version", "event_type", "event_time", "payload");
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final int MAX_ID_LENGTH = 64;
    private static final int MAX_TEXT_LENGTH = 255;

    private final JsonMapper jsonMapper;

    public EventValidator(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public ValidatedEvent validate(String raw) {
        JsonNode node = parse(raw);

        List<String> missing = REQUIRED.stream()
                .filter(f -> node.get(f) == null || node.get(f).isNull())
                .toList();
        if (!missing.isEmpty()) {
            throw new RejectedEventException(MISSING_FIELD, "Missing field(s): " + String.join(", ", missing));
        }

        String eventId = idField(node, "event_id");
        String entityId = idField(node, "entity_id");
        String typeName = idField(node, "event_type");

        JsonNode versionNode = node.get("version");
        if (!versionNode.isIntegralNumber() || !versionNode.canConvertToInt()) {
            throw new RejectedEventException(INVALID_FIELD_TYPE, "version must be an integer, got: " + versionNode);
        }
        int version = versionNode.intValue();

        Instant eventTime = timestamp(node.get("event_time"));

        JsonNode payload = node.get("payload");
        if (!payload.isObject()) {
            throw new RejectedEventException(INVALID_FIELD_TYPE, "payload must be a JSON object");
        }

        EventType type = EventType.fromWire(typeName)
                .orElseThrow(() -> new RejectedEventException(UNKNOWN_EVENT_TYPE, "Unknown event_type: " + typeName));

        checkVersion(type, version);
        checkPayload(type, payload);

        String hash = sha256(type.wireName() + "|" + entityId + "|" + version + "|" + eventTime + "|" + payload);
        return new ValidatedEvent(eventId, entityId, version, type, eventTime, payload, hash);
    }

    private JsonNode parse(String raw) {
        JsonNode node;
        try {
            node = jsonMapper.readTree(raw);
        } catch (JacksonException e) {
            throw new RejectedEventException(MALFORMED_JSON, "Not valid JSON: " + shorten(e.getOriginalMessage()));
        }
        if (node == null || !node.isObject()) {
            throw new RejectedEventException(MALFORMED_JSON, "Message is not a JSON object");
        }
        return node;
    }

    private static String idField(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (!value.isString() || value.stringValue().isBlank()) {
            throw new RejectedEventException(INVALID_FIELD_TYPE, field + " must be a non-empty string");
        }
        String s = value.stringValue().trim();
        if (s.length() > MAX_ID_LENGTH) {
            throw new RejectedEventException(INVALID_FIELD_TYPE, field + " is longer than " + MAX_ID_LENGTH);
        }
        return s;
    }

    private static Instant timestamp(JsonNode value) {
        if (!value.isString()) {
            throw new RejectedEventException(INVALID_FIELD_TYPE, "event_time must be an ISO-8601 string");
        }
        try {
            return OffsetDateTime.parse(value.stringValue()).toInstant();
        } catch (DateTimeParseException e) {
            throw new RejectedEventException(INVALID_FIELD_TYPE,
                    "event_time is not ISO-8601 (e.g. 2026-11-10T10:00:00Z): " + value.stringValue());
        }
    }

    private static void checkVersion(EventType type, int version) {
        if (version < 1) {
            throw new RejectedEventException(INVALID_VERSION, "version must be >= 1, got " + version);
        }
        if (type == EventType.ORDER_CREATED && version != 1) {
            throw new RejectedEventException(INVALID_VERSION, "OrderCreated must be version 1, got " + version);
        }
        if (type != EventType.ORDER_CREATED && version < 2) {
            throw new RejectedEventException(INVALID_VERSION, type.wireName() + " must be version >= 2, got " + version);
        }
    }

    private static void checkPayload(EventType type, JsonNode payload) {
        switch (type) {
            case ORDER_CREATED -> {
                payloadText(payload, "customer_id");
                payloadAmount(payload);
                payloadCurrency(payload);
            }
            case PAYMENT_CAPTURED -> {
                payloadText(payload, "payment_id");
                payloadAmount(payload);
                payloadCurrency(payload);
            }
            case ORDER_SHIPPED -> payloadText(payload, "tracking_id");
            case ORDER_CANCELLED -> payloadText(payload, "reason");
        }
    }

    private static void payloadText(JsonNode payload, String field) {
        JsonNode v = payload.get(field);
        if (v == null || !v.isString() || v.stringValue().isBlank()) {
            throw new RejectedEventException(INVALID_PAYLOAD, "payload." + field + " must be a non-empty string");
        }
        int max = field.equals("reason") ? MAX_TEXT_LENGTH : MAX_ID_LENGTH;
        if (v.stringValue().length() > max) {
            throw new RejectedEventException(INVALID_PAYLOAD, "payload." + field + " is longer than " + max);
        }
    }

    private static void payloadAmount(JsonNode payload) {
        JsonNode v = payload.get("amount");
        if (v == null || !v.isNumber()) {
            throw new RejectedEventException(INVALID_PAYLOAD, "payload.amount must be a number, got: " + v);
        }
        BigDecimal amount = v.decimalValue();
        if (amount.signum() <= 0 || amount.compareTo(new BigDecimal("9999999999.99")) > 0) {
            throw new RejectedEventException(INVALID_PAYLOAD, "payload.amount must be > 0, got: " + amount);
        }
    }

    private static void payloadCurrency(JsonNode payload) {
        JsonNode v = payload.get("currency");
        if (v == null || !v.isString() || !CURRENCY.matcher(v.stringValue()).matches()) {
            throw new RejectedEventException(INVALID_PAYLOAD, "payload.currency must be 3 capital letters, got: " + v);
        }
    }

    private static String sha256(String s) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String shorten(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }
}
