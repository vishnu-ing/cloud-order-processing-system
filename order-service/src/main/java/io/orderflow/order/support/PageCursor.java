package io.orderflow.order.support;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import io.orderflow.common.event.EventJson;

/**
 * Opaque cursor over a DynamoDB {@code LastEvaluatedKey}.
 *
 * <p>Why cursors and not {@code page=3&size=20}: DynamoDB has no offset. Emulating one means
 * re-reading and discarding every preceding item, so page 50 costs 50x page 1 and the cost is paid
 * in RCUs. A cursor is O(1) regardless of depth and is also stable under concurrent inserts — a new
 * order arriving mid-scroll cannot shift rows into or out of a page the client already saw.
 *
 * <p>The encoding keeps a one-character type tag per attribute so numeric sort keys round-trip
 * correctly, then base64url-encodes the JSON. Clients must treat it as opaque; nothing about the
 * format is part of the API contract.
 */
public final class PageCursor {

    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {
    };

    private PageCursor() {
    }

    public static String encode(Map<String, AttributeValue> lastEvaluatedKey) {
        if (lastEvaluatedKey == null || lastEvaluatedKey.isEmpty()) {
            return null;
        }
        Map<String, String> tagged = new LinkedHashMap<>();
        lastEvaluatedKey.forEach((name, value) -> {
            if (value.s() != null) {
                tagged.put(name, "S:" + value.s());
            } else if (value.n() != null) {
                tagged.put(name, "N:" + value.n());
            }
        });
        try {
            byte[] json = EventJson.mapper().writeValueAsBytes(tagged);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to encode pagination cursor", e);
        }
    }

    /**
     * @throws IllegalArgumentException when the cursor is not one this service produced, which the
     *         exception handler maps to a 400 rather than a 500
     */
    public static Map<String, AttributeValue> decode(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return Map.of();
        }
        try {
            byte[] json = Base64.getUrlDecoder().decode(cursor);
            Map<String, String> tagged = EventJson.mapper()
                    .readValue(new String(json, StandardCharsets.UTF_8), MAP_TYPE);
            Map<String, AttributeValue> key = new LinkedHashMap<>();
            tagged.forEach((name, value) -> {
                if (value.startsWith("N:")) {
                    key.put(name, AttributeValue.fromN(value.substring(2)));
                } else if (value.startsWith("S:")) {
                    key.put(name, AttributeValue.fromS(value.substring(2)));
                } else {
                    throw new IllegalArgumentException("Untagged cursor attribute: " + name);
                }
            });
            return key;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Malformed pagination cursor", e);
        }
    }
}
