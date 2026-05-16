package io.github.lyonbrown4d.nespa;

import io.github.lyonbrown4d.nespa.internal.Frame;
import java.nio.charset.StandardCharsets;

public class CacheException extends java.io.IOException {
    private final int code;

    public CacheException(int code, String message) {
        super(message == null || message.isBlank() ? "cache tcp error: code " + code : message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static CacheException fromFrame(Frame frame) {
        String body = new String(frame.getMetadata(), StandardCharsets.UTF_8);
        return new CacheException(parseIntField(body, "code"), parseStringField(body, "message"));
    }

    private static int parseIntField(String json, String name) {
        String marker = "\"" + name + "\":";
        int start = json.indexOf(marker);
        if (start < 0) {
            return 0;
        }
        int valueStart = start + marker.length();
        int valueEnd = valueStart;
        while (valueEnd < json.length() && Character.isDigit(json.charAt(valueEnd))) {
            valueEnd++;
        }
        if (valueEnd == valueStart) {
            return 0;
        }
        return Integer.parseInt(json.substring(valueStart, valueEnd));
    }

    private static String parseStringField(String json, String name) {
        String marker = "\"" + name + "\":\"";
        int start = json.indexOf(marker);
        if (start < 0) {
            return "";
        }
        int valueStart = start + marker.length();
        StringBuilder value = new StringBuilder();
        boolean escaped = false;
        for (int index = valueStart; index < json.length(); index++) {
            char next = json.charAt(index);
            if (escaped) {
                value.append(next);
                escaped = false;
                continue;
            }
            if (next == '\\') {
                escaped = true;
                continue;
            }
            if (next == '"') {
                break;
            }
            value.append(next);
        }
        return value.toString();
    }
}
