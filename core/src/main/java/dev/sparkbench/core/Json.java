package dev.sparkbench.core;

import java.lang.reflect.Array;
import java.util.*;

public final class Json {
    private Json() {}

    public static String encode(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Map<?, ?> map) {
            StringJoiner j = new StringJoiner(",", "{", "}");
            map.forEach((k, v) -> j.add(encode(k.toString()) + ":" + encode(v)));
            return j.toString();
        }
        if (value instanceof Iterable<?> list) {
            StringJoiner j = new StringJoiner(",", "[", "]");
            list.forEach(v -> j.add(encode(v)));
            return j.toString();
        }
        if (value.getClass().isArray()) {
            List<Object> a = new ArrayList<>();
            for (int i = 0; i < Array.getLength(value); i++) {
                a.add(Array.get(value, i));
            }
            return encode(a);
        }
        StringBuilder s = new StringBuilder("\"");
        for (char c : value.toString().toCharArray()) {
            switch (c) {
                case '"' -> s.append("\\\"");
                case '\\' -> s.append("\\\\");
                case '\n' -> s.append("\\n");
                case '\r' -> s.append("\\r");
                case '\t' -> s.append("\\t");
                default -> {
                    if (c < 32) {
                        s.append(String.format("\\u%04x", (int) c));
                    } else {
                        s.append(c);
                    }
                }
            }
        }
        return s.append('"').toString();
    }
}
