package dev.sparkbench.results;

import dev.sparkbench.core.Json;

import java.util.*;

public final class Report {
    private final Map<String, Object> data = new LinkedHashMap<>();

    public Report put(String key, Object value) {
        data.put(key, value);
        return this;
    }

    public void emit() {
        emit("BENCH_RESULT");
    }

    public void emit(String marker) {
        System.out.println(marker + " " + Json.encode(data));
    }
}
