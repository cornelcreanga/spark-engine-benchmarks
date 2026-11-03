package dev.sparkbench.events;

import java.util.LinkedHashMap;
import java.util.Map;

final class EventFields {
    private EventFields() {}

    /** Unlike Map.of, permits null values such as an unanswered DNS lookup. */
    static Map<String, Object> of(Object... entries) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            fields.put((String) entries[index], entries[index + 1]);
        }
        return fields;
    }
}
