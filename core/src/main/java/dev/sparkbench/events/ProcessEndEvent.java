package dev.sparkbench.events;

import java.util.Map;

/** A process exit, its elapsed lifetime, and termination cause. */
public record ProcessEndEvent(
        EventMetadata metadata,
        ProcessContext process,
        long exitCode,
        long durationMs,
        String terminationReason)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.PROCESS_END;
    }

    @Override
    public Map<String, Object> eventFields() {
        return EventFields.of(
                "exit_code", exitCode,
                "process_duration_ms", durationMs);
    }

    @Override
    public Map<String, String> attributes() {
        return Map.of("termination_reason", terminationReason);
    }
}
