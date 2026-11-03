package dev.sparkbench.events;

import java.util.Map;

/** A newly started process and the parent that launched it. */
public record ProcessStartEvent(
        EventMetadata metadata,
        ProcessContext process,
        long parentProcessId,
        String parentProcessName,
        String workingDirectory,
        String commandLine,
        String integrityLevel)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.PROCESS_START;
    }

    @Override
    public Map<String, Object> eventFields() {
        return EventFields.of(
                "parent_process_id", parentProcessId,
                "parent_process_name", parentProcessName,
                "working_directory", workingDirectory);
    }

    @Override
    public Map<String, String> attributes() {
        return Map.of(
                "command_line", commandLine,
                "integrity_level", integrityLevel);
    }
}
