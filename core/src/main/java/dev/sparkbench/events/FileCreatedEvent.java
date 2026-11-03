package dev.sparkbench.events;

import java.util.Map;

/** Creation of a file with its initial size and content hash. */
public record FileCreatedEvent(
        EventMetadata metadata,
        ProcessContext process,
        String path,
        String extension,
        long sizeBytes,
        String sha256,
        String owner,
        String permissions)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.FILE_CREATED;
    }

    @Override
    public Map<String, Object> eventFields() {
        return EventFields.of(
                "file_path", path,
                "file_extension", extension,
                "file_size_bytes", sizeBytes,
                "file_sha256", sha256);
    }

    @Override
    public Map<String, String> attributes() {
        return Map.of(
                "file_owner", owner,
                "file_permissions", permissions);
    }
}
