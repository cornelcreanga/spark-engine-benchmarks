package dev.sparkbench.events;

import java.util.Map;

/** Deletion of a file, retaining its last known size and content hash. */
public record FileDeletedEvent(
        EventMetadata metadata,
        ProcessContext process,
        String path,
        String extension,
        long sizeBytes,
        String sha256,
        String deletionMethod)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.FILE_DELETED;
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
        return Map.of("deletion_method", deletionMethod);
    }
}
