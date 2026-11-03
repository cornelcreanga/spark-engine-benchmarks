package dev.sparkbench.events;

import java.util.Map;

/** A file moved from its original path to a new path. */
public record FileRenamedEvent(
        EventMetadata metadata,
        ProcessContext process,
        String sourcePath,
        String destinationPath,
        String extension,
        long sizeBytes,
        boolean overwriteExisting)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.FILE_RENAMED;
    }

    @Override
    public Map<String, Object> eventFields() {
        return EventFields.of(
                "file_path", sourcePath,
                "destination_path", destinationPath,
                "file_extension", extension,
                "file_size_bytes", sizeBytes);
    }

    @Override
    public Map<String, String> attributes() {
        return Map.of("overwrite_existing", Boolean.toString(overwriteExisting));
    }
}
