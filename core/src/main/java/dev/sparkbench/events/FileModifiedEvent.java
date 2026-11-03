package dev.sparkbench.events;

import java.util.Map;

/** A file write with sizes before and after the write and its new hash. */
public record FileModifiedEvent(
        EventMetadata metadata,
        ProcessContext process,
        String path,
        String extension,
        long previousSizeBytes,
        long sizeBytes,
        long bytesWritten,
        String sha256,
        String writeOperation)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.FILE_MODIFIED;
    }

    @Override
    public Map<String, Object> eventFields() {
        return EventFields.of(
                "file_path", path,
                "file_extension", extension,
                "previous_file_size_bytes", previousSizeBytes,
                "file_size_bytes", sizeBytes,
                "bytes_written", bytesWritten,
                "file_sha256", sha256);
    }

    @Override
    public Map<String, String> attributes() {
        return Map.of("write_operation", writeOperation);
    }
}
