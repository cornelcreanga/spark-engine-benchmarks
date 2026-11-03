package dev.sparkbench.events;

import java.io.Serializable;
import java.util.Objects;

/** The process responsible for the DNS, process-lifecycle, or file event. */
public record ProcessContext(long processId, String processName, String executablePath)
        implements Serializable {
    public ProcessContext {
        if (processId <= 0) {
            throw new IllegalArgumentException("processId must be positive");
        }
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(executablePath, "executablePath");
    }
}
