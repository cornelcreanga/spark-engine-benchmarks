package dev.sparkbench.events;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Fields identifying an event and the machine that reported it. IDs use canonical UUID strings. */
public record EventMetadata(
        UUID eventId,
        Instant eventTime,
        String customerId,
        String systemId,
        String hostname,
        String username)
        implements Serializable {
    public EventMetadata {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventTime, "eventTime");
        customerId = UUID.fromString(customerId).toString();
        systemId = UUID.fromString(systemId).toString();
        Objects.requireNonNull(hostname, "hostname");
        Objects.requireNonNull(username, "username");
    }
}
