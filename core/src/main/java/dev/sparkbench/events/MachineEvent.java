package dev.sparkbench.events;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** A typed event with an explicit mapping to the shared Iceberg table. */
public sealed interface MachineEvent extends Serializable
        permits DnsRequestEvent,
                ProcessStartEvent,
                ProcessEndEvent,
                FileCreatedEvent,
                FileModifiedEvent,
                FileRenamedEvent,
                FileDeletedEvent {
    EventMetadata metadata();

    ProcessContext process();

    EventType type();

    /** Only properties specific to this event type; other table fields remain null. */
    Map<String, Object> eventFields();

    /** Less frequently queried properties stored as strings in the attributes map. */
    Map<String, String> attributes();

    default Map<String, Object> topLevelFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("eventid", metadata().eventId().toString());
        fields.put("eventtime", metadata().eventTime());
        fields.put("cid", metadata().customerId());
        fields.put("sid", metadata().systemId());
        fields.put("eventtype", type().tableValue());
        fields.put("hostname", metadata().hostname());
        fields.put("username", metadata().username());
        fields.put("process_id", process().processId());
        fields.put("process_name", process().processName());
        fields.put("executable_path", process().executablePath());
        fields.putAll(eventFields());
        return fields;
    }
}
