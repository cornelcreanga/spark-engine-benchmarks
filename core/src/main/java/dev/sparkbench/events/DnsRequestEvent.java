package dev.sparkbench.events;

import java.util.Map;

/** A DNS lookup and its response, including failed or unanswered lookups. */
public record DnsRequestEvent(
        EventMetadata metadata,
        ProcessContext process,
        String queryName,
        String queryType,
        String responseCode,
        String resolvedIp,
        String dnsServerIp,
        long durationMs,
        String transport,
        boolean cacheHit)
        implements MachineEvent {
    @Override
    public EventType type() {
        return EventType.DNS_REQUEST;
    }

    @Override
    public Map<String, Object> eventFields() {
        return EventFields.of(
                "dns_query_name", queryName,
                "dns_query_type", queryType,
                "dns_response_code", responseCode,
                "dns_resolved_ip", resolvedIp,
                "dns_server_ip", dnsServerIp,
                "dns_duration_ms", durationMs);
    }

    @Override
    public Map<String, String> attributes() {
        return Map.of("dns_transport", transport, "dns_cache_hit", Boolean.toString(cacheHit));
    }
}
