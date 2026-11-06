package dev.sparkbench.dataset;

import dev.sparkbench.core.Config;
import dev.sparkbench.events.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructField;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;

public final class EventsSelfTest {
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static void main(String[] args) throws Exception {
        Config base = Config.load(Path.of("config/local.properties"));
        verifyTopLevelOnly(base);
        verifyUuidStorage();
        verifyConcreteEvents(base);
        verifyStableSystemIds(base);
        verifyIndependentPayloadStreams(base);
        for (int buckets : new int[] {4, 16, 256}) {
            verifyDistribution(base, buckets);
        }
        System.out.println(
                "Dataset self-test passed: seven typed events, nullable fields, Spark encoding, "
                        + "4/16/256 buckets, payload bytes, deterministic events, UUIDs, skew");
    }

    private static void verifyTopLevelOnly(Config base) throws Exception {
        Config simple = base.with("dataset.includeAttributes", "false");
        Events fullEvents = new Events(base), simpleEvents = new Events(simple);
        var serializer =
                ExpressionEncoder.apply(EventTable.generationSchema(simple)).createSerializer();
        check(EventTable.schema(simple).length() == 29, "29 scalar event columns");
        for (StructField field : EventTable.schema(simple).fields()) {
            check(!field.name().equals("attributes"), "map is absent");
        }
        for (long index = 0; index < 1000; index++) {
            Row full = fullEvents.row(index), scalar = simpleEvents.row(index);
            serializer.apply(scalar);
            for (int column = 0; column < 29; column++) {
                check(
                        Objects.equals(full.get(column), scalar.get(column)),
                        "scalar values match map-bearing events");
            }
            check(Objects.equals(full.get(30), scalar.get(29)), "same Iceberg bucket");
        }
        check(
                !Generator.inputFingerprint(base).equals(Generator.inputFingerprint(simple)),
                "schema variants have different input contracts");
        check(
                EventTable.schema(simple).apply("message").dataType().typeName().equals("string"),
                "simple table retains message");
        check(!Generator.createTableSql(simple).contains("attributes"), "simple DDL excludes map");
    }

    private static void verifyUuidStorage() {
        UUID customer = UUID.fromString("00112233-4455-4677-8899-aabbccddeeff");
        UUID system = UUID.fromString("fedcba98-7654-4321-89ab-cdef01234567");
        EventMetadata metadata =
                new EventMetadata(
                        customer,
                        Instant.EPOCH,
                        customer.toString(),
                        system.toString(),
                        "host",
                        "user");
        EventMetadata equal =
                new EventMetadata(
                        customer,
                        Instant.EPOCH,
                        customer.toString().toUpperCase(),
                        system.toString().toUpperCase(),
                        "host",
                        "user");
        check(
                metadata.equals(equal) && metadata.hashCode() == equal.hashCode(),
                "canonical UUID string value equality");
        check(metadata.customerId().equals(customer.toString()), "canonical customer UUID");
        check(metadata.systemId().equals(system.toString()), "canonical system UUID");
        try {
            new EventMetadata(
                    customer, Instant.EPOCH, "invalid", system.toString(), "host", "user");
            throw new AssertionError("accepted an invalid UUID string");
        } catch (IllegalArgumentException expected) {
        }
    }

    private static void verifyStableSystemIds(Config config) {
        Events events = new Events(config);
        Events repeated = new Events(config);
        Map<UUID, Map<String, UUID>> systems = new HashMap<>();
        Map<UUID, UUID> owners = new HashMap<>();
        for (int index = 0; index < 5000; index++) {
            EventMetadata metadata = events.event(index).metadata();
            check(metadata.equals(repeated.event(index).metadata()), "stable UUID generation");
            UUID customer = UUID.fromString(metadata.customerId());
            UUID system = UUID.fromString(metadata.systemId());
            check(customer.version() == 4 && customer.variant() == 2, "customer UUID format");
            check(system.version() == 4 && system.variant() == 2, "system UUID format");
            UUID previous =
                    systems.computeIfAbsent(customer, ignored -> new HashMap<>())
                            .putIfAbsent(metadata.hostname(), system);
            check(
                    previous == null || previous.equals(system),
                    "same machine retains its system UUID");
            UUID owner = owners.putIfAbsent(system, customer);
            check(owner == null || owner.equals(customer), "system UUIDs are customer-specific");
        }
        for (Map<String, UUID> customerSystems : systems.values()) {
            check(
                    customerSystems.size() <= config.integer("dataset.systemsPerCustomer"),
                    "configured system count");
            check(
                    new HashSet<>(customerSystems.values()).size() == customerSystems.size(),
                    "distinct machine UUIDs");
        }
    }

    private static boolean rowsEqual(Row left, Row right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int column = 0; column < left.size(); column++) {
            if (!Objects.deepEquals(left.get(column), right.get(column))) {
                return false;
            }
        }
        return true;
    }

    private static void verifyIndependentPayloadStreams(Config config) {
        Events events = new Events(config);
        Set<String> prefixes = new HashSet<>();
        Set<String> shiftedPrefixes = new HashSet<>();
        int messageColumn = EventTable.SCHEMA.fieldIndex("message");
        for (int index = 0; index < 20000; index++) {
            String message = events.row(index).getString(messageColumn);
            check(prefixes.add(message.substring(0, 128)), "duplicate random padding prefix");
            shiftedPrefixes.add(message.substring(1, 129));
        }
        prefixes.retainAll(shiftedPrefixes);
        check(prefixes.isEmpty(), "random padding streams overlap after a one-character shift");
        Events anotherSeed = new Events(config.with("dataset.seed", "43"));
        check(!events.row(0).equals(anotherSeed.row(0)), "configured seed changes generated rows");
    }

    private static void verifyConcreteEvents(Config config) {
        EventMetadata metadata =
                new EventMetadata(
                        UUID.fromString("90fe3a45-2474-42c1-8199-b1c260702db7"),
                        Instant.parse(config.get("dataset.hour")),
                        "00112233-4455-4677-8899-aabbccddeeff",
                        "fedcba98-7654-4321-89ab-cdef01234567",
                        "test-host",
                        "usér");
        var serializer = ExpressionEncoder.apply(Events.SCHEMA).createSerializer();
        Set<Class<?>> classes = new HashSet<>();
        for (EventType type : EventType.values()) {
            MachineEvent event = EventFactory.create(type, metadata, new SplittableRandom(42));
            classes.add(event.getClass());
            check(event.type() == type, "event discriminator");
            check(event.metadata().equals(metadata), "shared metadata");
            Row row = EventTable.row(event, config, new SplittableRandom(71), 0);
            serializer.apply(row);
            verifyPayload(row, 768, 256);
            verifyMappedProperties(event, row);
            verifyMeaningfulProperties(event);
        }
        check(classes.size() == 7, "one concrete class per event type");

        DnsRequestEvent unanswered =
                new DnsRequestEvent(
                        metadata,
                        new ProcessContext(12, "browser", "/usr/bin/browser"),
                        "missing.example.test",
                        "A",
                        "NXDOMAIN",
                        null,
                        "10.0.0.53",
                        8,
                        "UDP",
                        false);
        Row row = EventTable.row(unanswered, config, new SplittableRandom(1), 0);
        serializer.apply(row);
        check(row.isNullAt(EventTable.SCHEMA.fieldIndex("dns_resolved_ip")), "unanswered DNS");
        verifyPayload(row, 768, 256);
    }

    private static void verifyMappedProperties(MachineEvent event, Row row) {
        Map<String, Object> fields = event.topLevelFields();
        for (StructField field : EventTable.SCHEMA.fields()) {
            if (field.name().equals("message") || field.name().equals("attributes")) {
                continue;
            }
            int index = EventTable.SCHEMA.fieldIndex(field.name());
            Object actual = row.get(index);
            if (actual instanceof java.sql.Timestamp timestamp) {
                actual = timestamp.toInstant();
            }
            check(
                    Objects.deepEquals(actual, fields.get(field.name())),
                    event.type() + " field " + field.name());
        }
        Map<String, String> attributes = row.getJavaMap(EventTable.SCHEMA.fieldIndex("attributes"));
        event.attributes().forEach((key, value) -> check(value.equals(attributes.get(key)), key));
    }

    private static void verifyMeaningfulProperties(MachineEvent event) {
        check(event.process().processId() > 0, "originating process");
        check(
                event.process().executablePath().endsWith(event.process().processName()),
                "executable");
        if (event instanceof DnsRequestEvent dns) {
            check(dns.queryName().endsWith(".example.test"), "DNS name");
            check(dns.durationMs() >= 0, "DNS duration");
            check((dns.resolvedIp() != null) == dns.responseCode().equals("NOERROR"), "DNS answer");
        } else if (event instanceof ProcessStartEvent start) {
            check(start.parentProcessId() > 0, "parent process");
            check(start.commandLine().startsWith(start.process().executablePath()), "command line");
        } else if (event instanceof ProcessEndEvent end) {
            check(end.durationMs() > 0, "process lifetime");
        } else if (event instanceof FileCreatedEvent created) {
            check(created.path().endsWith("." + created.extension()), "created file extension");
            check(created.sha256().matches("[0-9a-f]{64}"), "created file hash");
        } else if (event instanceof FileModifiedEvent modified) {
            check(
                    modified.sizeBytes() == modified.previousSizeBytes() + modified.bytesWritten(),
                    "append size accounting");
            check(modified.sha256().matches("[0-9a-f]{64}"), "modified file hash");
        } else if (event instanceof FileRenamedEvent renamed) {
            check(!renamed.sourcePath().equals(renamed.destinationPath()), "rename destination");
        } else if (event instanceof FileDeletedEvent deleted) {
            check(deleted.sizeBytes() >= 0, "last known file size");
            check(deleted.sha256().matches("[0-9a-f]{64}"), "deleted file hash");
        }
    }

    private static void verifyDistribution(Config base, int buckets) throws Exception {
        Path properties = Files.createTempFile("events-test-", ".properties");
        Config config;
        try {
            Files.writeString(
                    properties, "dataset.buckets=" + buckets + "\ndataset.customers=4096\n");
            config = base.overlay(Config.load(properties));
        } finally {
            Files.delete(properties);
        }
        Events events = new Events(config);
        long[] counts = new long[buckets];
        Set<String> types = new HashSet<>();
        Instant start = Instant.parse(config.get("dataset.hour"));
        int samples = buckets == 256 ? 60000 : 20000;
        for (int index = 0; index < samples; index++) {
            Row row = events.row(index);
            int bucket = row.getInt(Events.SCHEMA.fieldIndex("_bucket"));
            String customerId = row.getString(EventTable.SCHEMA.fieldIndex("cid"));
            String systemId = row.getString(EventTable.SCHEMA.fieldIndex("sid"));
            check(
                    UUID.fromString(customerId).toString().equals(customerId)
                            && UUID.fromString(systemId).toString().equals(systemId),
                    "canonical UUID string columns");
            int actualIcebergBucket =
                    org.apache.iceberg.transforms.Transforms.<String>bucket(
                                    org.apache.iceberg.types.Types.StringType.get(), buckets)
                            .apply(customerId);
            check(actualIcebergBucket == bucket, "Iceberg string transform compatibility");
            counts[bucket]++;
            types.add(row.getString(EventTable.SCHEMA.fieldIndex("eventtype")));
            check(
                    Events.bucket(row.getAs(EventTable.SCHEMA.fieldIndex("cid")), buckets)
                            == bucket,
                    "actual Iceberg bucket mismatch");
            check(
                    UUID.fromString(row.getString(EventTable.SCHEMA.fieldIndex("eventid")))
                                    .version()
                            == 4,
                    "UUID version");
            Instant time = row.getTimestamp(EventTable.SCHEMA.fieldIndex("eventtime")).toInstant();
            check(
                    !time.isBefore(start) && time.isBefore(start.plusSeconds(3600)),
                    "one-hour interval");
            if (index < 100) {
                verifyPayload(row, 768, 256);
                check(rowsEqual(row, events.row(index)), "deterministic generation");
            }
        }
        check(types.size() == 7, "all event types");
        int band = Math.max(1, buckets / 4);
        double low = 0;
        double high = 0;
        for (int index = 0; index < band; index++) {
            low += counts[index];
            high += counts[buckets - 1 - index];
        }
        check(high / low > 1.8 && high / low < 3.6, "bucket skew " + high / low);
    }

    private static void verifyPayload(Row row, int expectedTop, int expectedMap) {
        int top = 0;
        for (int index = 0; index < EventTable.SCHEMA.length(); index++) {
            if (EventTable.SCHEMA.fields()[index].name().equals("attributes")
                    || row.isNullAt(index)) {
                continue;
            }
            Object value = row.get(index);
            if (value instanceof java.sql.Timestamp timestamp) {
                value = timestamp.toInstant();
            }
            top +=
                    value instanceof byte[] bytes
                            ? bytes.length
                            : value.toString().getBytes(StandardCharsets.UTF_8).length;
        }
        Map<String, String> attributes = row.getJavaMap(EventTable.SCHEMA.fieldIndex("attributes"));
        int map =
                attributes.values().stream()
                        .mapToInt(value -> value.getBytes(StandardCharsets.UTF_8).length)
                        .sum();
        check(top == expectedTop && map == expectedMap, "payload split: " + top + "/" + map);
    }
}
