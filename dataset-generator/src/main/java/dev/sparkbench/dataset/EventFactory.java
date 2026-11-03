package dev.sparkbench.dataset;

import dev.sparkbench.events.*;

import java.util.SplittableRandom;

/** Generates meaningful synthetic properties for each concrete event class. */
public final class EventFactory {
    private static final String[] PROCESS_NAMES = {"agent", "browser", "worker", "indexer"};
    private static final String[] FILE_EXTENSIONS = {"log", "json", "csv", "dat"};

    private EventFactory() {}

    public static MachineEvent create(
            EventType type, EventMetadata metadata, SplittableRandom random) {
        String processName = PROCESS_NAMES[random.nextInt(PROCESS_NAMES.length)];
        ProcessContext process =
                new ProcessContext(
                        random.nextLong(1, 65536), processName, "/usr/bin/" + processName);

        return switch (type) {
            case DNS_REQUEST -> dnsRequest(metadata, process, random);
            case PROCESS_START ->
                    new ProcessStartEvent(
                            metadata,
                            process,
                            random.nextLong(1, 65536),
                            "launcher",
                            "/var/lib/agent",
                            process.executablePath() + " --mode=batch",
                            "medium");
            case PROCESS_END ->
                    new ProcessEndEvent(
                            metadata,
                            process,
                            random.nextInt(10) == 0 ? 1L : 0L,
                            random.nextLong(1, 86400000),
                            "normal_exit");
            case FILE_CREATED -> {
                String extension = extension(random);
                yield new FileCreatedEvent(
                        metadata,
                        process,
                        path(random, extension),
                        extension,
                        random.nextLong(0, 10485760),
                        sha256(random),
                        metadata.username(),
                        "0644");
            }
            case FILE_MODIFIED -> {
                String extension = extension(random);
                long previousSize = random.nextLong(0, 10485760);
                long bytesWritten = random.nextLong(1, 65536);
                yield new FileModifiedEvent(
                        metadata,
                        process,
                        path(random, extension),
                        extension,
                        previousSize,
                        previousSize + bytesWritten,
                        bytesWritten,
                        sha256(random),
                        "append");
            }
            case FILE_RENAMED -> {
                String extension = extension(random);
                String originalPath = path(random, extension);
                yield new FileRenamedEvent(
                        metadata,
                        process,
                        originalPath,
                        "/archive/" + originalPath.substring(6),
                        extension,
                        random.nextLong(0, 10485760),
                        false);
            }
            case FILE_DELETED -> {
                String extension = extension(random);
                yield new FileDeletedEvent(
                        metadata,
                        process,
                        path(random, extension),
                        extension,
                        random.nextLong(0, 10485760),
                        sha256(random),
                        "unlink");
            }
        };
    }

    private static DnsRequestEvent dnsRequest(
            EventMetadata metadata, ProcessContext process, SplittableRandom random) {
        boolean answered = random.nextInt(10) != 0;
        boolean ipv6 = random.nextBoolean();
        boolean cacheHit = answered && random.nextBoolean();
        String address = null;
        if (answered) {
            address =
                    ipv6
                            ? "2001:db8::" + Integer.toHexString(random.nextInt(1, 65536))
                            : "192.0.2." + random.nextInt(1, 255);
        }
        return new DnsRequestEvent(
                metadata,
                process,
                "service-" + random.nextInt(1000) + ".example.test",
                ipv6 ? "AAAA" : "A",
                answered ? "NOERROR" : "NXDOMAIN",
                address,
                "10.0.0.53",
                cacheHit ? 1 : random.nextLong(2, 251),
                "UDP",
                cacheHit);
    }

    private static String extension(SplittableRandom random) {
        return FILE_EXTENSIONS[random.nextInt(FILE_EXTENSIONS.length)];
    }

    private static String path(SplittableRandom random, String extension) {
        return "/data/file-" + random.nextLong(1, 1000000) + "." + extension;
    }

    private static String sha256(SplittableRandom random) {
        StringBuilder hash = new StringBuilder(64);
        for (int index = 0; index < 64; index++) {
            hash.append("0123456789abcdef".charAt(random.nextInt(16)));
        }
        return hash.toString();
    }
}
