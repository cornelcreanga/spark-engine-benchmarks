package dev.sparkbench.core;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

public final class Config implements Serializable {
    private final TreeMap<String, String> values = new TreeMap<>();

    public static Config load(Path path) throws IOException {
        return load(path, new HashSet<>());
    }

    private static Config load(Path path, Set<Path> seen) throws IOException {
        path = path.toAbsolutePath().normalize();
        if (!seen.add(path)) {
            throw new IllegalArgumentException("Cyclic config inheritance: " + path);
        }
        Properties p = new Properties();
        try (Reader in = Files.newBufferedReader(path)) {
            p.load(in);
        }
        Config c =
                p.containsKey("config.parent")
                        ? load(path.resolveSibling(p.getProperty("config.parent")), seen)
                        : new Config();
        if (p.containsKey("config.includes")) {
            for (String include : p.getProperty("config.includes").split(",")) {
                c = c.overlay(load(path.resolveSibling(include.trim()), seen));
            }
        }
        Config merged = c;
        p.forEach(
                (k, v) -> {
                    if (!k.equals("config.parent") && !k.equals("config.includes")) {
                        merged.values.put(k.toString(), v.toString().trim());
                    }
                });
        seen.remove(path);
        return merged;
    }

    public Config overlay(Config other) {
        Config c = new Config();
        c.values.putAll(values);
        c.values.putAll(other.values);
        return c;
    }

    public Config with(String key, String value) {
        Config copy = new Config();
        copy.values.putAll(values);
        copy.values.put(key, value);
        return copy;
    }

    public String get(String key) {
        String s = values.get(key);
        if (s == null || s.isBlank()) {
            throw new IllegalArgumentException("Missing config: " + key);
        }
        return s;
    }

    public String get(String key, String fallback) {
        return values.getOrDefault(key, fallback);
    }

    public long number(String key) {
        return Long.parseLong(get(key));
    }

    public int integer(String key) {
        return Math.toIntExact(number(key));
    }

    public double decimal(String key) {
        return Double.parseDouble(get(key));
    }

    public boolean flag(String key) {
        return Boolean.parseBoolean(get(key, "false"));
    }

    public SortedMap<String, String> all() {
        return Collections.unmodifiableSortedMap(values);
    }

    public SortedMap<String, String> prefix(String prefix) {
        SortedMap<String, String> out = new TreeMap<>();
        values.forEach(
                (k, v) -> {
                    if (k.startsWith(prefix)) {
                        out.put(k.substring(prefix.length()), v);
                    }
                });
        return out;
    }

    public void store(Path path) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Properties p = new Properties();
        p.putAll(values);
        try (Writer out = Files.newBufferedWriter(path)) {
            p.store(out, "Spark benchmark effective configuration");
        }
    }

    public String table() {
        return identifier(get("catalog.name"))
                + "."
                + identifier(get("catalog.namespace"))
                + "."
                + identifier(get("catalog.table"));
    }

    public String shortTable() {
        return get("catalog.namespace") + "." + get("catalog.table");
    }

    public static String identifier(String s) {
        if (!s.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + s);
        }
        return s;
    }

    public static String literal(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    public double[] weights(String key, int count) {
        String[] items = get(key).split(",");
        if (items.length != count) {
            throw new IllegalArgumentException(key + " needs " + count + " weights");
        }
        double[] weights = Arrays.stream(items).mapToDouble(Double::parseDouble).toArray();
        if (Arrays.stream(weights).anyMatch(w -> !Double.isFinite(w) || w <= 0)) {
            throw new IllegalArgumentException(key + " weights must be positive");
        }
        return weights;
    }

    public boolean filesystemStorage() {
        return get("storage.backend", "filesystem").equals("filesystem");
    }

    public boolean validationEnabled() {
        return Boolean.parseBoolean(get("validation.enabled", "true"));
    }

    public void validate() {
        table();
        if (!Set.of("s3", "filesystem").contains(get("storage.backend", "filesystem"))) {
            throw new IllegalArgumentException("storage.backend must be s3 or filesystem");
        }
        if (filesystemStorage()) {
            java.net.URI warehouse = java.net.URI.create(get("catalog.warehouse"));
            Path mount = Path.of(get("filesystem.mountPath")).normalize();
            if (!"file".equals(warehouse.getScheme())
                    || !mount.isAbsolute()
                    || !Path.of(warehouse).normalize().startsWith(mount)) {
                throw new IllegalArgumentException(
                        "Filesystem warehouse must be a file URI under filesystem.mountPath");
            }
            if (!get("filesystem.claim").matches("[a-z][a-z0-9-]{0,62}")) {
                throw new IllegalArgumentException("Invalid filesystem.claim");
            }
            if (flag("validation.trino.enabled")) {
                throw new IllegalArgumentException(
                        "Filesystem mode currently supports the OSS validator only; disable Trino");
            }
        }
        if (!Set.of("none", "warm").contains(get("benchmark.cacheMode", "none"))) {
            throw new IllegalArgumentException("benchmark.cacheMode must be none or warm");
        }
        if (get("benchmark.cacheMode", "none").equals("warm")) {
            double fraction = decimal("benchmark.warmCacheMinResidentFraction");
            if (!filesystemStorage()
                    || number("benchmark.warmCacheTimeoutSeconds") <= 0
                    || !Double.isFinite(fraction)
                    || fraction <= 0
                    || fraction > 1) {
                throw new IllegalArgumentException(
                        "Warm-cache mode requires local filesystem, a timeout, and a residency"
                                + " fraction in (0,1]");
            }
        }
        if (flag("benchmark.discardOutputs") && (!filesystemStorage() || validationEnabled())) {
            throw new IllegalArgumentException(
                    "Discarding current-run outputs requires filesystem mode and disabled data"
                            + " validation");
        }
        if (!Set.of("spark", "quanton").contains(get("engine.operator", "spark"))) {
            throw new IllegalArgumentException("engine.operator must be spark or quanton");
        }
        if (get("engine.operator", "spark").equals("quanton") && !filesystemStorage()) {
            throw new IllegalArgumentException(
                    "Quanton currently requires a shared filesystem profile");
        }
        if (!Set.of("sort", "binpack").contains(get("benchmark.compactionStrategy", "sort"))) {
            throw new IllegalArgumentException(
                    "benchmark.compactionStrategy must be sort or binpack");
        }
        if (!Set.of("true", "false").contains(get("validation.enabled", "true"))) {
            throw new IllegalArgumentException("validation.enabled must be true or false");
        }
        if (values.containsKey("image.icebergVersion")
                && !(get("engine.operator", "spark").equals("quanton")
                        && get("image.icebergVersion").equals("bundled"))
                && !Set.of("1.11.0", "1.12.0").contains(get("image.icebergVersion"))) {
            throw new IllegalArgumentException("Supported Iceberg runtimes: 1.11.0 and 1.12.0");
        }
        if (values.containsKey("image.sparkLine")
                && !get("spark.version").startsWith(get("image.sparkLine") + ".")) {
            throw new IllegalArgumentException("spark.version and image.sparkLine must agree");
        }
        for (String k :
                List.of(
                        "dataset.targetBytes",
                        "dataset.fileBytes",
                        "dataset.payloadBytes",
                        "dataset.buckets",
                        "dataset.customers",
                        "dataset.systemsPerCustomer",
                        "dataset.partitions",
                        "dataset.calibrationRows",
                        "dataset.maxBatchRows",
                        "dataset.maxBatches",
                        "spark.driver.cores",
                        "spark.executor.instances",
                        "spark.executor.cores",
                        "spark.timeoutSeconds",
                        "benchmark.repetitions")) {
            if (number(k) <= 0) {
                throw new IllegalArgumentException(k + " must be positive");
            }
        }
        if (values.containsKey("dataset.minBatches") && number("dataset.minBatches") <= 0) {
            throw new IllegalArgumentException("dataset.minBatches must be positive");
        }
        if (values.containsKey("dataset.rows") && number("dataset.rows") <= 0) {
            throw new IllegalArgumentException("dataset.rows must be positive");
        }
        if (!Set.of("true", "false").contains(get("dataset.includeAttributes", "true"))) {
            throw new IllegalArgumentException("dataset.includeAttributes must be true or false");
        }
        if (integer("dataset.customers") < integer("dataset.buckets")) {
            throw new IllegalArgumentException("Need at least one customer per bucket");
        }
        if (integer("dataset.payloadBytes") < 512) {
            throw new IllegalArgumentException("payloadBytes must be >= 512");
        }
        double fraction = decimal("dataset.topLevelFraction");
        if (!Double.isFinite(fraction) || fraction < 0.5 || fraction > 0.9) {
            throw new IllegalArgumentException("topLevelFraction must be in [0.5, 0.9]");
        }
        double tolerance = decimal("dataset.sizeTolerance");
        if (!(tolerance > 0 && tolerance < 1)) {
            throw new IllegalArgumentException("sizeTolerance must be in (0,1)");
        }
        if (!(decimal("dataset.bucketSkew") >= 1)
                || !Double.isFinite(decimal("dataset.bucketSkew"))) {
            throw new IllegalArgumentException("bucketSkew must be finite and >= 1");
        }
        if (!Instant.parse(get("dataset.hour"))
                .equals(
                        Instant.parse(get("dataset.hour"))
                                .truncatedTo(java.time.temporal.ChronoUnit.HOURS))) {
            throw new IllegalArgumentException("dataset.hour must be an exact UTC hour");
        }
        if (!Set.of("legacy", "ocsf").contains(get("dataset.model", "legacy"))) {
            throw new IllegalArgumentException("dataset.model must be legacy or ocsf");
        }
        if (get("dataset.model", "legacy").equals("ocsf")) {
            if (integer("dataset.ocsf.topLevelFields") <= 0)
                throw new IllegalArgumentException("Need OCSF fields");
            for (String key :
                    List.of(
                            "dataset.ocsf.optionalFieldProbability",
                            "dataset.ocsf.recommendedFieldProbability")) {
                double probability = decimal(key);
                if (!Double.isFinite(probability) || probability < 0 || probability > 1)
                    throw new IllegalArgumentException("Invalid " + key);
            }
            for (var entry : prefix("dataset.eventWeight.").entrySet()) {
                double weight = Double.parseDouble(entry.getValue());
                if (!Double.isFinite(weight) || weight < 0)
                    throw new IllegalArgumentException(
                            "Invalid OCSF event weight: " + entry.getKey());
            }
        } else weights("dataset.eventWeights", 7);
        if (values.containsKey("dataset.bucketWeights")) {
            weights("dataset.bucketWeights", integer("dataset.buckets"));
        }
        for (String k :
                List.of(
                        "target-file-size-bytes",
                        "min-input-files",
                        "max-file-group-size-bytes",
                        "max-concurrent-file-group-rewrites",
                        "partial-progress.max-commits")) {
            if (Long.parseLong(get("compaction." + k)) <= 0) {
                throw new IllegalArgumentException("Invalid compaction." + k);
            }
        }
        for (String k : List.of("k8s.namespace", "engine.name")) {
            if (values.containsKey(k) && !get(k).matches("[a-z][a-z0-9-]{0,39}")) {
                throw new IllegalArgumentException("Invalid name: " + k);
            }
        }
    }
}
