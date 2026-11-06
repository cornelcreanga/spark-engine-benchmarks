package dev.sparkbench.benchmarks;

import dev.sparkbench.core.Config;
import dev.sparkbench.core.Json;

import org.apache.spark.sql.SparkSession;

import java.nio.file.*;
import java.util.*;

/** Barrier: a node-level reader populates page cache outside the Spark pod's memory limit. */
final class WarmCache {
    static Map<String, Object> await(SparkSession spark, Config c) throws Exception {
        Path directory =
                Path.of(
                        c.get("filesystem.mountPath"),
                        ".warm-cache",
                        c.get("benchmark.applicationName"));
        Files.createDirectories(directory);
        List<Map<String, Object>> files = new ArrayList<>();
        for (var row :
                spark.sql("SELECT file_path, file_size_in_bytes FROM " + c.table() + ".files")
                        .collectAsList()) {
            files.add(Map.of("path", row.getString(0), "bytes", row.getLong(1)));
        }
        Path temporary = directory.resolve("request.tmp");
        Files.writeString(temporary, Json.encode(files));
        Files.move(temporary, directory.resolve("request.json"), StandardCopyOption.ATOMIC_MOVE);
        System.out.println("WARM_CACHE_READY " + directory);
        long deadline =
                System.nanoTime() + c.number("benchmark.warmCacheTimeoutSeconds") * 1_000_000_000L;
        while (!Files.exists(directory.resolve("done.properties"))) {
            if (Files.exists(directory.resolve("error.txt"))) {
                throw new IllegalStateException(
                        "Warm-up failed: " + Files.readString(directory.resolve("error.txt")));
            }
            if (System.nanoTime() >= deadline) throw new IllegalStateException("Warm-up timed out");
            Thread.sleep(100);
        }
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(directory.resolve("done.properties"))) {
            properties.load(reader);
        }
        Map<String, Object> result = new TreeMap<>();
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            if (key.contains("Fraction") || key.equals("seconds"))
                result.put(key, Double.parseDouble(value));
            else result.put(key, Long.parseLong(value));
        }
        return result;
    }
}
