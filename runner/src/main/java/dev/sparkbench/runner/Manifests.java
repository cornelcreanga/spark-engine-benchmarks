package dev.sparkbench.runner;

import dev.sparkbench.core.*;

import java.nio.file.*;
import java.util.*;

final class Manifests {
    static Map<String, Object> map(Object... pairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put(pairs[i].toString(), pairs[i + 1]);
        }
        return m;
    }

    static Map<String, Object> resource(
            String api, String kind, String name, Config c, Object... more) {
        Map<String, Object> m =
                map(
                        "apiVersion",
                        api,
                        "kind",
                        kind,
                        "metadata",
                        map("name", name, "namespace", c.get("k8s.namespace")));
        m.putAll(map(more));
        return m;
    }

    static Map<String, Object> spark(Config c, String name, String action) {
        String catalog = "spark.sql.catalog." + c.get("catalog.name");
        Map<String, String> conf = new TreeMap<>();
        conf.put(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        conf.put(catalog, "org.apache.iceberg.spark.SparkCatalog");
        conf.put(catalog + ".type", "rest");
        conf.put(catalog + ".uri", c.get("catalog.uri"));
        conf.put(catalog + ".warehouse", c.get("catalog.warehouse"));
        conf.put(catalog + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        conf.put(catalog + ".s3.endpoint", c.get("s3.endpoint"));
        conf.put(catalog + ".s3.path-style-access", "true");
        conf.put(catalog + ".client.region", c.get("s3.region"));
        if (c.all().containsKey("s3.accessKey")) {
            conf.put(catalog + ".s3.access-key-id", c.get("s3.accessKey"));
        }
        if (c.all().containsKey("s3.secretKey")) {
            conf.put(catalog + ".s3.secret-access-key", c.get("s3.secretKey"));
        }
        conf.put("spark.sql.shuffle.partitions", c.get("spark.shufflePartitions"));
        conf.put("spark.dynamicAllocation.enabled", "false");
        conf.put("spark.sql.adaptive.enabled", "false");
        conf.put("spark.sql.session.timeZone", "UTC");
        conf.putAll(c.prefix("spark.conf."));
        if (!conf.get("spark.sql.extensions")
                .contains("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")) {
            throw new IllegalArgumentException(
                    "Custom SQL extensions must retain IcebergSparkSessionExtensions");
        }
        Map<String, Object> driver =
                map(
                        "cores",
                        c.integer("spark.driver.cores"),
                        "memory",
                        c.get("spark.driver.memory"),
                        "memoryOverhead",
                        c.get("spark.driver.memoryOverhead"),
                        "serviceAccount",
                        "spark-bench",
                        "configMaps",
                        List.of(map("name", name, "path", "/etc/spark-bench")));
        Map<String, Object> executor =
                map(
                        "instances",
                        c.integer("spark.executor.instances"),
                        "cores",
                        c.integer("spark.executor.cores"),
                        "memory",
                        c.get("spark.executor.memory"),
                        "memoryOverhead",
                        c.get("spark.executor.memoryOverhead"));
        if (c.all().containsKey("k8s.credentialsSecret")) {
            Object from = List.of(map("secretRef", map("name", c.get("k8s.credentialsSecret"))));
            driver.put("envFrom", from);
            executor.put("envFrom", from);
        }
        Map<String, Object> spec =
                map(
                        "type",
                        "Java",
                        "mode",
                        "cluster",
                        "sparkVersion",
                        c.get("spark.version"),
                        "image",
                        c.get("engine.image"),
                        "imagePullPolicy",
                        c.get("k8s.imagePullPolicy"),
                        "mainClass",
                        "dev.sparkbench.jobs.Main",
                        "mainApplicationFile",
                        "local:///opt/spark/bench/spark-bench-jobs.jar",
                        "arguments",
                        List.of(action, "/etc/spark-bench/job.properties"),
                        "restartPolicy",
                        map("type", "Never"),
                        "sparkConf",
                        conf,
                        "driver",
                        driver,
                        "executor",
                        executor);
        return resource("sparkoperator.k8s.io/v1beta2", "SparkApplication", name, c, "spec", spec);
    }

    static void write(Path file, Object object) throws Exception {
        Files.createDirectories(file.toAbsolutePath().getParent());
        Files.writeString(file, Json.encode(object) + "\n");
    }
}
