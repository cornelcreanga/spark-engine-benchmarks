package dev.sparkbench.runner;

import dev.sparkbench.core.*;

import java.nio.file.*;
import java.util.*;

public final class SelfTest {
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static void main(String[] args) throws Exception {
        Config smoke = Config.load(Path.of("config/smoke.properties"));
        smoke.validate();
        check(smoke.integer("dataset.buckets") == 4, "profile override");
        check(
                smoke.number("compaction.target-file-size-bytes") == 536870912L,
                "profile inheritance");
        Config c = smoke.overlay(Config.load(Path.of("config/engines/oss.properties")));
        c.validate();
        Config comet =
                Config.load(Path.of("config/local.properties"))
                        .overlay(Config.load(Path.of("config/engines/comet.properties")));
        comet.validate();
        check(comet.get("image.icebergVersion").equals("1.11.0"), "Comet Iceberg runtime");
        Map<?, ?> cometSpec = (Map<?, ?>) Manifests.spark(comet, "sb-comet", "compact").get("spec");
        Map<?, ?> cometConf = (Map<?, ?>) cometSpec.get("sparkConf");
        check(
                cometConf.get("spark.plugins").equals("org.apache.spark.CometPlugin"),
                "Comet plugin");
        check(
                cometConf.get("spark.comet.iceberg.write.enabled").equals("true"),
                "Comet native writer");
        check(
                cometConf.get("spark.comet.write.iceberg.splitOperator.enabled").equals("true"),
                "Comet split write operator");
        check(
                cometConf.get("spark.memory.offHeap.size").equals("8g"),
                "Comet native memory budget");
        check(
                ((Map<?, ?>) cometSpec.get("executor")).get("memory").equals("4g"),
                "Comet heap budget");
        check(
                cometConf
                        .get("spark.sql.extensions")
                        .toString()
                        .contains("IcebergSparkSessionExtensions"),
                "Comet retains Iceberg extension");
        try {
            c.with("image.icebergVersion", "1.10.0").validate();
            throw new AssertionError("Unsupported Iceberg runtime accepted");
        } catch (IllegalArgumentException expected) {
        }
        Map<String, Object> manifest = Manifests.spark(c, "sb-test", "compact");
        Map<?, ?> spec = (Map<?, ?>) manifest.get("spec");
        Map<?, ?> conf = (Map<?, ?>) spec.get("sparkConf");
        check(
                conf.get("spark.sql.catalog.bench.type").equals("hadoop"),
                "default filesystem catalog");
        check(
                conf.get("spark.sql.catalog.bench.warehouse").equals("file:///warehouse/iceberg"),
                "default local warehouse");
        check(((Map<?, ?>) spec.get("executor")).get("cores").equals(1), "resource profile");
        String json = Json.encode(Map.of("value", "a\n\"\\\t"));
        check(json.equals("{\"value\":\"a\\n\\\"\\\\\\t\"}"), "JSON escaping: " + json);
        Path infra = Infrastructure.render(c);
        String text = Files.readString(infra);
        check(
                text.contains("benchmark-warehouse") && !text.contains("Deployment"),
                "local storage without service deployments");
        Config fs =
                Config.load(Path.of("config/filesystem.properties"))
                        .overlay(Config.load(Path.of("config/engines/oss.properties")));
        fs.validate();
        Map<?, ?> fsSpec = (Map<?, ?>) Manifests.spark(fs, "sb-filesystem", "compact").get("spec");
        Map<?, ?> fsConf = (Map<?, ?>) fsSpec.get("sparkConf");
        check(
                fsConf.get("spark.sql.catalog.bench.type").equals("hadoop"),
                "filesystem Hadoop catalog");
        check(
                !fsConf.containsKey("spark.sql.catalog.bench.s3.endpoint"),
                "filesystem has no S3 catalog settings");
        check(((List<?>) fsSpec.get("volumes")).size() == 1, "shared filesystem PVC");
        check(
                ((Map<?, ?>) fsSpec.get("driver"))
                        .get("volumeMounts")
                        .equals(((Map<?, ?>) fsSpec.get("executor")).get("volumeMounts")),
                "identical filesystem mounts");
        Config quanton =
                Config.load(Path.of("config/simple-event-name.properties"))
                        .overlay(Config.load(Path.of("config/engines/quanton.properties")));
        quanton.validate();
        Map<?, ?> quantonManifest = Manifests.spark(quanton, "sb-quanton", "compact");
        check(
                quantonManifest.get("kind").equals("QuantonSparkApplication"),
                "Quanton resource kind");
        Map<?, ?> quantonSpec =
                (Map<?, ?>) ((Map<?, ?>) quantonManifest.get("spec")).get("sparkApplicationSpec");
        check(
                quantonSpec.get("mainApplicationFile").toString().startsWith("local:///warehouse/"),
                "Quanton uses shared benchmark JAR");
        check(
                ((Map<?, ?>) quantonSpec.get("sparkConf"))
                        .get("spark.quanton.clustering.accelerate")
                        .equals("true"),
                "Quanton clustering acceleration");
        check(
                ((Map<?, ?>) quantonSpec.get("executor")).get("memory").equals("12g"),
                "Quanton total heap/native memory budget");
        String fsInfra = Files.readString(Infrastructure.render(fs));
        check(
                fsInfra.contains("benchmark-warehouse") && !fsInfra.contains("Deployment"),
                "filesystem infrastructure needs no service deployments");
        Path temp = Files.createTempDirectory("sparkbench-config-test");
        Files.writeString(temp.resolve("parent.properties"), "key=parent\n");
        Files.writeString(
                temp.resolve("weights.properties"),
                "key=include\ndataset.eventWeight.process_activity=10\n");
        Files.writeString(
                temp.resolve("included.properties"),
                "config.parent=parent.properties\nconfig.includes=weights.properties\nkey=own\n");
        Config included = Config.load(temp.resolve("included.properties"));
        check(included.get("key").equals("own"), "profile overrides include and parent");
        check(
                included.get("dataset.eventWeight.process_activity").equals("10"),
                "named weight include");
        Files.writeString(
                temp.resolve("include-cycle.properties"),
                "config.includes=include-cycle.properties\n");
        try {
            Config.load(temp.resolve("include-cycle.properties"));
            throw new AssertionError("Include cycle accepted");
        } catch (IllegalArgumentException expected) {
        }
        Files.writeString(temp.resolve("bad.properties"), "config.parent=bad.properties\n");
        try {
            Config.load(temp.resolve("bad.properties"));
            throw new AssertionError("cycle accepted");
        } catch (IllegalArgumentException expected) {
        }
        try (var paths = Files.list(temp)) {
            for (Path file : paths.toList()) Files.delete(file);
        }
        Files.delete(temp);
        System.out.println(
                "Runner self-test passed: inheritance, validation, manifests, JSON, catalog"
                        + " persistence");
    }
}
