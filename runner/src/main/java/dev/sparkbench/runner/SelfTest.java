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
        Config comet = Config.load(Path.of("config/local.properties"))
                .overlay(Config.load(Path.of("config/engines/comet.properties")));
        comet.validate();
        check(comet.get("image.icebergVersion").equals("1.11.0"), "Comet Iceberg runtime");
        Map<?, ?> cometSpec = (Map<?, ?>) Manifests.spark(comet, "sb-comet", "compact").get("spec");
        Map<?, ?> cometConf = (Map<?, ?>) cometSpec.get("sparkConf");
        check(cometConf.get("spark.plugins").equals("org.apache.spark.CometPlugin"), "Comet plugin");
        check(cometConf.get("spark.comet.iceberg.write.enabled").equals("true"), "Comet native writer");
        check(cometConf.get("spark.comet.write.iceberg.splitOperator.enabled").equals("true"),
                "Comet split write operator");
        check(cometConf.get("spark.memory.offHeap.size").equals("8g"), "Comet native memory budget");
        check(((Map<?, ?>) cometSpec.get("executor")).get("memory").equals("4g"), "Comet heap budget");
        check(cometConf.get("spark.sql.extensions").toString().contains("IcebergSparkSessionExtensions"),
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
                conf.get("spark.sql.catalog.bench.s3.endpoint").equals("http://localstack:4566"),
                "S3 endpoint");
        check(((Map<?, ?>) spec.get("executor")).get("cores").equals(1), "resource profile");
        String json = Json.encode(Map.of("value", "a\n\"\\\t"));
        check(json.equals("{\"value\":\"a\\n\\\"\\\\\\t\"}"), "JSON escaping: " + json);
        Path infra = Infrastructure.render(c);
        String text = Files.readString(infra);
        check(text.contains("jdbc:sqlite:/catalog/iceberg.db?journal_mode=WAL"), "durable catalog");
        check(text.contains("localstack/localstack:"), "LocalStack");
        Path temp = Files.createTempDirectory("sparkbench-config-test");
        Files.writeString(temp.resolve("bad.properties"), "config.parent=bad.properties\n");
        try {
            Config.load(temp.resolve("bad.properties"));
            throw new AssertionError("cycle accepted");
        } catch (IllegalArgumentException expected) {
        }
        Files.delete(temp.resolve("bad.properties"));
        Files.delete(temp);
        System.out.println(
                "Runner self-test passed: inheritance, validation, manifests, JSON, catalog"
                        + " persistence");
    }
}
