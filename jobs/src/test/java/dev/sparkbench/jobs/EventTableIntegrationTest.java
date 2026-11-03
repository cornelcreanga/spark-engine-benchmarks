package dev.sparkbench.jobs;

import static org.apache.spark.sql.functions.col;

import dev.sparkbench.benchmarks.Compaction;
import dev.sparkbench.benchmarks.TableValidation;
import dev.sparkbench.core.Config;
import dev.sparkbench.dataset.EventTable;
import dev.sparkbench.dataset.Events;
import dev.sparkbench.dataset.Generator;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Real Spark/Iceberg integration, independent of Kubernetes and Docker. */
public final class EventTableIntegrationTest {
    public static void main(String[] args) throws Exception {
        Path temporary = Files.createTempDirectory("sparkbench-table-test-");
        Config config =
                Config.load(Path.of("config/smoke.properties"))
                        .overlay(Config.load(Path.of("config/engines/oss.properties")))
                        .with("dataset.buckets", "1")
                        .with("dataset.customers", "32")
                        .with("dataset.partitions", "2")
                        .with("catalog.namespace", "event_model_test")
                        .with("catalog.table", "events")
                        .with("benchmark.deferRestore", "true");
        SparkSession.Builder builder =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("event-table-integration")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.shuffle.partitions", "2")
                        .config("spark.sql.adaptive.enabled", "false")
                        .config("spark.sql.session.timeZone", "UTC")
                        .config(
                                "spark.sql.extensions",
                                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                        .config("spark.sql.catalog.bench", "org.apache.iceberg.spark.SparkCatalog");
        if (System.getenv("BENCH_TEST_REST_URI") == null) {
            builder.config("spark.sql.catalog.bench.type", "hadoop")
                    .config(
                            "spark.sql.catalog.bench.warehouse",
                            temporary.resolve("warehouse").toUri().toString());
        } else {
            builder.config("spark.sql.catalog.bench.type", "rest")
                    .config("spark.sql.catalog.bench.uri", System.getenv("BENCH_TEST_REST_URI"))
                    .config("spark.sql.catalog.bench.warehouse", "s3://spark-bench/warehouse")
                    .config("spark.sql.catalog.bench.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                    .config(
                            "spark.sql.catalog.bench.s3.endpoint",
                            System.getenv("BENCH_TEST_S3_ENDPOINT"))
                    .config("spark.sql.catalog.bench.s3.path-style-access", "true")
                    .config("spark.sql.catalog.bench.s3.access-key-id", "test")
                    .config("spark.sql.catalog.bench.s3.secret-access-key", "test")
                    .config("spark.sql.catalog.bench.client.region", "us-east-1");
        }
        try (SparkSession spark = builder.getOrCreate()) {
            spark.sparkContext().setLogLevel("WARN");
            spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.event_model_test");
            spark.sql(Generator.createTableSql(config));
            Events events = new Events(config);
            for (int batch = 0; batch < 3; batch++) {
                spark.range(batch * 490L, (batch + 1) * 490L, 1, 2)
                        .mapPartitions(events, Encoders.row(Events.SCHEMA))
                        .repartition(2, col("_bucket"), col("eventtype"))
                        .sortWithinPartitions("_bucket", "eventtype")
                        .drop("_bucket")
                        .writeTo(config.table())
                        .append();
            }
            long baseline = Generator.snapshot(spark, config);
            spark.sql(
                    "ALTER TABLE "
                            + config.table()
                            + " CREATE TAG benchmark_input AS OF VERSION "
                            + baseline);
            check(spark.table(config.table()).count() == 1470, "all typed rows written");
            check(
                    java.util.Arrays.equals(
                            spark.table(config.table()).columns(), EventTable.SCHEMA.fieldNames()),
                    "DDL and generated row schema agree");
            check(
                    spark.sql("SELECT DISTINCT eventtype FROM " + config.table()).count() == 7,
                    "all seven event types readable");
            check(
                    spark.sql(
                                            "SELECT count(*) FROM "
                                                    + config.table()
                                                    + " WHERE length(cid) <> 36 OR"
                                                    + " length(sid) <> 36")
                                    .first()
                                    .getLong(0)
                            == 0,
                    "UUIDs are stored as canonical strings");
            String fingerprint = Compaction.fingerprint(spark, config);
            long originalFiles =
                    ((Number) Compaction.stats(spark, config).get("files")).longValue();
            Compaction.run(spark, config);
            long compactedFiles =
                    ((Number) Compaction.stats(spark, config).get("files")).longValue();
            check(compactedFiles < originalFiles, "compaction actually rewrote the files");
            TableValidation.run(spark, config);
            if (Boolean.parseBoolean(System.getenv("BENCH_TEST_KEEP_COMPACTED"))) {
                System.out.println("Compacted table retained for independent Trino validation");
                return;
            }
            spark.sql(
                    "UPDATE "
                            + config.table()
                            + " SET dns_duration_ms = dns_duration_ms + 1 WHERE eventtype ="
                            + " 'dnsrequest'");
            check(
                    !fingerprint.equals(Compaction.fingerprint(spark, config)),
                    "new DNS columns are covered by the fingerprint");
            try {
                TableValidation.run(spark, config);
                throw new AssertionError("validator accepted changed content");
            } catch (IllegalStateException expected) {
                check(
                        expected.getMessage().contains("different from the baseline"),
                        "content failure reason");
            }
            Compaction.reset(spark, config);
            check(
                    fingerprint.equals(Compaction.fingerprint(spark, config)),
                    "baseline restoration");
            spark.sql(
                    "ALTER TABLE "
                            + config.table()
                            + " SET TBLPROPERTIES ('write.metadata.metrics.default'='none')");
            spark.sql(
                            "CALL bench.system.rewrite_data_files(table =>"
                                    + " 'event_model_test.events', options => map('rewrite-all',"
                                    + " 'true'))")
                    .collect();
            try {
                TableValidation.run(spark, config);
                throw new AssertionError("validator accepted missing file statistics");
            } catch (IllegalStateException expected) {
                check(
                        expected.getMessage().contains("statistics")
                                || expected.getMessage().contains("metrics"),
                        "statistics failure reason");
            } finally {
                spark.sql(
                        "ALTER TABLE "
                                + config.table()
                                + " SET TBLPROPERTIES ('write.metadata.metrics.default'='full')");
                Compaction.reset(spark, config);
            }
            Config reusable =
                    config.with("catalog.table", "prepared_events")
                            .with("dataset.targetBytes", "2097152")
                            .with("dataset.sizeTolerance", "0.25")
                            .with("dataset.calibrationRows", "1000")
                            .with("dataset.maxBatchRows", "4000");
            Generator.prepare(spark, reusable);
            long inputSnapshot = Generator.snapshot(spark, reusable);
            Generator.prepare(spark, reusable.with("spark.executor.memory", "8g"));
            check(
                    inputSnapshot == Generator.snapshot(spark, reusable),
                    "prepare reuses the baseline without rewriting");
            try {
                Generator.prepare(spark, reusable.with("dataset.seed", "99"));
                throw new AssertionError("prepare accepted a different dataset contract");
            } catch (IllegalStateException expected) {
                check(
                        expected.getMessage().contains("different settings"),
                        "input mismatch reason");
            }
            Config incomplete = reusable.with("catalog.table", "incomplete_events");
            spark.sql(Generator.createTableSql(incomplete));
            try {
                Generator.prepare(spark, incomplete);
                throw new AssertionError("prepare accepted incomplete input");
            } catch (IllegalStateException expected) {
                check(
                        expected.getMessage().contains("baseline tag is missing"),
                        "incomplete input reason");
            }
            System.out.println(
                    "Event table integration passed: typed schema, real compaction, full"
                            + " statistics, corruption detection, restoration");
        } finally {
            try (var paths = Files.walk(temporary)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static void check(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError(description);
        }
    }
}
