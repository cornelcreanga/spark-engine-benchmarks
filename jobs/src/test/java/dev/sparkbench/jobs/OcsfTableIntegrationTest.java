package dev.sparkbench.jobs;

import dev.sparkbench.core.Config;
import dev.sparkbench.dataset.*;

import org.apache.spark.sql.SparkSession;

import java.nio.file.*;
import java.util.*;

/** Writes/reuses the real wide schema through Spark/Iceberg, without external services. */
public final class OcsfTableIntegrationTest {
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void main(String[] args) throws Exception {
        Path temporary =
                Files.createTempDirectory(Path.of("build"), "ocsf-integration-").toAbsolutePath();
        Config c =
                Config.load(Path.of("config/ocsf.properties"))
                        .overlay(Config.load(Path.of("config/engines/oss.properties")))
                        .with("catalog.namespace", "ocsf_test")
                        .with("catalog.table", "events")
                        .with(
                                "catalog.warehouse",
                                temporary.resolve("warehouse").toUri().toString())
                        .with("filesystem.mountPath", temporary.toString())
                        .with("dataset.buckets", "1")
                        .with("dataset.customers", "32")
                        .with("dataset.partitions", "2")
                        .with("dataset.rows", "4096")
                        .with("dataset.calibrationRows", "1100")
                        .with("dataset.maxBatchRows", "1500");
        c.validate();
        try (SparkSession spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("ocsf-integration")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.shuffle.partitions", "2")
                        .config("spark.sql.session.timeZone", "UTC")
                        .config(
                                "spark.sql.extensions",
                                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                        .config("spark.sql.catalog.bench", "org.apache.iceberg.spark.SparkCatalog")
                        .config("spark.sql.catalog.bench.type", "hadoop")
                        .config("spark.sql.catalog.bench.warehouse", c.get("catalog.warehouse"))
                        .getOrCreate()) {
            spark.sparkContext().setLogLevel("WARN");
            Generator.prepare(spark, c);
            check(spark.table(c.table()).schema().length() == 206, "wide table column count");
            check(spark.table(c.table()).count() == 4096, "exact row count");
            check(
                    spark.sql("SELECT DISTINCT class_uid FROM " + c.table()).count() == 86,
                    "all class UIDs written");
            var row =
                    spark.sql(
                                    "SELECT metadata__version, attributes FROM "
                                            + c.table()
                                            + " WHERE eventtype='process_activity' LIMIT 1")
                            .first();
            check(row.getString(0).equals("1.9.0"), "real schema version");
            check(!row.getJavaMap(1).isEmpty(), "map data survives Parquet round trip");
            long baseline = Generator.snapshot(spark, c);
            Generator.prepare(spark, c);
            check(Generator.snapshot(spark, c) == baseline, "reuse retains baseline snapshot");
            System.out.println(
                    "OCSF table integration passed: 206 columns, 4096 rows, 86 classes, map round"
                            + " trip, baseline reuse");
        } finally {
            try (var paths = Files.walk(temporary)) {
                for (Path p : paths.sorted(Comparator.reverseOrder()).toList())
                    Files.deleteIfExists(p);
            }
        }
    }
}
