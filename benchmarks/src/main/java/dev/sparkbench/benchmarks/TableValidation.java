package dev.sparkbench.benchmarks;

import dev.sparkbench.core.Config;
import dev.sparkbench.results.Report;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/** Reads all values with OSS Spark and verifies the Iceberg per-file statistics. */
public final class TableValidation {
    private TableValidation() {}

    public static void run(SparkSession spark, Config config) {
        Report report =
                new Report().put("validator", "oss-spark").put("image", config.get("engine.image"));
        try {
            for (String column : List.of("cid", "sid")) {
                if (!spark.table(config.table())
                        .schema()
                        .apply(column)
                        .dataType()
                        .equals(DataTypes.StringType)) {
                    throw new IllegalStateException(column + " must store UUIDs as strings");
                }
            }
            long invalidIds =
                    spark.sql(
                                    "SELECT count(*) FROM "
                                            + config.table()
                                            + " WHERE cid IS NULL OR sid IS NULL OR"
                                            + " cid NOT RLIKE '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' OR"
                                            + " sid NOT RLIKE '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'")
                            .first()
                            .getLong(0);
            if (invalidIds != 0) {
                throw new IllegalStateException("Customer/system UUIDs must be canonical version-4 strings");
            }
            report.put("uuidStringsValid", true);
            long baseline = Compaction.baseline(spark, config);
            String expected =
                    Compaction.fingerprint(
                            spark, config, config.table() + " VERSION AS OF " + baseline);
            String actual = Compaction.fingerprint(spark, config);
            report.put("baselineSnapshot", baseline)
                    .put("baselineFingerprint", expected)
                    .put("outputFingerprint", actual);
            if (!expected.equals(actual)) {
                throw new IllegalStateException("OSS Spark found data different from the baseline");
            }
            report.put("dataIdentical", true)
                    .put("allColumnsReadable", true)
                    .put("statistics", verifyStatistics(spark, config))
                    .put("status", "success");
        } catch (RuntimeException failure) {
            report.put("status", "failed").put("error", failure.toString());
            throw failure;
        } finally {
            report.emit("BENCH_VALIDATION");
        }
    }

    public static Map<String, Object> verifyStatistics(SparkSession spark, Config config) {
        List<StructField> columns =
                java.util.Arrays.stream(spark.table(config.table()).schema().fields())
                        .filter(field -> !field.name().equals("attributes"))
                        .toList();
        StringJoiner aggregates = new StringJoiner(", ");
        aggregates.add("_file AS file_path").add("count(*) AS actual_records");
        for (StructField column : columns) {
            String name = quoted(column.name());
            aggregates.add("count(*) - count(" + name + ") AS " + quoted(column.name() + "_nulls"));
            aggregates.add("min(" + name + ") AS " + quoted(column.name() + "_min"));
            aggregates.add("max(" + name + ") AS " + quoted(column.name() + "_max"));
        }
        Map<String, Row> observedFiles = new LinkedHashMap<>();
        for (Row row :
                spark.sql("SELECT " + aggregates + " FROM " + config.table() + " GROUP BY _file")
                        .collectAsList()) {
            observedFiles.put(row.getAs("file_path"), row);
        }
        List<Row> metadataFiles =
                spark.sql(
                                "SELECT file_path, record_count, file_size_in_bytes,"
                                        + " readable_metrics FROM "
                                        + config.table()
                                        + ".files WHERE content = 0")
                        .collectAsList();
        if (metadataFiles.size() != observedFiles.size()) {
            throw new IllegalStateException("Metadata files do not match the readable data files");
        }
        long records = 0;
        for (Row file : metadataFiles) {
            String path = file.getAs("file_path");
            Row observed = observedFiles.get(path);
            if (observed == null) {
                throw new IllegalStateException(
                        "File listed in metadata was not readable: " + path);
            }
            long recordCount = ((Number) file.getAs("record_count")).longValue();
            requireEqual(recordCount, observed.getAs("actual_records"), path + " record_count");
            if (((Number) file.getAs("file_size_in_bytes")).longValue() <= 0) {
                throw new IllegalStateException("Invalid file size: " + path);
            }
            Row metrics = file.getAs("readable_metrics");
            if (metrics == null) {
                throw new IllegalStateException("No readable metrics for " + path);
            }
            for (StructField column : columns) {
                String name = column.name();
                Row bounds = metrics.getAs(name);
                if (bounds == null) {
                    throw new IllegalStateException(
                            "Missing metrics for " + path + " column " + name);
                }
                requireEqual(recordCount, bounds.getAs("value_count"), name + " value_count");
                requireEqual(
                        observed.getAs(name + "_nulls"),
                        bounds.getAs("null_value_count"),
                        name + " null_value_count");
                requireEqual(
                        observed.getAs(name + "_min"),
                        bounds.getAs("lower_bound"),
                        name + " lower_bound");
                requireEqual(
                        observed.getAs(name + "_max"),
                        bounds.getAs("upper_bound"),
                        name + " upper_bound");
            }
            records += recordCount;
        }
        return Map.of(
                "filesChecked",
                metadataFiles.size(),
                "recordsChecked",
                records,
                "primitiveColumnsChecked",
                columns.size(),
                "recordCountsMatch",
                true,
                "nullCountsMatch",
                true,
                "boundsMatch",
                true);
    }

    private static String quoted(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    private static void requireEqual(Object expected, Object actual, String description) {
        if (!Objects.deepEquals(expected, actual)) {
            throw new IllegalStateException(
                    "Invalid Iceberg statistics: "
                            + description
                            + " expected="
                            + display(expected)
                            + " actual="
                            + display(actual));
        }
    }

    private static String display(Object value) {
        return value instanceof byte[] bytes
                ? "0x" + java.util.HexFormat.of().formatHex(bytes)
                : String.valueOf(value);
    }
}
