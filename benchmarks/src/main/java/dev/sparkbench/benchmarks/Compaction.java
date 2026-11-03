package dev.sparkbench.benchmarks;

import dev.sparkbench.core.Config;
import dev.sparkbench.results.Report;

import org.apache.spark.sql.*;

import java.util.*;

public final class Compaction {
    public static long baseline(SparkSession spark, Config c) {
        List<Row> refs =
                spark.sql(
                                "SELECT snapshot_id FROM "
                                        + c.table()
                                        + ".refs WHERE name='benchmark_input'")
                        .collectAsList();
        if (refs.size() != 1) {
            throw new IllegalStateException(
                    "Missing benchmark_input tag; generate input successfully first");
        }
        return ((Number) refs.get(0).get(0)).longValue();
    }

    public static void reset(SparkSession spark, Config c) {
        long id = baseline(spark, c);
        spark.sql(
                        "CALL "
                                + c.get("catalog.name")
                                + ".system.set_current_snapshot(table => "
                                + Config.literal(c.shortTable())
                                + ", snapshot_id => "
                                + id
                                + ")")
                .collect();
        spark.catalog().refreshTable(c.table());
        long actual =
                ((Number)
                                spark.sql(
                                                "SELECT snapshot_id FROM "
                                                        + c.table()
                                                        + ".refs WHERE name='main'")
                                        .first()
                                        .get(0))
                        .longValue();
        if (actual != id) {
            throw new IllegalStateException("Snapshot restoration failed");
        }
    }

    public static Map<String, Object> stats(SparkSession spark, Config c) {
        Row r =
                spark.sql(
                                "SELECT"
                                    + " count(*),coalesce(sum(file_size_in_bytes),0),coalesce(sum(record_count),0),min(file_size_in_bytes),max(file_size_in_bytes),percentile_approx(file_size_in_bytes,array(0.5,0.9,0.99))"
                                    + " FROM "
                                        + c.table()
                                        + ".files")
                        .first();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("files", r.getLong(0));
        m.put("bytes", r.get(1));
        m.put("records", r.get(2));
        m.put("minFileBytes", r.get(3));
        m.put("maxFileBytes", r.get(4));
        m.put("fileSizePercentiles", r.isNullAt(5) ? null : r.getList(5));
        return m;
    }

    public static String fingerprint(SparkSession spark, Config c) {
        return fingerprint(spark, c, c.table());
    }

    public static String fingerprint(SparkSession spark, Config c, String relation) {
        // Canonical map ordering makes the checksum independent of reader map iteration order.
        String fields =
                Arrays.stream(spark.table(c.table()).columns())
                        .map(
                                column ->
                                        column.equals("attributes")
                                                ? "to_json(array_sort(map_entries(`attributes`)))"
                                                : "`" + column.replace("`", "``") + "`")
                        .collect(java.util.stream.Collectors.joining(","));
        return spark.sql(
                        "SELECT count(*),sum(cast(xxhash64("
                                + fields
                                + ") AS DECIMAL(38,0))),sum(cast(xxhash64(9173,"
                                + fields
                                + ") AS DECIMAL(38,0))) FROM "
                                + relation)
                .first()
                .toString();
    }

    public static void run(SparkSession spark, Config c) {
        Report report =
                new Report()
                        .put("engine", c.get("engine.name"))
                        .put("image", c.get("engine.image"))
                        .put("sparkVersion", spark.version())
                        .put("strategy", c.get("benchmark.compactionStrategy", "sort"))
                        .put("validationEnabled", c.validationEnabled())
                        .put("configuration", redacted(c));
        Throwable failure = null;
        try {
            reset(spark, c);
            report.put("baselineSnapshot", baseline(spark, c));
            Map<String, Object> before = stats(spark, c);
            String checksum = c.validationEnabled() ? fingerprint(spark, c) : null;
            report.put("before", before).put("fingerprintBefore", checksum);
            String sql = compactionSql(c);
            spark.sparkContext().listenerBus().waitUntilEmpty(30000);
            TaskCounters counters = new TaskCounters();
            spark.sparkContext().addSparkListener(counters);
            long started = System.nanoTime();
            Dataset<Row> result;
            List<Row> output;
            try {
                result = spark.sql(sql);
                output = result.collectAsList();
                counters.finished = System.nanoTime();
            } finally {
                spark.sparkContext().listenerBus().waitUntilEmpty(30000);
                spark.sparkContext().removeSparkListener(counters);
                report.put("taskMetrics", counters.values());
            }
            double seconds = counters.finishedSeconds(started);
            List<Map<String, Object>> procedure = new ArrayList<>();
            for (Row row : output) {
                Map<String, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < result.columns().length; i++) {
                    m.put(result.columns()[i], row.get(i));
                }
                procedure.add(m);
            }
            report.put("durationSeconds", seconds)
                    .put(
                            "inputBytesPerSecond",
                            ((Number) before.get("bytes")).doubleValue() / seconds)
                    .put("procedure", procedure);
            spark.catalog().refreshTable(c.table());
            String after = c.validationEnabled() ? fingerprint(spark, c) : null;
            report.put("after", stats(spark, c)).put("fingerprintAfter", after);
            if (c.validationEnabled() && !checksum.equals(after)) {
                throw new IllegalStateException("Content fingerprint changed during compaction");
            }
            for (Map<String, Object> row : procedure) {
                for (var entry : row.entrySet()) {
                    if (entry.getKey().contains("failed")
                            && entry.getValue() instanceof Number n
                            && n.longValue() > 0) {
                        throw new IllegalStateException(
                                "Partial progress left failed rewrite groups: " + procedure);
                    }
                }
            }
            report.put("status", "success");
        } catch (Throwable e) {
            failure = e;
            report.put("status", "failed").put("error", e.toString());
        } finally {
            if (failure == null && c.flag("benchmark.deferRestore")) {
                report.put("baselineRestored", false).put("restorationDeferredForValidation", true);
            } else {
                try {
                    reset(spark, c);
                    report.put("baselineRestored", true);
                } catch (Throwable restore) {
                    report.put("baselineRestored", false).put("restoreError", restore.toString());
                    if (failure == null) {
                        failure = restore;
                    } else {
                        failure.addSuppressed(restore);
                    }
                }
            }
            report.emit();
        }
        if (failure != null) {
            throw new RuntimeException("Benchmark failed", failure);
        }
    }

    public static String compactionSql(Config c) {
        String strategy = c.get("benchmark.compactionStrategy", "sort");
        if (!Set.of("sort", "binpack").contains(strategy)) {
            throw new IllegalArgumentException("Unsupported compaction strategy: " + strategy);
        }
        StringJoiner options = new StringJoiner(",");
        c.prefix("compaction.")
                .forEach((k, v) -> options.add(Config.literal(k) + "," + Config.literal(v)));
        return "CALL " + Config.identifier(c.get("catalog.name"))
                + ".system.rewrite_data_files(table => " + Config.literal(c.shortTable())
                + ", strategy => " + Config.literal(strategy)
                + (strategy.equals("sort")
                        ? ", sort_order => 'cid ASC NULLS FIRST, sid ASC NULLS FIRST'" : "")
                + ", options => map(" + options + "))";
    }

    private static Map<String, String> redacted(Config c) {
        Map<String, String> m = new TreeMap<>(c.all());
        m.replaceAll(
                (k, v) ->
                        k.toLowerCase(Locale.ROOT).matches(".*(secret|password|token|accesskey).*")
                                ? "[redacted]"
                                : v);
        return m;
    }
}
