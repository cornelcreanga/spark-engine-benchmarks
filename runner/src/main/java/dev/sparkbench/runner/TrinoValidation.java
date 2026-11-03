package dev.sparkbench.runner;

import dev.sparkbench.core.Config;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Compares output to the retained baseline using Trino's independent Iceberg reader. */
final class TrinoValidation {
    private TrinoValidation() {}

    static void run(Config config, Suite suite, String label) throws Exception {
        Path directory = suite.output.resolve(label + "-trino");
        Files.createDirectories(directory);
        try {
            String table =
                    identifier(config.get("validation.trino.catalog"))
                            + "."
                            + identifier(config.get("catalog.namespace"))
                            + "."
                            + identifier(config.get("catalog.table"));
            String columnsQuery =
                    "SELECT column_name FROM "
                            + identifier(config.get("validation.trino.catalog"))
                            + ".information_schema.columns WHERE table_schema = "
                            + Config.literal(config.get("catalog.namespace"))
                            + " AND table_name = "
                            + Config.literal(config.get("catalog.table"))
                            + " ORDER BY ordinal_position";
            String columns = query(config, suite, columnsQuery);
            List<String> names =
                    columns.lines()
                            .filter(line -> !line.isBlank())
                            .map(TrinoValidation::unquote)
                            .toList();
            if (names.isEmpty()) {
                throw new IllegalStateException("Trino found no columns for " + table);
            }
            String values =
                    names.stream()
                            .map(
                                    name ->
                                            name.equals("attributes")
                                                    ? "array_sort(map_entries("
                                                            + identifier(name)
                                                            + "))"
                                                    : identifier(name))
                            .collect(Collectors.joining(", "));
            String aggregate =
                    "SELECT count(*) AS row_count, to_hex(checksum(ROW("
                            + values
                            + "))) AS fingerprint FROM "
                            + table;
            String sql =
                    "WITH baseline AS ("
                            + aggregate
                            + " FOR VERSION AS OF 'benchmark_input'), "
                            + "compacted AS ("
                            + aggregate
                            + ") SELECT baseline.row_count = compacted.row_count AND"
                            + " baseline.fingerprint = compacted.fingerprint AS identical,"
                            + " baseline.row_count, compacted.row_count, baseline.fingerprint,"
                            + " compacted.fingerprint FROM baseline CROSS JOIN compacted";
            Files.writeString(directory.resolve("validation.sql"), sql + "\n");
            String output = query(config, suite, sql);
            Files.writeString(directory.resolve("comparison.tsv"), output);
            String[] result = output.strip().split("\t", -1);
            if (result.length != 5 || !unquote(result[0]).equals("true")) {
                throw new IllegalStateException("Trino data comparison failed: " + output);
            }
            long count = Long.parseLong(unquote(result[2]));
            String stats = query(config, suite, "SHOW STATS FOR " + table);
            Files.writeString(directory.resolve("statistics.tsv"), stats);
            boolean rowCountMatches =
                    stats.lines()
                            .map(line -> line.split("\t", -1))
                            .filter(fields -> fields.length >= 5)
                            .anyMatch(
                                    fields ->
                                            isNull(fields[0])
                                                    && !isNull(fields[4])
                                                    && new BigDecimal(unquote(fields[4]))
                                                                    .compareTo(
                                                                            BigDecimal.valueOf(
                                                                                    count))
                                                            == 0);
            if (!rowCountMatches) {
                throw new IllegalStateException(
                        "Trino SHOW STATS row count does not match its table scan");
            }
            Manifests.write(
                    directory.resolve("validation.json"),
                    Map.of(
                            "validator",
                            "trino",
                            "status",
                            "success",
                            "dataIdentical",
                            true,
                            "allColumnsReadable",
                            true,
                            "rowCount",
                            count,
                            "statisticsRowCountMatches",
                            true));
        } catch (Exception failure) {
            Manifests.write(
                    directory.resolve("validation.json"),
                    Map.of("validator", "trino", "status", "failed", "error", failure.toString()));
            throw failure;
        }
    }

    private static String query(Config config, Suite suite, String sql) throws Exception {
        List<String> arguments =
                new ArrayList<>(
                        List.of(
                                "exec",
                                config.get("validation.trino.pod"),
                                "--",
                                "java",
                                "-Xmx" + config.get("validation.trino.cliHeap", "128m"),
                                "--enable-native-access=ALL-UNNAMED",
                                "-XX:+IgnoreUnrecognizedVMOptions",
                                "-jar",
                                config.get("validation.trino.cliJar", "/usr/bin/trino"),
                                "--server",
                                config.get("validation.trino.server"),
                                "--output-format",
                                "TSV",
                                "--execute",
                                sql));
        return suite.kubectlStdout(
                config.number("spark.timeoutSeconds"), arguments.toArray(String[]::new));
    }

    private static boolean isNull(String value) {
        String text = unquote(value);
        return text.isEmpty() || text.equals("NULL");
    }

    private static String identifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String unquote(String value) {
        String stripped = value.strip();
        if (stripped.startsWith("\"") && stripped.endsWith("\"")) {
            return stripped.substring(1, stripped.length() - 1).replace("\"\"", "\"");
        }
        return stripped;
    }
}
