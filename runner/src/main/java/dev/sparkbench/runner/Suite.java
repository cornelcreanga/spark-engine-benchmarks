package dev.sparkbench.runner;

import static dev.sparkbench.runner.Manifests.*;

import dev.sparkbench.core.*;

import java.nio.file.*;
import java.time.*;
import java.util.*;

final class Suite {
    boolean safeToRestore = true;
    final Config base;
    final Path output;

    Suite(Config c) throws Exception {
        base = c;
        output =
                Path.of(
                        c.get("results.directory"),
                        Instant.now().toString().replace(":", "-")
                                + "-"
                                + UUID.randomUUID().toString().substring(0, 8));
        Files.createDirectories(output);
    }

    String kubectl(String... args) throws Exception {
        return kubectl(600, args);
    }

    String kubectl(long seconds, String... args) throws Exception {
        return Commands.run(kubectlCommand(args), seconds);
    }

    String kubectlStdout(long seconds, String... args) throws Exception {
        return Commands.stdout(kubectlCommand(args), seconds);
    }

    private List<String> kubectlCommand(String... args) {
        List<String> cmd =
                new ArrayList<>(
                        List.of(
                                "kubectl",
                                "--context",
                                base.get("k8s.context"),
                                "-n",
                                base.get("k8s.namespace")));
        cmd.addAll(List.of(args));
        return cmd;
    }

    void apply(Path path) throws Exception {
        kubectl("apply", "-f", path.toString());
    }

    void packages(Config c) throws Exception {
        packagePrefix(c, "benchmark.");
        packagePrefix(c, "engine.");
    }

    private void packagePrefix(Config c, String prefix) throws Exception {
        if (c.all().containsKey(prefix + "helm.chart")) {
            List<String> cmd =
                    new ArrayList<>(
                            List.of(
                                    "helm",
                                    "upgrade",
                                    "--install",
                                    c.get(prefix + "helm.release"),
                                    c.get(prefix + "helm.chart"),
                                    "--version",
                                    c.get(prefix + "helm.version"),
                                    "--namespace",
                                    c.get(prefix + "helm.namespace", c.get("k8s.namespace")),
                                    "--create-namespace",
                                    "--kube-context",
                                    c.get("k8s.context"),
                                    "--wait",
                                    "--timeout",
                                    "5m"));
            if (c.all().containsKey(prefix + "helm.values")) {
                cmd.addAll(List.of("--values", c.get(prefix + "helm.values")));
            }
            System.out.print(Commands.run(cmd));
        }
        if (c.all().containsKey(prefix + "manifest")) {
            apply(Path.of(c.get(prefix + "manifest")));
        }
    }

    Path job(Config c, String action, String label) throws Exception {
        String name = "sb-" + label + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path dir = output.resolve(name);
        Files.createDirectories(dir);
        Path properties = dir.resolve("job.properties");
        c.store(properties);
        Path config = dir.resolve("configmap.json");
        write(
                config,
                resource(
                        "v1",
                        "ConfigMap",
                        name,
                        c,
                        "data",
                        map("job.properties", Files.readString(properties))));
        Path manifest = dir.resolve("application.json");
        write(manifest, spark(c, name, action));
        apply(config);
        safeToRestore = false;
        apply(manifest);
        Exception failure = null;
        try {
            long deadline = System.nanoTime() + c.number("spark.timeoutSeconds") * 1_000_000_000L;
            while (true) {
                String state =
                        kubectl(
                                        "get",
                                        "sparkapplication",
                                        name,
                                        "-o",
                                        "jsonpath={.status.applicationState.state}")
                                .trim();
                if (state.equals("COMPLETED")) {
                    safeToRestore = true;
                    break;
                }
                if (Set.of("FAILED", "FAILED_SUBMISSION", "SUBMISSION_FAILED", "INVALIDATING")
                        .contains(state)) {
                    throw new IllegalStateException(name + " ended with " + state);
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(name + " exceeded timeout");
                }
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            failure = e;
        } finally {
            try {
                String status = kubectl("get", "sparkapplication", name, "-o", "json");
                Files.writeString(dir.resolve("status.json"), status);
            } catch (Exception e) {
                Files.writeString(dir.resolve("status-error.txt"), e.toString());
            }
            try {
                String logs = kubectl("logs", name + "-driver");
                Files.writeString(dir.resolve("driver.log"), logs);
                String result =
                        logs.lines()
                                .filter(l -> l.startsWith("BENCH_RESULT "))
                                .map(l -> l.substring(13))
                                .reduce("", (a, b) -> a + b + "\n");
                Files.writeString(dir.resolve("results.jsonl"), result);
                String validation =
                        logs.lines()
                                .filter(line -> line.startsWith("BENCH_VALIDATION "))
                                .map(line -> line.substring("BENCH_VALIDATION ".length()))
                                .collect(java.util.stream.Collectors.joining("\n"));
                if (!validation.isBlank()) {
                    Files.writeString(dir.resolve("validation.jsonl"), validation + "\n");
                }
                if (action.equals("validate") && validation.isBlank() && failure == null) {
                    failure =
                            new IllegalStateException("Completed job emitted no validation result");
                }
                if (action.equals("compact") && result.isBlank() && failure == null) {
                    failure =
                            new IllegalStateException("Completed job emitted no benchmark result");
                }
            } catch (Exception e) {
                Files.writeString(dir.resolve("logs-error.txt"), e.toString());
                if (failure == null) {
                    failure = e;
                }
            }
            try {
                Files.writeString(
                        dir.resolve("pods.json"),
                        kubectl(
                                "get",
                                "pods",
                                "-l",
                                "sparkoperator.k8s.io/app-name=" + name,
                                "-o",
                                "json"));
            } catch (Exception ignored) {
            }
        }
        if (action.equals("compact")
                && (!Files.exists(dir.resolve("results.jsonl"))
                        || Files.readString(dir.resolve("results.jsonl")).isBlank())) {
            write(
                    dir.resolve("runner-failure.json"),
                    map(
                            "engine",
                            c.get("engine.name"),
                            "status",
                            "failed",
                            "error",
                            failure == null ? "Missing driver result" : failure.toString()));
            Files.writeString(
                    dir.resolve("results.jsonl"),
                    Json.encode(
                                    map(
                                            "engine",
                                            c.get("engine.name"),
                                            "image",
                                            c.get("engine.image"),
                                            "status",
                                            "failed",
                                            "driverReport",
                                            false,
                                            "error",
                                            failure == null
                                                    ? "Missing driver result"
                                                    : failure.toString(),
                                            "baselineRestored",
                                            null))
                            + "\n");
        }
        if (failure != null) {
            if (!safeToRestore) {
                try {
                    kubectl(
                            "delete",
                            "sparkapplication",
                            name,
                            "--ignore-not-found=true",
                            "--cascade=foreground",
                            "--wait=true",
                            "--timeout=120s");
                    kubectl(
                            "wait",
                            "--for=delete",
                            "pod",
                            "-l",
                            "sparkoperator.k8s.io/app-name=" + name,
                            "--timeout=120s");
                    safeToRestore = true;
                } catch (Exception stop) {
                    failure.addSuppressed(stop);
                }
            }
            throw failure;
        }
        System.out.println("Completed " + name + "; artifacts: " + dir);
        return dir;
    }

    void run(List<Config> engines, Config control) throws Exception {
        for (Config engine : engines) {
            packages(engine);
            for (int repetition = 1;
                    repetition <= base.integer("benchmark.repetitions");
                    repetition++) {
                job(control, "reset", "prepare");
                Exception failure = null;
                Path benchmarkDirectory = null;
                boolean validationEnabled = base.validationEnabled();
                boolean validatorsPassed = false;
                boolean ossSparkPassed = false;
                boolean trinoPassed = false;
                boolean baselineRestored = false;
                try {
                    String label = engine.get("engine.name") + "-r" + repetition;
                    benchmarkDirectory =
                            job(engine.with("benchmark.deferRestore", "true")
                                    .with("validation.enabled", Boolean.toString(validationEnabled)),
                                    "compact", label);
                    if (validationEnabled) {
                        job(control, "validate", label + "-oss-validation");
                        ossSparkPassed = true;
                        if (base.flag("validation.trino.enabled")) {
                            TrinoValidation.run(base, this, label);
                            trinoPassed = true;
                        }
                        validatorsPassed = true;
                    } else {
                        System.out.println("Validation skipped by configuration for " + label);
                    }
                } catch (Exception e) {
                    failure = e;
                } finally {
                    try {
                        if (!safeToRestore) {
                            throw new IllegalStateException(
                                    "Cannot verify the engine driver stopped; refusing concurrent"
                                            + " snapshot restoration. Stop the application and run"
                                            + " reset.");
                        }
                        job(control, "reset", "restore");
                        baselineRestored = true;
                    } catch (Exception e) {
                        if (failure == null) {
                            failure = e;
                        } else {
                            failure.addSuppressed(e);
                        }
                    }
                    if (benchmarkDirectory != null) {
                        write(
                                benchmarkDirectory.resolve("verification.json"),
                                map(
                                        "status",
                                        failure == null ? "success" : "failed",
                                        "validationEnabled",
                                        validationEnabled,
                                        "validationStatus",
                                        validationEnabled ? (validatorsPassed ? "passed" : "failed") : "skipped",
                                        "validatorsPassed",
                                        validationEnabled ? validatorsPassed : null,
                                        "ossSparkValidation",
                                        validationEnabled ? ossSparkPassed : null,
                                        "trinoValidation",
                                        validationEnabled && base.flag("validation.trino.enabled") ? trinoPassed : null,
                                        "trinoValidationEnabled",
                                        validationEnabled && base.flag("validation.trino.enabled"),
                                        "baselineRestored",
                                        baselineRestored,
                                        "error",
                                        failure == null ? null : failure.toString()));
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        }
    }
}
