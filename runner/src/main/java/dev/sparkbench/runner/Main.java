package dev.sparkbench.runner;

import dev.sparkbench.core.*;

import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Usage:"
                        + " plan|provision|image|prepare|generate|run|reset|validate|validate-trino|local-settings"
                        + " CONFIG [ENGINE ...]");
        }
        Config base = Config.load(Path.of(args[1]));
        base.validate();
        Config control = base.overlay(Config.load(Path.of("config/engines/oss.properties")));
        control.validate();
        List<Config> engines = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            Config c = base.overlay(Config.load(Path.of(args[i])));
            c.validate();
            engines.add(c);
        }
        if (engines.isEmpty()) {
            engines.add(control);
        }
        if (args[0].equals("local-settings")) {
            Map<String, Object> settings = new TreeMap<>();
            for (String key :
                    List.of(
                            "k8s.context",
                            "k8s.minikubeProfile",
                            "k8s.namespace",
                            "catalog.table",
                            "results.directory",
                            "dataset.targetBytes",
                            "benchmark.repetitions",
                            "local.minikube.cpus",
                            "local.minikube.memoryMiB",
                            "local.minikube.timeoutSeconds",
                            "local.minFreeDiskGiB",
                            "local.helm.version")) {
                settings.put(key, base.get(key));
            }
            List<Map<String, String>> imageDefinitions = new ArrayList<>();
            imageDefinitions.add(
                    Map.of(
                            "config",
                            "config/engines/oss.properties",
                            "image",
                            control.get("engine.image")));
            for (int index = 0; index < engines.size(); index++) {
                Config engine = engines.get(index);
                if (!engine.get("engine.operator", "spark").equals("quanton")
                        && !engine.get("engine.image").equals(control.get("engine.image"))) {
                    imageDefinitions.add(
                            Map.of("config", args[index + 2], "image", engine.get("engine.image")));
                }
            }
            settings.put("images", imageDefinitions);
            System.out.println(Json.encode(settings));
            return;
        }
        if (args[0].equals("plan")) {
            Path dir = Path.of("build/plan");
            Files.createDirectories(dir);
            Infrastructure.render(base);
            for (Config c : engines) {
                Manifests.write(
                        dir.resolve(c.get("engine.name") + ".json"),
                        Manifests.spark(c, "sb-plan-" + c.get("engine.name"), "compact"));
            }
            System.out.println(
                    "Configuration valid. SparkApplications: "
                            + dir
                            + "; infrastructure: build/infrastructure/local.json");
            return;
        }
        if (args[0].equals("image")) {
            for (Config c : engines) {
                Images.build(c);
            }
            return;
        }
        Files.createDirectories(Path.of("build/locks"));
        Path lockPath =
                Path.of(
                        "build/locks",
                        base.get("k8s.context").replaceAll("[^A-Za-z0-9-]", "_")
                                + "-"
                                + base.get("k8s.namespace")
                                + "-"
                                + base.shortTable()
                                + ".lock");
        try (FileChannel channel =
                        FileChannel.open(
                                lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock = channel.tryLock()) {
            if (lock == null) {
                throw new IllegalStateException("Another local runner is using this table");
            }
            Suite suite = new Suite(base);
            switch (args[0]) {
                case "provision" -> Infrastructure.provision(base, suite);
                case "prepare" -> {
                    suite.packages(control);
                    suite.job(control, "prepare", "prepare-input");
                }
                case "generate" -> {
                    suite.packages(control);
                    suite.job(control, "generate", "generate");
                }
                case "reset" -> suite.job(control, "reset", "reset");
                case "cleanup-outputs" -> suite.job(control, "cleanup-outputs", "cleanup");
                case "validate-trino" -> TrinoValidation.run(base, suite, "manual");
                case "validate" -> suite.job(control, "validate", "validate");
                case "run" -> suite.run(engines, control);
                default -> throw new IllegalArgumentException("Unknown command: " + args[0]);
            }
        }
    }
}
