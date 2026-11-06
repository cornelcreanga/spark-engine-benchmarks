package dev.sparkbench.runner;

import dev.sparkbench.core.Config;

import java.nio.file.*;
import java.util.*;

final class Images {
    static Path prepare(Config c) throws Exception {
        Path dir = Path.of("build/images", c.get("engine.name"));
        Files.createDirectories(dir);
        Files.copy(
                Path.of("jobs/build/libs/spark-bench-jobs.jar"),
                dir.resolve("spark-bench-jobs.jar"),
                StandardCopyOption.REPLACE_EXISTING);
        // Isolate copied assets from the Dockerfile so custom snippets have a stable build context.
        Path extras = dir.resolve("extras");
        if (Files.exists(extras)) {
            try (var paths = Files.walk(extras)) {
                for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(p);
                }
            }
        }
        Files.createDirectories(extras);
        if (c.all().containsKey("image.extraJarsDir")) {
            try (var jars = Files.list(Path.of(c.get("image.extraJarsDir")))) {
                for (Path jar : jars.filter(p -> p.toString().endsWith(".jar")).toList()) {
                    Files.copy(jar, extras.resolve(jar.getFileName()));
                }
            }
        }
        String base = c.get("image.base");
        if (!base.matches("[a-zA-Z0-9_./:@-]+")) {
            throw new IllegalArgumentException("Invalid base image");
        }
        String version = c.get("image.icebergVersion"),
                line = c.get("image.sparkLine"),
                scala = c.get("image.scalaBinary");
        StringBuilder docker = new StringBuilder("FROM " + base + "\nUSER root\n");
        for (String artifact :
                List.of("iceberg-spark-runtime-" + line + "_" + scala, "iceberg-aws-bundle")) {
            String file = artifact + "-" + version + ".jar";
            docker.append(
                            "ADD --chown=185:0"
                                    + " https://repo.maven.apache.org/maven2/org/apache/iceberg/")
                    .append(artifact)
                    .append('/')
                    .append(version)
                    .append('/')
                    .append(file)
                    .append(" /opt/spark/jars/")
                    .append(file)
                    .append('\n');
        }
        // Explicit extra coordinates; include transitive JARs here or in extraJarsDir.
        for (String coordinate : c.get("image.mavenCoordinates", "").split(",")) {
            if (!coordinate.isBlank()) {
                String[] parts = coordinate.trim().split(":");
                if (parts.length != 3 || !coordinate.matches("[A-Za-z0-9_.:-]+")) {
                    throw new IllegalArgumentException(
                            "Expected group:artifact:version: " + coordinate);
                }
                docker.append("ADD --chown=185:0 https://repo.maven.apache.org/maven2/")
                        .append(parts[0].replace('.', '/'))
                        .append('/')
                        .append(parts[1])
                        .append('/')
                        .append(parts[2])
                        .append('/')
                        .append(parts[1])
                        .append('-')
                        .append(parts[2])
                        .append(".jar /opt/spark/jars/\n");
            }
        }
        docker.append(
                "COPY --chown=185:0 extras/ /opt/spark/jars/\n"
                        + "COPY --chown=185:0 spark-bench-jobs.jar"
                        + " /opt/spark/bench/spark-bench-jobs.jar\n");
        StringBuilder defaults = new StringBuilder();
        c.prefix("spark.conf.")
                .forEach((k, v) -> defaults.append(k).append(' ').append(v).append('\n'));
        Files.writeString(dir.resolve("spark-defaults.conf"), defaults);
        docker.append(
                "COPY --chown=185:0 spark-defaults.conf /opt/spark/conf/spark-defaults.conf\n"
                        + "RUN mkdir -p /tmp/spark-events && chmod 1777 /tmp/spark-events\n");
        if (c.all().containsKey("image.extraDockerfile")) {
            docker.append(Files.readString(Path.of(c.get("image.extraDockerfile")))).append('\n');
        }
        docker.append("USER 185\n");
        Files.writeString(dir.resolve("Dockerfile"), docker);
        return dir;
    }

    static void build(Config c) throws Exception {
        if (c.get("engine.operator", "spark").equals("quanton")) {
            System.out.println("Quanton uses its operator-managed image; no local image is built.");
            return;
        }
        Path dir = prepare(c);
        System.out.print(
                Commands.run(
                        List.of("docker", "build", "-t", c.get("engine.image"), dir.toString()),
                        3600));
        Files.writeString(
                dir.resolve("image-inspect.json"),
                Commands.run(List.of("docker", "image", "inspect", c.get("engine.image"))));
        if (c.flag("image.push")) {
            System.out.print(Commands.run(List.of("docker", "push", c.get("engine.image")), 3600));
        }
        if (c.flag("image.loadMinikube")) {
            System.out.print(
                    Commands.run(
                            List.of(
                                    "minikube",
                                    "-p",
                                    c.get("k8s.minikubeProfile"),
                                    "image",
                                    "load",
                                    c.get("engine.image")),
                            1200));
        }
    }
}
