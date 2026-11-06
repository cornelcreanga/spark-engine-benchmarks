package dev.sparkbench.runner;

import dev.sparkbench.core.Config;

import java.nio.file.*;
import java.util.*;

/** Operator integration. Credential values are read only by Helm from outside the project. */
final class Quanton {
    private static final Set<String> prepared = new HashSet<>();

    static void prepare(Config c, Suite suite) throws Exception {
        String key = c.get("k8s.context") + "/" + c.get("k8s.namespace");
        if (prepared.contains(key)) return;
        if (!c.filesystemStorage()) {
            throw new IllegalArgumentException(
                    "Quanton currently requires the shared filesystem profile");
        }
        Path values =
                Path.of(
                                c.get(
                                        "quanton.valuesFile",
                                        Path.of(
                                                        System.getProperty("user.home"),
                                                        "onehouse-values.yaml")
                                                .toString()))
                        .toRealPath();
        if (values.startsWith(Path.of("").toRealPath())) {
            throw new IllegalArgumentException(
                    "Keep Quanton credential values outside the project");
        }
        // Do not template/copy the secret values or save rendered Helm manifests to artifacts.
        Commands.run(
                List.of(
                        "helm",
                        "upgrade",
                        "--install",
                        "quanton-operator",
                        "oci://registry-1.docker.io/onehouseai/quanton-operator",
                        "--version",
                        c.get("quanton.chartVersion"),
                        "--namespace",
                        "quanton-operator",
                        "--create-namespace",
                        "--kube-context",
                        c.get("k8s.context"),
                        "--set",
                        "quantonOperator.jobNamespaces={" + c.get("k8s.namespace") + "}",
                        "--set",
                        "onehouseConfig.enableAIAgent=false",
                        "--values",
                        values.toString(),
                        "--wait",
                        "--timeout",
                        "5m"),
                360);
        String volume =
                suite.kubectlStdout(
                                60,
                                "get",
                                "pvc",
                                c.get("filesystem.claim"),
                                "-o",
                                "jsonpath={.spec.volumeName}")
                        .trim();
        String host =
                suite.kubectlStdout(60, "get", "pv", volume, "-o", "jsonpath={.spec.hostPath.path}")
                        .trim();
        if (!host.matches("/[A-Za-z0-9_./-]+")) {
            throw new IllegalStateException("Quanton local mode requires a hostPath-backed PVC");
        }
        // hostPath does not recursively apply fsGroup. Allow the vendor's UID to share
        // existing OSS-owned tables through group 185; setgid keeps new paths interoperable.
        Commands.run(
                List.of(
                        "minikube",
                        "-p",
                        c.get("k8s.minikubeProfile"),
                        "ssh",
                        "--",
                        "sudo mkdir -p '"
                                + host
                                + "/.warm-cache' && sudo chgrp -R 185 '"
                                + host
                                + "/iceberg' '"
                                + host
                                + "/.warm-cache' && sudo chmod -R g+rwX '"
                                + host
                                + "/iceberg' '"
                                + host
                                + "/.warm-cache' && sudo find '"
                                + host
                                + "/iceberg' '"
                                + host
                                + "/.warm-cache' -type d -exec chmod g+s {} +"));
        String directory = host + "/.sparkbench/jobs";
        Commands.run(
                List.of(
                        "minikube",
                        "-p",
                        c.get("k8s.minikubeProfile"),
                        "ssh",
                        "--",
                        "sudo mkdir -p '"
                                + directory
                                + "' && sudo chmod 755 '"
                                + host
                                + "/.sparkbench' '"
                                + directory
                                + "'"));
        String temporary = "/tmp/sparkbench-quanton-jobs.jar";
        Commands.run(
                List.of(
                        "minikube",
                        "-p",
                        c.get("k8s.minikubeProfile"),
                        "cp",
                        "jobs/build/libs/spark-bench-jobs.jar",
                        temporary));
        Commands.run(
                List.of(
                        "minikube",
                        "-p",
                        c.get("k8s.minikubeProfile"),
                        "ssh",
                        "--",
                        "sudo install -m 644 '"
                                + temporary
                                + "' '"
                                + directory
                                + "/spark-bench-jobs.jar'"));
        prepared.add(key);
        System.out.println("Quanton operator ready; benchmark JAR staged on shared PVC.");
    }

    static void awaitApplication(Config c, String name, Suite suite) throws Exception {
        long deadline =
                System.nanoTime() + c.number("quanton.submissionTimeoutSeconds") * 1_000_000_000L;
        while (System.nanoTime() < deadline) {
            String state =
                    suite.kubectlStdout(
                                    60,
                                    "get",
                                    "quantonsparkapplication",
                                    name,
                                    "-o",
                                    "jsonpath={.status.phase}{'|'}{.status.sparkApplicationName}")
                            .trim();
            String[] parts = state.split("\\|", -1);
            if (parts[0].equalsIgnoreCase("Failed")) {
                throw new IllegalStateException(
                        "Quanton submission failed for "
                                + name
                                + "; inspect its Kubernetes status/events");
            }
            if (parts.length == 2 && !parts[1].isBlank()) {
                if (!parts[1].equals(name)) {
                    throw new IllegalStateException(
                            "Unexpected Quanton child application name: " + parts[1]);
                }
                return;
            }
            Thread.sleep(3000);
        }
        throw new IllegalStateException(
                "Timed out awaiting Quanton child SparkApplication for " + name);
    }
}
