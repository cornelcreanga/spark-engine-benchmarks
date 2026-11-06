package dev.sparkbench.runner;

import static dev.sparkbench.runner.Manifests.*;

import dev.sparkbench.core.Config;

import java.nio.file.*;
import java.util.*;

final class Infrastructure {
    static Map<String, Object> pvc(String name, String size, Config c) {
        return resource(
                "v1",
                "PersistentVolumeClaim",
                name,
                c,
                "spec",
                map(
                        "accessModes",
                        List.of("ReadWriteOnce"),
                        "resources",
                        map("requests", map("storage", size))));
    }

    static Path render(Config c) throws Exception {
        List<Object> items = new ArrayList<>();
        String ns = c.get("k8s.namespace");
        items.add(map("apiVersion", "v1", "kind", "Namespace", "metadata", map("name", ns)));
        items.add(resource("v1", "ServiceAccount", "spark-bench", c));
        items.add(
                resource(
                        "rbac.authorization.k8s.io/v1",
                        "Role",
                        "spark-bench",
                        c,
                        "rules",
                        List.of(
                                map(
                                        "apiGroups",
                                        List.of(""),
                                        "resources",
                                        List.of(
                                                "pods",
                                                "services",
                                                "configmaps",
                                                "persistentvolumeclaims"),
                                        "verbs",
                                        List.of(
                                                "create",
                                                "get",
                                                "list",
                                                "watch",
                                                "delete",
                                                "deletecollection",
                                                "patch",
                                                "update")))));
        items.add(
                resource(
                        "rbac.authorization.k8s.io/v1",
                        "RoleBinding",
                        "spark-bench",
                        c,
                        "subjects",
                        List.of(
                                map(
                                        "kind",
                                        "ServiceAccount",
                                        "name",
                                        "spark-bench",
                                        "namespace",
                                        ns)),
                        "roleRef",
                        map(
                                "apiGroup",
                                "rbac.authorization.k8s.io",
                                "kind",
                                "Role",
                                "name",
                                "spark-bench")));
        if (c.filesystemStorage()) {
            items.add(pvc(c.get("filesystem.claim"), c.get("filesystem.storage", "120Gi"), c));
        }
        Path path = Path.of("build/infrastructure/local.json");
        write(path, map("apiVersion", "v1", "kind", "List", "items", items));
        return path;
    }

    static void provision(Config c, Suite suite) throws Exception {
        long timeout = Long.parseLong(c.get("infrastructure.timeoutSeconds", "900"));
        if (timeout <= 0) {
            throw new IllegalArgumentException("infrastructure.timeoutSeconds must be positive");
        }
        System.out.print(
                Commands.run(
                        List.of(
                                "helm",
                                "repo",
                                "add",
                                "spark-operator",
                                "https://kubeflow.github.io/spark-operator",
                                "--force-update")));
        System.out.print(Commands.run(List.of("helm", "repo", "update", "spark-operator")));
        // Apply namespace/RBAC/storage first; Helm watches this explicit namespace.
        suite.apply(render(c));
        System.out.print(
                Commands.run(
                        List.of(
                                "helm",
                                "upgrade",
                                "--install",
                                c.get("operator.release"),
                                c.get("operator.chart"),
                                "--version",
                                c.get("operator.chartVersion"),
                                "--namespace",
                                c.get("operator.namespace"),
                                "--create-namespace",
                                "--kube-context",
                                c.get("k8s.context"),
                                "--set",
                                "spark.jobNamespaces={" + c.get("k8s.namespace") + "}",
                                "--wait",
                                "--timeout",
                                timeout + "s"),
                        timeout + 60));
    }
}
