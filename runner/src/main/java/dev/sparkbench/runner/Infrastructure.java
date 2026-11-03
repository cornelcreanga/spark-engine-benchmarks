package dev.sparkbench.runner;

import static dev.sparkbench.runner.Manifests.*;

import dev.sparkbench.core.Config;

import java.nio.file.*;
import java.util.*;

final class Infrastructure {
    static Object env(String name, String value) {
        return map("name", name, "value", value);
    }

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

    static Map<String, Object> service(String name, int port, Config c) {
        return resource(
                "v1",
                "Service",
                name,
                c,
                "spec",
                map(
                        "selector",
                        map("app", name),
                        "ports",
                        List.of(map("port", port, "targetPort", port))));
    }

    static Map<String, Object> deployment(
            String name,
            String image,
            int port,
            List<Object> env,
            String volume,
            String mount,
            Config c) {
        return resource(
                "apps/v1",
                "Deployment",
                name,
                c,
                "spec",
                map(
                        "replicas",
                        1,
                        "strategy",
                        map("type", "Recreate"),
                        "selector",
                        map("matchLabels", map("app", name)),
                        "template",
                        map(
                                "metadata",
                                map("labels", map("app", name)),
                                "spec",
                                map(
                                        "containers",
                                        List.of(
                                                map(
                                                        "name",
                                                        name,
                                                        "image",
                                                        image,
                                                        "ports",
                                                        List.of(map("containerPort", port)),
                                                        "env",
                                                        env,
                                                        "volumeMounts",
                                                        List.of(
                                                                map(
                                                                        "name",
                                                                        "data",
                                                                        "mountPath",
                                                                        mount)),
                                                        "readinessProbe",
                                                        map(
                                                                "tcpSocket",
                                                                map("port", port),
                                                                "initialDelaySeconds",
                                                                5,
                                                                "periodSeconds",
                                                                5))),
                                        "volumes",
                                        List.of(
                                                map(
                                                        "name",
                                                        "data",
                                                        "persistentVolumeClaim",
                                                        map("claimName", volume)))))));
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
        items.add(pvc("localstack-data", c.get("localstack.storage", "100Gi"), c));
        items.add(pvc("catalog-data", "1Gi", c));
        items.add(service("localstack", 4566, c));
        items.add(service("iceberg-rest", 8181, c));
        List<Object> localEnv =
                new ArrayList<>(
                        List.of(
                                env("SERVICES", "s3"),
                                env("PERSISTENCE", "1"),
                                env("AWS_DEFAULT_REGION", c.get("s3.region"))));
        // Optional: create this Secret yourself if your selected LocalStack image needs a token.
        Map<String, Object> local =
                deployment(
                        "localstack",
                        c.get("localstack.image"),
                        4566,
                        localEnv,
                        "localstack-data",
                        "/var/lib/localstack",
                        c);
        if (c.all().containsKey("localstack.authSecret")) {
            localEnv.add(
                    map(
                            "name",
                            "LOCALSTACK_AUTH_TOKEN",
                            "valueFrom",
                            map(
                                    "secretKeyRef",
                                    map(
                                            "name",
                                            c.get("localstack.authSecret"),
                                            "key",
                                            "auth-token"))));
        }
        items.add(local);
        items.add(
                deployment(
                        "iceberg-rest",
                        c.get("catalog.image"),
                        8181,
                        new ArrayList<>(
                                List.of(
                                        env("CATALOG_WAREHOUSE", c.get("catalog.warehouse")),
                                        env(
                                                "CATALOG_CATALOG__IMPL",
                                                "org.apache.iceberg.jdbc.JdbcCatalog"),
                                        env(
                                                "CATALOG_URI",
                                                "jdbc:sqlite:/catalog/iceberg.db?journal_mode=WAL"),
                                        env(
                                                "CATALOG_IO__IMPL",
                                                "org.apache.iceberg.aws.s3.S3FileIO"),
                                        env("CATALOG_S3_ENDPOINT", c.get("s3.endpoint")),
                                        env("CATALOG_S3_PATH__STYLE__ACCESS", "true"),
                                        env("AWS_ACCESS_KEY_ID", c.get("s3.accessKey")),
                                        env("AWS_SECRET_ACCESS_KEY", c.get("s3.secretKey")),
                                        env("AWS_REGION", c.get("s3.region")))),
                        "catalog-data",
                        "/catalog",
                        c));
        if (c.flag("validation.trino.enabled")) {
            items.add(trinoConfig(c));
            items.add(service("trino", 8080, c));
            items.add(trinoDeployment(c));
        }
        Path path = Path.of("build/infrastructure/local.json");
        write(path, map("apiVersion", "v1", "kind", "List", "items", items));
        return path;
    }

    private static Map<String, Object> trinoConfig(Config config) {
        String connector =
                "connector.name=iceberg\n"
                        + "iceberg.catalog.type=rest\n"
                        + "iceberg.rest-catalog.uri="
                        + config.get("catalog.uri")
                        + "\n"
                        + "fs.native-s3.enabled=true\n"
                        + "s3.endpoint="
                        + config.get("s3.endpoint")
                        + "\n"
                        + "s3.region="
                        + config.get("s3.region")
                        + "\n"
                        + "s3.path-style-access=true\n"
                        + "s3.aws-access-key="
                        + config.get("s3.accessKey")
                        + "\n"
                        + "s3.aws-secret-key="
                        + config.get("s3.secretKey")
                        + "\n";
        return resource(
                "v1",
                "ConfigMap",
                "trino-config",
                config,
                "data",
                map(
                        "iceberg.properties",
                        connector,
                        "config.properties",
                        "coordinator=true\n"
                                + "node-scheduler.include-coordinator=true\n"
                                + "http-server.http.port=8080\n"
                                + "discovery.uri=http://localhost:8080\n"
                                + "query.max-memory=256MB\n"
                                + "query.max-memory-per-node=256MB\n"
                                + "memory.heap-headroom-per-node=256MB\n"
                                + "task.concurrency=2\n"
                                + "task.max-worker-threads=4\n"
                                + "task.min-drivers=4\n"
                                + "task.max-drivers-per-task=4\n",
                        "jvm.config",
                        "-server\n"
                            + "-Xmx2G\n"
                            + "-XX:ActiveProcessorCount=2\n"
                            + "-XX:+ExitOnOutOfMemoryError\n",
                        "node.properties",
                        "node.environment=benchmark\nnode.data-dir=/tmp/trino\n"));
    }

    private static Map<String, Object> trinoDeployment(Config config) {
        List<Object> configFiles =
                List.of(
                        map("key", "iceberg.properties", "path", "catalog/iceberg.properties"),
                        map("key", "config.properties", "path", "config.properties"),
                        map("key", "jvm.config", "path", "jvm.config"),
                        map("key", "node.properties", "path", "node.properties"));
        Map<String, Object> container =
                map(
                        "name", "trino",
                        "image", config.get("validation.trino.image"),
                        "resources",
                                map(
                                        "requests", map("cpu", "500m", "memory", "2Gi"),
                                        "limits", map("memory", "4Gi")),
                        "ports", List.of(map("containerPort", 8080)),
                        "readinessProbe",
                                map(
                                        "httpGet", map("path", "/v1/info", "port", 8080),
                                        "initialDelaySeconds", 10,
                                        "periodSeconds", 5),
                        "volumeMounts", List.of(map("name", "config", "mountPath", "/etc/trino")));
        Map<String, Object> pod =
                map(
                        "containers", List.of(container),
                        "volumes",
                                List.of(
                                        map(
                                                "name",
                                                "config",
                                                "configMap",
                                                map(
                                                        "name",
                                                        "trino-config",
                                                        "items",
                                                        configFiles))));
        return resource(
                "apps/v1",
                "Deployment",
                "trino",
                config,
                "spec",
                map(
                        "replicas", 1,
                        "selector", map("matchLabels", map("app", "trino")),
                        "template",
                                map("metadata", map("labels", map("app", "trino")), "spec", pod)));
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
        suite.kubectl(
                timeout + 30,
                "rollout",
                "status",
                "deployment/localstack",
                "--timeout=" + timeout + "s");
        suite.kubectl(
                timeout + 30,
                "rollout",
                "status",
                "deployment/iceberg-rest",
                "--timeout=" + timeout + "s");
        if (c.flag("validation.trino.enabled")) {
            suite.kubectl(
                    timeout + 30,
                    "rollout",
                    "status",
                    "deployment/trino",
                    "--timeout=" + timeout + "s");
        }
        String bucket = c.get("s3.bucket");
        // Idempotent creation: only an existing, readable bucket is accepted as success.
        try {
            suite.kubectl(
                    "exec",
                    "deployment/localstack",
                    "--",
                    "awslocal",
                    "s3api",
                    "head-bucket",
                    "--bucket",
                    bucket);
        } catch (Exception e) {
            suite.kubectl(
                    "exec",
                    "deployment/localstack",
                    "--",
                    "awslocal",
                    "s3api",
                    "create-bucket",
                    "--bucket",
                    bucket);
        }
    }
}
