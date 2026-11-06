package dev.sparkbench.runner;

import dev.sparkbench.core.Config;

import java.util.List;

final class NodeWarmup {
    private static String quote(String text) {
        return "'" + text.replace("'", "'\"'\"'") + "'";
    }

    static String run(Config c, String name, Suite suite) throws Exception {
        String cluster = c.get("k8s.minikubeProfile");
        String volume =
                suite.kubectlStdout(
                                30,
                                "get",
                                "pvc",
                                c.get("filesystem.claim"),
                                "-o",
                                "jsonpath={.spec.volumeName}")
                        .trim();
        String root =
                suite.kubectlStdout(30, "get", "pv", volume, "-o", "jsonpath={.spec.hostPath.path}")
                        .trim();
        if (!root.startsWith("/"))
            throw new IllegalArgumentException("Warm-cache mode requires a node hostPath PVC");
        Commands.run(
                List.of(
                        "minikube",
                        "-p",
                        cluster,
                        "cp",
                        "scripts/warm-cache.py",
                        "/tmp/sparkbench-warm-cache.py"));
        String command =
                "sudo python3 /tmp/sparkbench-warm-cache.py --directory "
                        + quote(root + "/.warm-cache/" + name)
                        + " --mount "
                        + quote(c.get("filesystem.mountPath"))
                        + " --volume-root "
                        + quote(root)
                        + " --wait "
                        + c.get("benchmark.warmCacheTimeoutSeconds")
                        + " --min-resident "
                        + c.get("benchmark.warmCacheMinResidentFraction");
        return Commands.stdout(
                List.of("minikube", "-p", cluster, "ssh", "--", command),
                c.number("benchmark.warmCacheTimeoutSeconds") + 60);
    }
}
