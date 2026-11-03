#!/usr/bin/env python3
"""Real LocalStack/REST/Spark/Trino check in disposable Docker containers.

Run as: sg docker -c 'python3 scripts/test-docker-readers.py'
No Kubernetes cluster is needed. All created containers and the network are removed.
"""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
NAME = "sparkbench-readers-" + uuid.uuid4().hex[:8]
ARTIFACTS = ROOT / "build" / NAME
ARTIFACTS.mkdir(parents=True)
containers = []


def run(arguments, **kwargs):
    return subprocess.run(
        arguments, check=True, capture_output=True, text=True, **kwargs
    ).stdout.strip()


def docker(*arguments):
    return run(["docker", *arguments])


def wait_for(url, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if response.status == 200:
                    return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Service did not become ready: " + url)


def start(role, image, *arguments):
    name = NAME + "-" + role
    docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        NAME,
        "--network-alias",
        role,
        *arguments,
        image,
    )
    containers.append(name)
    return name


def port(name, container_port):
    return docker("port", name, str(container_port)).splitlines()[0].rsplit(":", 1)[1]


try:
    docker("network", "create", NAME)
    localstack = start(
        "localstack",
        "localstack/localstack:4.10.0",
        "-p",
        "127.0.0.1::4566",
        "-e",
        "SERVICES=s3",
        "-e",
        "AWS_DEFAULT_REGION=us-east-1",
    )
    s3_endpoint = "http://127.0.0.1:" + port(localstack, 4566)
    wait_for(s3_endpoint + "/_localstack/health")
    docker(
        "exec",
        localstack,
        "awslocal",
        "s3api",
        "create-bucket",
        "--bucket",
        "spark-bench",
    )
    catalog = start(
        "iceberg-rest",
        "apache/iceberg-rest-fixture:latest",
        "-p",
        "127.0.0.1::8181",
        "-e",
        "CATALOG_WAREHOUSE=s3://spark-bench/warehouse",
        "-e",
        "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
        "-e",
        "CATALOG_S3_ENDPOINT=http://localstack:4566",
        "-e",
        "CATALOG_S3_PATH__STYLE__ACCESS=true",
        "-e",
        "AWS_ACCESS_KEY_ID=test",
        "-e",
        "AWS_SECRET_ACCESS_KEY=test",
        "-e",
        "AWS_REGION=us-east-1",
    )
    rest_uri = "http://127.0.0.1:" + port(catalog, 8181)
    wait_for(rest_uri + "/v1/config")
    subprocess.run(
        ["./gradlew", ":runner:installDist", ":runner:classes", "--no-daemon"],
        check=True,
        stdout=(ARTIFACTS / "build.log").open("w"),
        stderr=subprocess.STDOUT,
    )
    subprocess.run(
        ["runner/build/install/runner/bin/runner", "plan", "config/smoke.properties"],
        check=True,
        stdout=(ARTIFACTS / "plan.log").open("w"),
        stderr=subprocess.STDOUT,
    )
    resources = json.loads((ROOT / "build/infrastructure/local.json").read_text())[
        "items"
    ]
    trino_config = next(
        item["data"]
        for item in resources
        if item["kind"] == "ConfigMap" and item["metadata"]["name"] == "trino-config"
    )
    with tempfile.TemporaryDirectory(prefix="sparkbench-trino-") as temporary:
        temporary = Path(temporary)
        (temporary / "catalog").mkdir()
        for name, value in trino_config.items():
            path = (
                temporary / ("catalog" if name == "iceberg.properties" else "") / name
            )
            path.write_text(value)
        trino = start(
            "trino",
            "trinodb/trino:476",
            "-p",
            "127.0.0.1::8080",
            "--mount",
            f"type=bind,src={temporary},dst=/etc/trino,readonly",
        )
        wait_for("http://127.0.0.1:" + port(trino, 8080) + "/v1/info", seconds=240)
        environment = dict(
            os.environ,
            SPARK_LOCAL_IP="127.0.0.1",
            BENCH_TEST_REST_URI=rest_uri,
            BENCH_TEST_S3_ENDPOINT=s3_endpoint,
            BENCH_TEST_KEEP_COMPACTED="true",
        )
        with (ARTIFACTS / "spark.log").open("w") as log:
            subprocess.run(
                ["./gradlew", ":jobs:verifyEventTable", "--no-daemon"],
                env=environment,
                check=True,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        # Exercise the production Trino validator; only its transport changes to docker exec.
        wrapper = temporary / "kubectl"
        wrapper.write_text(
            "#!/usr/bin/env python3\nimport os, sys\n"
            f"os.execvp('docker', ['docker', 'exec', {trino!r}, *sys.argv[sys.argv.index('--') + 1:]])\n"
        )
        wrapper.chmod(0o755)
        profile = temporary / "validation.properties"
        profile.write_text(
            f"config.parent={ROOT / 'config/smoke.properties'}\n"
            "catalog.namespace=event_model_test\ncatalog.table=events\n"
            f"results.directory={ARTIFACTS / 'trino-results'}\n"
        )
        with (ARTIFACTS / "trino-validation.log").open("w") as log:
            subprocess.run(
                [
                    "runner/build/install/runner/bin/runner",
                    "validate-trino",
                    str(profile),
                ],
                check=True,
                env=dict(
                    os.environ, PATH=str(temporary) + os.pathsep + os.environ["PATH"]
                ),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
    print(
        "Docker readers integration passed: Spark + Trino, LocalStack, REST catalog, compacted files, statistics"
    )
    print(ARTIFACTS)
finally:
    for name in reversed(containers):
        try:
            (ARTIFACTS / (name + ".log")).write_text(docker("logs", name))
        except Exception:
            pass
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.run(["docker", "network", "rm", NAME], capture_output=True)
