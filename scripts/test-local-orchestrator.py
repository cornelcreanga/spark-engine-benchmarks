#!/usr/bin/env python3
"""Exercise the real Bash workflow with fake external services; no cluster is contacted."""

import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

FAKE_COMMAND = r"""#!/usr/bin/env python3
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tarfile

name = Path(sys.argv[0]).name
args = sys.argv[1:]
root = Path(os.environ['FAKE_PROJECT'])
log = root / 'commands.jsonl'
with log.open('a') as output:
    output.write(json.dumps({'command': name, 'args': args}) + '\n')
state_file = root / 'state.json'
state = json.loads(state_file.read_text()) if state_file.exists() else {}
def save():
    state_file.write_text(json.dumps(state))
def properties(path, seen=None):
    path = Path(path).resolve()
    seen = set() if seen is None else seen
    if path in seen:
        raise RuntimeError('cycle')
    seen.add(path)
    values = {}
    for line in path.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip()
    parent = values.pop('config.parent', None)
    result = properties(path.parent / parent, seen) if parent else {}
    result.update(values)
    return result
if name == 'docker':
    if 'info' in args:
        if os.environ.get('FAKE_DOCKER_DOWN'):
            sys.exit(1)
        if '--format' in args:
            print(json.dumps({'NCPU': 16, 'MemTotal': int(os.environ.get('FAKE_HOST_MIB', '49152')) * 1024**2}))
    elif 'inspect' in args and '--type' in args:
        print(json.dumps({'NanoCpus': 16 * 10**9, 'Memory': int(os.environ.get('FAKE_CLUSTER_MIB', '38912')) * 1024**2}))
    elif 'inspect' in args and os.environ.get('FAKE_MISSING_IMAGE'):
        sys.exit(1)
elif name == 'sg':
    sys.exit(1)
elif name == 'minikube':
    if 'start' in args:
        if os.environ.get('FAKE_START_FAIL'):
            sys.exit(17)
        state['cluster'] = True
        save()
    elif 'kubectl' in args:
        forwarded = args[args.index('--') + 1:]
        os.execv(str(root / 'fake-bin/kubectl'), ['kubectl', *forwarded])
    elif 'status' in args:
        print(json.dumps({'Host': 'Running'}))
    elif 'ssh' in args:
        free_kib = int(os.environ.get('FAKE_DISK_GIB', '200')) * 1024**2
        print(f'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/fake 999999999 1 {free_kib} 1% /')
elif name == 'kubectl':
    if 'namespace' in args:
        sys.exit(0 if os.environ.get('FAKE_ACTIVE_WRITER') or state.get('infra') else 1)
    if 'crd' in args and 'get' in args:
        sys.exit(0 if os.environ.get('FAKE_ACTIVE_WRITER') or state.get('infra') else 1)
    if 'get' in args and 'nodes' in args:
        print(json.dumps({'items': [{'status': {'capacity': {'cpu': '16', 'memory': '48Gi'}}}]}))
    elif 'get' in args and 'sparkapplications' in args:
        items = [{'metadata': {'name': 'previous-writer'}, 'status': {'applicationState': {'state': 'RUNNING'}}}] if os.environ.get('FAKE_ACTIVE_WRITER') else []
        print(json.dumps({'items': items}))
    else:
        print('{}')
elif name == 'helm':
    if os.environ.get('FAKE_HELM_UNAVAILABLE'):
        sys.exit(127)
    print('v3.19.0+fake')
elif name == 'curl':
    destination = Path(args[args.index('-o') + 1])
    if destination.name.endswith('.sha256'):
        digest = hashlib.sha256(destination.with_suffix('').read_bytes()).hexdigest()
        if os.environ.get('FAKE_BAD_CHECKSUM'):
            digest = '0' * 64
        destination.write_text(digest + '  helm.tar.gz\n')
    else:
        content = b'#!/bin/sh\nprintf "v3.19.0+bootstrapped\\n"\n'
        with tarfile.open(destination, 'w:gz') as archive:
            item = tarfile.TarInfo('linux-amd64/helm')
            item.size = len(content)
            item.mode = 0o755
            archive.addfile(item, io.BytesIO(content))
elif name == 'gradlew':
    if os.environ.get('FAKE_BUILD_FAIL'):
        sys.exit(13)
elif name == 'runner':
    action = args[0]
    config = properties(args[1])
    if action == 'local-settings':
        settings = {key: value for key, value in config.items() if key.startswith('local.') or key in ['k8s.context', 'k8s.minikubeProfile', 'k8s.namespace', 'catalog.table', 'results.directory']}
        control = properties(root / 'config/engines/oss.properties')
        settings['images'] = [{'config': 'config/engines/oss.properties', 'image': control['engine.image']}]
        for path in args[2:]:
            engine = properties(root / path)
            if engine['engine.image'] != control['engine.image']:
                settings['images'].append({'config': path, 'image': engine['engine.image']})
        print(json.dumps(settings))
    elif action == 'provision':
        state['infra'] = True
        save()
    elif action == 'prepare':
        if os.environ.get('FAKE_INPUT_CONFLICT') and config['catalog.table'] in ['machine_events_uuid_string', 'machine_events_smoke_uuid_string']:
            print('Existing input is incomplete; use --fresh-input', file=sys.stderr)
            sys.exit(21)
        state['input'] = config['catalog.table']
        save()
    elif action == 'run':
        state['validationEnabled'] = config.get('validation.enabled', 'true')
        if os.environ.get('FAKE_RUN_FAIL'):
            print('Independent validation failed; baseline restoration attempted', file=sys.stderr)
            sys.exit(23)
        output = Path(config['results.directory']) / 'sample'
        output.mkdir(parents=True)
        (output / 'results.jsonl').write_text(json.dumps({'engine': 'oss', 'status': 'success', 'durationSeconds': 1.25}) + '\n')
        (output / 'verification.json').write_text(json.dumps({'status': 'success', 'validatorsPassed': True, 'baselineRestored': True, 'trinoValidationEnabled': True}))
        state['repetitions'] = config['benchmark.repetitions']
        save()
"""


class LocalOrchestratorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="sparkbench local test ")
        self.root = Path(self.temporary.name)
        shutil.copytree(ROOT / "config", self.root / "config")
        (self.root / "scripts").mkdir()
        for name in ["run-local.sh", "summarize.py"]:
            shutil.copy2(ROOT / "scripts" / name, self.root / "scripts" / name)
        self.binary = self.root / "fake-bin"
        self.binary.mkdir()
        implementation = self.binary / "fake-command"
        implementation.write_text(FAKE_COMMAND)
        implementation.chmod(0o755)
        for name in ["docker", "sg", "minikube", "kubectl", "helm", "curl"]:
            (self.binary / name).symlink_to(implementation)
        (self.root / "gradlew").symlink_to(implementation)
        runner = self.root / "runner/build/install/runner/bin/runner"
        runner.parent.mkdir(parents=True)
        runner.symlink_to(implementation)
        self.environment = dict(
            os.environ,
            PATH=str(self.binary) + os.pathsep + os.environ["PATH"],
            FAKE_PROJECT=str(self.root),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def invoke(self, *arguments, **flags):
        result = subprocess.run(
            ["bash", "scripts/run-local.sh", *arguments],
            cwd=self.root,
            env=dict(self.environment, **flags),
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result

    def commands(self):
        return [
            json.loads(line)
            for line in (self.root / "commands.jsonl").read_text().splitlines()
        ]

    def state(self):
        return json.loads((self.root / "state.json").read_text())

    def test_smoke_from_missing_infrastructure(self):
        result = self.invoke("smoke")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.state()["repetitions"], "1")
        actions = [
            entry["args"][0]
            for entry in self.commands()
            if entry["command"] == "runner"
        ]
        self.assertEqual(
            actions, ["local-settings", "plan", "provision", "image", "prepare", "run"]
        )
        self.assertTrue(list((self.root / "build/local-runs").glob("*/summary.csv")))
        self.assertFalse(any("delete" in entry["args"] for entry in self.commands()))

    def test_validation_overrides(self):
        result = self.invoke("full", "--skip-validation")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.state()["validationEnabled"], "false")
        result = self.invoke("full", "--config", "config/binpack.properties", "--validate")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.state()["validationEnabled"], "true")

    def test_full_budget_and_one_repetition(self):
        result = self.invoke("full", "--cluster", "full-test")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.state()["repetitions"], "1")
        start = next(
            entry["args"]
            for entry in self.commands()
            if entry["command"] == "minikube" and "start" in entry["args"]
        )
        self.assertIn("--cpus=12", start)
        self.assertIn("--memory=38912", start)
        self.assertIn("full-test", start)

    def test_plan_does_not_contact_infrastructure(self):
        result = self.invoke("smoke", "--plan-only", FAKE_DOCKER_DOWN="1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(
            any(
                entry["command"] in ["docker", "kubectl", "minikube"]
                for entry in self.commands()
            )
        )

    def test_docker_unavailable(self):
        result = self.invoke("smoke", FAKE_DOCKER_DOWN="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Docker is unavailable", result.stderr)

    def test_host_memory_too_small(self):
        result = self.invoke("full", FAKE_HOST_MIB="32768")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("host reserve", result.stdout)
        self.assertFalse(any("start" in entry["args"] for entry in self.commands()))

    def test_existing_cluster_too_small(self):
        result = self.invoke("full", FAKE_CLUSTER_MIB="16384")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Existing Docker cluster limits are too small", result.stdout)
        self.assertFalse(
            any(
                entry["command"] == "runner" and entry["args"][0] == "provision"
                for entry in self.commands()
            )
        )

    def test_insufficient_disk(self):
        result = self.invoke("full", FAKE_DISK_GIB="10")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("free in cluster storage", result.stdout)

    def test_active_writer_blocks_changes(self):
        result = self.invoke("smoke", FAKE_ACTIVE_WRITER="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("previous-writer", result.stdout)
        self.assertFalse(
            any(
                entry["command"] == "runner" and entry["args"][0] == "provision"
                for entry in self.commands()
            )
        )

    def test_input_conflict_and_fresh_input(self):
        result = self.invoke("smoke", FAKE_INPUT_CONFLICT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--fresh-input", result.stdout)
        result = self.invoke("smoke", "--fresh-input", FAKE_INPUT_CONFLICT="1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertRegex(
            self.state()["input"], r"^machine_events_smoke_uuid_string_[0-9a-f]{8}$"
        )

    def test_reuse_loads_images_without_rebuilding(self):
        result = self.invoke("smoke", "--reuse-images")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(
            any(
                entry["command"] == "runner" and entry["args"][0] == "image"
                for entry in self.commands()
            )
        )
        self.assertTrue(
            any(
                entry["command"] == "minikube" and "load" in entry["args"]
                for entry in self.commands()
            )
        )

    def test_missing_reused_image(self):
        result = self.invoke("smoke", "--reuse-images", FAKE_MISSING_IMAGE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Image missing", result.stdout)

    def test_custom_engine_builds_control_too(self):
        result = self.invoke(
            "smoke", "--engine", "config/engines/custom.example.properties"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        images = [
            entry["args"][-1]
            for entry in self.commands()
            if entry["command"] == "runner" and entry["args"][0] == "image"
        ]
        self.assertEqual(
            images,
            [
                "config/engines/oss.properties",
                "config/engines/custom.example.properties",
            ],
        )

    def test_gradle_failure_has_log(self):
        result = self.invoke("smoke", FAKE_BUILD_FAIL="1")
        self.assertEqual(result.returncode, 13)
        self.assertIn("Build Java applications", result.stdout)
        self.assertTrue(
            list((self.root / "build/local-runs").glob("*/failed-step.txt"))
        )

    def test_validation_failure_retains_infrastructure_and_diagnostics(self):
        result = self.invoke("smoke", FAKE_RUN_FAIL="1")
        self.assertEqual(result.returncode, 23)
        self.assertTrue(self.state()["infra"])
        self.assertTrue(
            list(
                (self.root / "build/local-runs").glob("*/diagnostics/pods-describe.txt")
            )
        )
        self.assertFalse(list((self.root / "build/local-runs").glob("*/summary.csv")))

    def test_project_lock_blocks_second_runner(self):
        lock = self.root / "build/locks/local-orchestrator.lock"
        lock.parent.mkdir(parents=True)
        with lock.open("w") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.invoke("smoke")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Another local orchestration", result.stderr)

    def test_helm_is_bootstrapped_without_system_install(self):
        result = self.invoke("smoke", FAKE_HELM_UNAVAILABLE="1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue((self.root / "build/tools/helm/v3.19.0/helm").is_file())

    def test_helm_checksum_failure_stops_before_cluster(self):
        result = self.invoke("smoke", FAKE_HELM_UNAVAILABLE="1", FAKE_BAD_CHECKSUM="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Helm checksum mismatch", result.stdout)
        self.assertFalse(any("start" in entry["args"] for entry in self.commands()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
