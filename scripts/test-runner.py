#!/usr/bin/env python3
"""Exercise orchestration with fake Kubernetes commands; no cluster is contacted."""

import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
os.chdir(root)
libraries = sorted((root / "runner/build/install/runner/lib").glob("*.jar"))
if not libraries:
    raise SystemExit("Run ./gradlew :runner:installDist first")
classes = os.pathsep.join(map(str, libraries))
with tempfile.TemporaryDirectory(prefix="sparkbench-runner-") as tmp:
    tmp = Path(tmp)
    fake = tmp / "kubectl"
    fake.write_text("""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path

args = sys.argv[1:]
p = Path(os.environ["FAKE_STATE"])
s = (
    json.loads(p.read_text())
    if p.exists()
    else {"apps": {}, "actions": [], "deleted": []}
)
out = ""
if "apply" in args:
    v = json.loads(Path(args[args.index("-f") + 1]).read_text())
    if v["kind"] == "SparkApplication":
        name = v["metadata"]["name"]
        action = v["spec"]["arguments"][0]
        s["apps"][name] = action
        s["actions"].append(action)
elif "exec" in args:
    print("WARNING: fake CLI diagnostic", file=sys.stderr)
    sql = args[args.index("--execute") + 1]
    if "information_schema.columns" in sql:
        out = "eventid\\nattributes"
    elif sql.startswith("SHOW STATS"):
        out = "\\t\\t\\t\\t1470.0\\t\\t"
    elif os.environ.get("FAKE_TRINO_FAIL"):
        out = "false\\t1470\\t1469\\tAA\\tBB"
    else:
        out = "true\\t1470\\t1470\\tAA\\tAA"
elif "get" in args and "sparkapplication" in args:
    name = args[args.index("sparkapplication") + 1]
    if any("jsonpath" in a for a in args):
        out = (
            "FAILED"
            if os.environ.get("FAKE_FAIL") and s["apps"][name] == "compact"
            else "COMPLETED"
        )
    else:
        out = "{}"
elif "get" in args and "pods" in args:
    out = '{"items":[]}'
elif "logs" in args:
    action = s["apps"].get(args[args.index("logs") + 1].removesuffix("-driver"))
    out = (
        'BENCH_VALIDATION {"status":"success"}\\n'
        if action == "validate"
        else 'BENCH_RESULT {"engine":"fake","status":"success","durationSeconds":2.0}\\n'
    )
elif "delete" in args and "sparkapplication" in args:
    if os.environ.get("FAKE_STOP_FAIL"):
        sys.exit(1)
    s["deleted"].append(args[args.index("sparkapplication") + 1])
p.write_text(json.dumps(s))
print(out)
""")
    fake.chmod(0o755)
    config = tmp / "config.properties"
    config.write_text(
        f'config.parent={root / "config/smoke.properties"}\nresults.directory={tmp / "results"}\n'
    )
    env = dict(
        os.environ,
        PATH=str(tmp) + os.pathsep + os.environ["PATH"],
        FAKE_STATE=str(tmp / "state.json"),
    )
    cmd = [
        "java",
        "-cp",
        str(classes),
        "dev.sparkbench.runner.Main",
        "run",
        str(config),
        "config/engines/oss.properties",
        "config/engines/custom.example.properties",
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads((tmp / "state.json").read_text())
    assert state["actions"] == ["reset", "compact", "validate", "reset"] * 2, state
    (tmp / "state.json").unlink()
    env["FAKE_FAIL"] = "1"
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode != 0, "failed engine was accepted"
    state = json.loads((tmp / "state.json").read_text())
    assert state["actions"] == ["reset", "compact", "reset"], state
    assert len(state["deleted"]) == 1, "failed writer was not stopped before restoring"
    (tmp / "state.json").unlink()
    env["FAKE_STOP_FAIL"] = "1"
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode != 0
    state = json.loads((tmp / "state.json").read_text())
    assert state["actions"] == [
        "reset",
        "compact",
    ], "restored while writer stop was unverified"
    assert "refusing concurrent snapshot restoration" in result.stderr
    (tmp / "state.json").unlink()
    env.pop("FAKE_FAIL")
    env.pop("FAKE_STOP_FAIL")
    env["FAKE_TRINO_FAIL"] = "1"
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode != 0, "Trino mismatch was accepted"
    state = json.loads((tmp / "state.json").read_text())
    assert state["actions"] == ["reset", "compact", "validate", "reset"]
    assert "Trino data comparison failed" in result.stderr
    env.pop("FAKE_TRINO_FAIL")
    (tmp / "state.json").unlink()
    with config.open("a") as f:
        f.write("validation.enabled=false\nbenchmark.compactionStrategy=binpack\n")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads((tmp / "state.json").read_text())
    assert state["actions"] == ["reset", "compact", "reset"] * 2, state
    verdicts = [json.loads(p.read_text()) for p in (tmp / "results").rglob("verification.json")]
    skipped = [v for v in verdicts if v.get("validationStatus") == "skipped"]
    assert len(skipped) == 2
    assert all(v["validatorsPassed"] is None and v["baselineRestored"] for v in skipped)
    fast_report = subprocess.run(
        ["python3", "scripts/summarize.py", str(tmp / "results"), "--csv", str(tmp / "fast.csv")],
        capture_output=True, text=True)
    assert fast_report.returncode == 0, fast_report.stderr
    assert "validationSkipped=2" in fast_report.stdout
    fixture = tmp / "summary-fixture"
    fixture.mkdir()
    (fixture / "results.jsonl").write_text(
        json.dumps(dict(engine="oss", status="success", durationSeconds=2.0))
        + "\n"
        + json.dumps(dict(engine="oss", status="failed"))
        + "\n"
    )
    (fixture / "pods.json").write_text(
        json.dumps(
            {"items": [{"status": {"containerStatuses": [{"imageID": "sha256:test"}]}}]}
        )
    )
    report = subprocess.run(
        [
            "python3",
            "scripts/summarize.py",
            str(fixture),
            "--csv",
            str(tmp / "summary.csv"),
        ],
        capture_output=True,
        text=True,
    )
    assert report.returncode == 0, report.stderr
    assert "failures=1" in report.stdout
    assert "sha256:test" in (tmp / "summary.csv").read_text()
print(
    "Runner integration test passed: two engines share baseline; failed engine stops and restores before aborting"
)
