#!/usr/bin/env python3
"""Aggregate benchmark JSONL artifacts into CSV and timing statistics."""

import argparse
import csv
import json
from pathlib import Path
from statistics import median, stdev

p = argparse.ArgumentParser()
p.add_argument("directory", nargs="?", default="build/results")
p.add_argument("--csv", default="build/summary.csv")
a = p.parse_args()
rows = []
for path in sorted(Path(a.directory).rglob("results.jsonl")):
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        v = json.loads(line)
        verification_path = path.with_name("verification.json")
        verification = (
            json.loads(verification_path.read_text())
            if verification_path.exists()
            else None
        )
        if verification is not None:
            v["status"] = verification["status"]
            v["baselineRestored"] = verification["baselineRestored"]
        elif v.get("restorationDeferredForValidation"):
            v["status"] = "unverified"
        ids = set()
        podfile = path.with_name("pods.json")
        if podfile.exists():
            for pod in json.loads(podfile.read_text()).get("items", []):
                for container in pod.get("status", {}).get("containerStatuses", []):
                    if container.get("imageID"):
                        ids.add(container["imageID"])
        row = dict(
            artifact=str(path),
            engine=v.get("engine"),
            status=v.get("status"),
            strategy=v.get("strategy", "sort"),
            storageBackend=v.get("configuration", {}).get("storage.backend", "s3"),
            cacheMode=v.get("cacheMode", "none"),
            repetition=v.get("configuration", {}).get("benchmark.repetition"),
            sequence=v.get("configuration", {}).get("benchmark.sequence"),
            warmupSeconds=v.get("warmup", {}).get("seconds"),
            warmCacheResidentFraction=v.get("warmup", {}).get("residentFraction"),
            warmupInputBytes=v.get("warmup", {}).get("inputBytes"),
            outputCleanupSeconds=v.get("outputCleanupSeconds"),
            outputSnapshotsExpired=v.get("outputSnapshotsExpired"),
            validationEnabled=(verification.get("validationEnabled", True) if verification
                               else v.get("validationEnabled", True)),
            validationStatus=(verification.get("validationStatus",
                                  "passed" if verification.get("validatorsPassed") else "failed")
                              if verification else "unknown"),
            seconds=v.get("durationSeconds"),
            inputBytesPerSecond=v.get("inputBytesPerSecond"),
            baselineSnapshot=v.get("baselineSnapshot"),
            baselineRestored=v.get("baselineRestored"),
            image=v.get("image"),
            imageIDs=";".join(sorted(ids)),
        )
        row["validatorsPassed"] = (
            verification.get("validatorsPassed") if verification else None
        )
        row["trinoValidationEnabled"] = (
            verification.get("trinoValidationEnabled") if verification else None
        )
        for phase in ["before", "after"]:
            for key in ["files", "bytes", "records", "minFileBytes", "maxFileBytes"]:
                row[phase + "_" + key] = v.get(phase, {}).get(key)
        row.update(v.get("taskMetrics", {}))
        rows.append(row)
if not rows:
    raise SystemExit("No benchmark results found")
out = Path(a.csv)
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", newline="") as f:
    writer = csv.DictWriter(
        f, fieldnames=list(dict.fromkeys(k for row in rows for k in row))
    )
    writer.writeheader()
    writer.writerows(rows)
groups = sorted({(row["engine"], row["strategy"], row["storageBackend"], row["cacheMode"], row["validationStatus"]) for row in rows})
statistics_rows = []
for engine, strategy, storage, cache, validation in groups:
    samples = [row for row in rows if
               (row["engine"], row["strategy"], row["storageBackend"], row["cacheMode"], row["validationStatus"]) == (engine, strategy, storage, cache, validation)]
    values = [row["seconds"] for row in samples if row["status"] == "success"]
    failures = sum(row["status"] != "success" for row in samples)
    label = f"{engine}/{strategy}/{storage}/{cache} [validation={validation}]"
    if values:
        statistics_rows.append(dict(engine=engine, strategy=strategy, storageBackend=storage,
                                    cacheMode=cache, validationStatus=validation, n=len(values),
                                    medianSeconds=median(values), minSeconds=min(values),
                                    maxSeconds=max(values), stdevSeconds=stdev(values) if len(values)>1 else 0,
                                    failures=failures))
        print(
            f"{label}: n={len(values)} median={median(values):.3f}s min={min(values):.3f}s max={max(values):.3f}s failures={failures} validationSkipped={len(samples) if validation == 'skipped' else 0}"
        )
    else:
        print(f"{label}: no successful samples; failures={failures}")
print(out)
if statistics_rows:
    statistics_path = out.with_name(out.stem + "-statistics.csv")
    with statistics_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(statistics_rows[0]))
        writer.writeheader()
        writer.writerows(statistics_rows)
    print(statistics_path)
