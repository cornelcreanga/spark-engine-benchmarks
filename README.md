# Spark engine benchmarks

Java/Gradle framework for benchmarking Spark Docker images, supporting sorted and bin-pack Iceberg compaction. The baseline uses Spark 4.0.2, Scala 2.13, Java 17 bytecode, and Iceberg 1.12.0. Custom engines add dependencies, native libraries, Spark configuration, and optional Kubernetes packages.

## Components

| Component | Responsibility |
| --- | --- |
| `core` | Configuration inheritance, validation, SQL quoting, JSON |
| `dataset-generator` | Deterministic executor-side event generation and physical-size calibration |
| `benchmarks` | Compaction, snapshot restoration, content checks, task counters |
| `spark-images` | Gradle entry point for Spark image building |
| `infrastructure` | Gradle entry point for shared filesystem storage, RBAC, Spark Operator |
| `runner` | Image builds, manifests, packages, engine execution, failure recovery |
| `results` | Structured result emission; aggregation in `scripts/summarize.py` |
| `jobs` | Application JAR; Spark and Iceberg libraries come from the image |

## One-command local orchestration

```bash
scripts/run-local.sh smoke
scripts/run-local.sh full
```

The Bash orchestrator builds the Java applications, validates configuration, checks Docker and host resources, ensures Helm 3 is available (bootstrapping a checksum-verified copy under `build/tools` when needed), starts/resumes Minikube, checks actual node/container limits and free storage, refuses to proceed alongside active Spark applications, provisions the shared warehouse PVC, RBAC, and the Spark Operator Helm chart, builds and loads the OSS/control plus requested engine images, prepares/reuses input, runs the benchmark and configured validators, and writes a CSV summary for this invocation only.

It uses the resource values in the selected properties file: smoke requests 6 CPUs/16 GiB and runs once; full requests 12 CPUs/38 GiB and runs once. Java, Python 3, Docker, Minikube, and standard Linux tools (`flock`, `timeout`) must be installed. Helm bootstrap also needs curl, tar, and sha256sum. Existing Docker-group membership is activated with `sg` when possible; a stopped Docker daemon or missing OS permissions fails with guidance. Minikube supplies a matching kubectl through an invocation-local wrapper, avoiding client/server version skew. No sudo/system installation is performed.

Examples:

```bash
# Inspect generated manifests without contacting Docker or Kubernetes.
scripts/run-local.sh smoke --plan-only

# Run in a named Minikube profile/context.
scripts/run-local.sh smoke --cluster sparkbench-smoke
scripts/run-local.sh full --cluster sparkbench-full

# Start a fresh table while retaining the existing input and results.
scripts/run-local.sh smoke --fresh-input

# Reuse existing local images, always loading them into the selected cluster.
scripts/run-local.sh smoke --reuse-images

# Include the Java integration/self-tests before starting infrastructure.
scripts/run-local.sh smoke --verify-code

# Supply a custom workload profile and one or more engine definitions.
scripts/run-local.sh full --config config/my-local.properties \
  --engine config/engines/oss.properties \
  --engine config/engines/my-engine.properties
```

The default Minikube profile/context comes from the configuration (`minikube`). A named cluster overrides both without editing your base files. An existing cluster whose Docker resource limits are too small fails with guidance instead of silently running an undersized benchmark. Use a correctly sized/new `--cluster` or resize the existing stopped profile yourself. Stop an idle old cluster with `minikube stop -p NAME` before starting another large one; the script does not stop/delete unrelated clusters or their data.

`prepare` generates input only if the table is absent. Existing input must have the current schema, a completed `benchmark_input` tag, matching dataset settings/generator fingerprint, and baseline bytes within tolerance. Changes to executor resources/repetitions do not invalidate input; changes to dataset settings do. Old/incomplete/incompatible tables fail with a `--fresh-input` recovery suggestion. A fresh table receives a unique suffix and never replaces the old table. Generation itself is not resumed mid-table.

Each invocation writes an effective configuration, settings, console log, job results, and `summary.csv` under `build/local-runs/<profile>-<timestamp>-<id>/`. On failure or interruption it exits nonzero, records the failed step, and attempts bounded cluster diagnostics (pods, events, PVCs, deployments, SparkApplications, and pod descriptions). It retains infrastructure and data for inspection. The Java runner attempts restoration around benchmark failures; the Bash wrapper never resets a snapshot blindly while another writer might be active. Fix the reported cause and rerun, or inspect/stop any active application before the printed manual reset command. Failed or partial invocations are never summarized as successful.

A local orchestration lock covers the whole flow. Initial missing-tool/Docker failures happen before infrastructure creation. This orchestrator is for local Docker-backed Minikube; use `scripts/bench` directly for an externally managed cluster.

The orchestration error-path suite contacts no real cluster:

```bash
python3 scripts/test-local-orchestrator.py
```

## Build and local smoke run

Prerequisites: Java 17 or 21, Docker access, Minikube, kubectl, Helm 3, and network access for Gradle, Maven, Helm, and container downloads. Install Helm separately if absent. Run commands from this directory. The wrapper uses Gradle 8.14.6 with a distribution checksum.

```bash
./gradlew build verify
minikube start --cpus=6 --memory=16384 --disk-size=100g
scripts/bench plan config/smoke.properties
scripts/bench provision config/smoke.properties
scripts/bench image config/smoke.properties config/engines/oss.properties
scripts/bench generate config/smoke.properties
scripts/bench run config/smoke.properties config/engines/oss.properties
python3 scripts/summarize.py
```

`plan` validates configuration and renders Kubernetes JSON under `build/plan` and `build/infrastructure`. Kubernetes accepts JSON manifests directly. `provision` installs the pinned Kubeflow Spark Operator chart, namespace/RBAC, and a shared warehouse PVC. Local profiles use an Iceberg Hadoop catalog at `file:///warehouse/iceberg`; no object-store emulator or REST catalog service is needed. The explicit Kubernetes context defaults to `minikube`; for another profile, change both `k8s.context` and `k8s.minikubeProfile`.

`config/local.properties` provides the full profile: 20 GiB of input, approximately 10 MiB files, one UTC hour, 16 buckets, driver 2 cores/4 GiB plus 1 GiB overhead, and two executors of 6 cores/12 GiB (2 GiB heap per core) plus 2 GiB overhead each. Both profiles run one repetition per engine. For a 48 GB host (approximately 45 GiB reported by Linux), allocate 12 CPUs and 38 GiB to Minikube. Spark requests 33 GiB in total, leaving approximately 5 GiB inside the cluster for Kubernetes and other cluster services, and approximately 7 GiB outside it for the host OS and other processes. These are starting budgets; monitor actual service usage during a full run. The smoke profile has 64 MiB of input, approximately 1 MiB files, 4 buckets, and smaller pods. Retained compaction output accumulates across runs; allow ample disk space.

For the full profile on a 48 GB host:

```bash
minikube start --driver=docker --cpus=12 --memory=38912
```

Allow at least 150 GiB of free space in Docker's storage for images, the 20 GiB baseline, retained outputs, and temporary data. Additional engines/repetitions retain additional outputs. The Docker driver uses host Docker storage; `--disk-size` does not reserve a separate VM disk.

Equivalent component tasks:

```bash
./gradlew :spark-images:buildImage -Pconfig=config/smoke.properties -Pengine=config/engines/oss.properties
./gradlew :infrastructure:provision -Pconfig=config/smoke.properties
```

## Dataset contract

Seven types share one table: `dnsrequest`, `prstart`, `prend`, `filecreated`, `filemodify`, `filerename`, `filedelete`. Fields include UUID event ID, timestamp, customer/system UUIDs, host/user, process fields, file paths, DNS fields, message, and string-map attributes. Customer and system IDs are canonical 36-character UUID strings in Java and `STRING` columns in Spark/Iceberg (`VARCHAR` in Trino). System UUIDs are derived deterministically from the customer UUID and system ordinal, so repeated events from the same machine retain the same ID. Events are synthetic; process start/end and file lifecycles are not correlated.

The individual immutable Java event classes live in `core/src/main/java/dev/sparkbench/events`. All implement `MachineEvent` and share `EventMetadata` (UUID, time, customer, system, host, user) and `ProcessContext` (PID, process name, executable). Their typed properties map to a union of nullable top-level columns and meaningful map attributes:

| Class | Top-level event properties | Map attributes |
| --- | --- | --- |
| `DnsRequestEvent` | Query name/type, response code, resolved address, server address, latency | Transport, cache hit |
| `ProcessStartEvent` | Parent PID/name, working directory | Command line, integrity level |
| `ProcessEndEvent` | Exit code, elapsed lifetime | Termination reason |
| `FileCreatedEvent` | Path, extension, initial size, synthetic SHA-256 | Owner, permissions |
| `FileModifiedEvent` | Path, extension, old/new size, bytes written, synthetic SHA-256 | Write operation |
| `FileRenamedEvent` | Original/destination path, extension, size | Overwrite flag |
| `FileDeletedEvent` | Path, extension, last known size/hash | Deletion method |

`EventTable.SCHEMA` defines both the Spark row layout and generated Iceberg DDL, so they cannot drift independently. Event-inapplicable columns are null. Unanswered DNS lookups have a null resolved address. Append events maintain `new size = old size + bytes written`. `message` and the map's `context` pad the configured byte budgets after meaningful properties have been counted, including non-ASCII UTF-8 values. A budget too small for those properties fails explicitly.

This replaces the initial generic schema (`pid`, `domain`, `path`, etc.) with descriptive columns such as `process_id`, `dns_query_name`, and `file_path`. Existing input must be regenerated into a new table or explicitly removed before generation; it is not silently migrated. The UUID schema uses separate default tables, `machine_events_smoke_uuid_string` and `machine_events_uuid_string`, preserving earlier numeric-ID and binary-UUID tables. Existing tables are not rewritten in place.

Partitioning is `hours(eventtime), bucket(N,cid), eventtype`. Iceberg's hidden hour transform represents UTC date/hour without redundant year/month/day columns. The entire dataset occupies one configurable hour.

Payload means UTF-8 **value** bytes before storage encoding. Customer and system UUIDs each count as 36 UTF-8 payload bytes. Other non-null top-level values use ISO_INSTANT timestamp text and decimal numeric text; map values are counted separately. Field names, map keys, null markers, and Parquet encoding are excluded. The default 1,024-byte payload contains exactly 768 top-level and 256 map-value bytes. Message/context fields supply deterministic random ASCII content. This is not a claim about compressed column sizes.

Customers are selected using Iceberg's actual string bucket hash over the stored UUID text. Default bucket weights rise linearly from 1 to `dataset.bucketSkew` (3). Set an explicit positive `dataset.bucketWeights` list to override them. Configure customer counts, systems per customer, event frequencies, payload size/fraction, seed, and parallelism in the profile. The driver holds only customer lookup metadata; event rows are generated on executors. Generator v4 retains the hashed row seeding introduced in v2 and uses string customer/system UUIDs. This avoids the overlapping padding streams in v1. Rebuild engine images and regenerate input when changing generator versions; the schema and input fingerprint prevent silent reuse of incompatible data.

Input size means committed Parquet bytes, excluding metadata. A calibration batch measures physical bytes using Iceberg's files metadata table; later batches adjust row counts until within `dataset.sizeTolerance`. Iceberg rolls files at the configured target. File sizes remain approximate, especially at partition/batch tails. Generation logs actual partition sizes. More buckets produce smaller partitions, and compaction cannot merge across partitions. Per-file overhead may make tiny datasets with many partitions impossible within tolerance; generation fails instead of marking them successful.

The baseline tag is created only after successful size validation. Existing tables are never overwritten automatically. After failed generation, use another table name or explicitly remove the old table before retrying.

## Compaction and repeatability

Defaults preserve the requested 512 MiB target, 3 minimum input files, 10 GiB file-group limit, 100 concurrent rewrites, partial progress enabled, and at most 10 partial commits. The rewrite strategy is explicitly `sort` with `cid ASC, sid ASC`.

Generation creates the `benchmark_input` tag. The OSS control image runs a separate restoration job before and after every engine repetition. The compaction job restores on entry. For runner-managed successful runs it retains the compacted snapshot until both independent validators finish; the runner restores in `finally`. Failed jobs and standalone compaction jobs attempt their own restoration. Restoration uses `set_current_snapshot`. The baseline tag, `gc.enabled=false`, and absence of snapshot expiration/orphan cleanup retain the original input files.

The measured interval covers the rewrite procedure and commits. It excludes scheduling, Spark startup, snapshot restoration, checksum queries, and metadata queries. Content checks compare row count and two order-independent hash sums covering every column, with canonical map ordering. These are probabilistic fingerprints, not exhaustive row comparisons. Failed groups reported by partial progress fail the run.

Every successful compaction is checked before restoration, outside the timed interval:

1. **OSS Spark** reads every column from the compacted table and the retained baseline snapshot. Row count and two order-independent hash sums must match, with canonical map ordering.
2. **Iceberg statistics** are recomputed from readable files: each file's record count and every top-level primitive column's value count, null count, lower bound, and upper bound must agree with stored Iceberg metrics. Input tables request full metrics (`write.metadata.metrics.default=full`), so truncated or missing bounds fail validation. File sizes must be positive. Map contents participate in the data fingerprint; map leaf bounds are not compared.
3. **Trino** independently scans every column of both snapshots and compares its row count and aggregate checksum. Its `SHOW STATS` table row count must also agree with its scan. Trino checks its own baseline-versus-output checksum, rather than assuming Spark and Trino hash functions share a binary representation.

When validation is enabled, both configured validators must finish successfully before a repetition is reported as verified. A validation or restoration failure fails the repetition and aborts the suite. `verification.json` records that final verdict; the CSV summarizer excludes deferred output without a verdict from successful timings. This is relaxed hash-based equality, not an exhaustive multiset comparison. Validation scans can be expensive and deliberately do not count toward compaction throughput.

Local profiles use the OSS Spark validator when validation is enabled; Trino is disabled. For an externally managed REST/S3 deployment, set `validation.trino.enabled=true`, `validation.trino.pod`, `validation.trino.server`, and `validation.trino.catalog` to an existing pod containing the CLI and a configured Iceberg connector. Its CLI uses `validation.trino.cliHeap` and `validation.trino.cliJar`. Local provisioning does not create external catalog or validator services.

Manual validation of the currently selected snapshot, without restoring it:

```bash
scripts/bench validate config/smoke.properties
scripts/bench validate-trino config/external.properties
```

Failed/timed-out applications are stopped before external restoration. If the runner cannot verify that writers stopped, it refuses to restore concurrently. Stop that application and recover with:

```bash
scripts/bench reset config/smoke.properties
```

The suite aborts after a failed run and attempted restoration. A local file lock prevents concurrent runners targeting the same context/namespace/table. Use one runner with no other writers or maintenance jobs; this is not a distributed lock. SparkApplications, ConfigMaps, snapshots, and output files remain for inspection. Cleanup is manual after preserving results.

Input files and snapshots are identical across repetitions. Cache state is uncontrolled: the framework does not clear JVM, OS, filesystem, or object-store caches. Engine order follows the command line. Control caches externally for cold-cache experiments.

## Shared local-filesystem benchmark

`config/filesystem.properties` keeps the full 20 GiB workload, two six-core
executors, existing engine memory budgets, and the same Minikube allocation.
Driver and executors mount one single-node PVC at `/warehouse`. The Iceberg
catalog is a Hadoop catalog at `file:///warehouse/iceberg`; benchmark metadata,
reads, and writes use local files. This is also the default for `config/local.properties`
and `config/smoke.properties`.

On first preparation, the OSS control job generates the input directly on the
shared filesystem and creates the `benchmark_input` tag. Later runs reuse this
local baseline. Existing filesystem tables require no migration.

For an externally managed S3 source only, the optional `filesystem.importFromS3=true`
path can copy its tagged Parquet files without re-encoding. Configure
`catalog.uri`, `filesystem.source.table`, `filesystem.source.warehouse`,
`filesystem.copyThreads`, and the external `s3.*` connection properties explicitly;
local profiles do not configure or contact an S3 source.

```bash
scripts/run-local.sh full --config config/filesystem.properties \
  --engine config/engines/oss.properties \
  --engine config/engines/comet.properties
```

For bin-pack, use `--config config/filesystem-binpack.properties` with the same
engine arguments. Both profiles reuse the same local baseline.

The profile disables verification by default. `--validate` enables the OSS
validator; Trino is disabled because its current deployment does not mount the
filesystem warehouse. This PVC layout is for the existing single-node Minikube
cluster; it is not a shared-filesystem solution for a multi-node cluster.

Keep enough free physical disk for the copied baseline, retained outputs, and
shuffle files. Old S3 outputs are not automatically removed. An explicit
`cleanup-outputs` runner action accepts an approved `cleanup.snapshotIds` list,
requires the tagged baseline to be current, protects all referenced snapshots,
and temporarily enables Iceberg garbage collection only for that expiration.

## Warm-cache measurements

`config/warm-filesystem.properties` repeats local-filesystem samples three times,
reverses engine order on every other round, and disables correctness verification.
Set `benchmark.cacheMode=warm` to request a node-level raw read of every baseline
Parquet byte immediately before each compaction. The Spark driver waits at a
barrier while `scripts/warm-cache.py` reads the shared PVC on the Minikube node,
then measures page residency with Linux `mincore`. At least 99% of input pages must
be resident before timing can start. Running this outside the Spark pod avoids
limiting the warmed page cache to the driver's memory limit.

Warm-up time and initial residency are recorded separately; warming is excluded
from compaction time and task counters. This warms the OS filesystem page cache,
not Spark's decoded DataFrame cache. Pages are not pinned and can be evicted as
the measured workload consumes memory. The default `cacheMode=none` means no
explicit cache conditioning, not a guaranteed cold cache.

The warm profile expires only snapshots created by the current sample after
restoring its baseline, outside timing. Pre-existing snapshots, input files, and
saved reports remain untouched. This keeps repeated output within the existing
disk budget. `benchmark.discardOutputs` requires disabled data verification.

Run both strategies for both engines in one alternating, three-round invocation:

```bash
scripts/run-local.sh full --config config/warm-filesystem.properties \
  --engine config/engines/oss.properties \
  --engine config/engines/comet.properties \
  --engine config/engines/oss-binpack.properties \
  --engine config/engines/comet-binpack.properties
```

The sample CSV includes warm-up duration, initial residency, cache mode, sequence,
and repetition. The separate `summary-statistics.csv` records sample count,
median, minimum, maximum, and standard deviation, keeping storage and cache modes
in separate groups.

## Bin-pack compaction and optional validation

```bash
scripts/run-local.sh full --config config/binpack.properties \
  --engine config/engines/oss.properties \
  --engine config/engines/comet.properties
```

This runs one iteration per engine against the existing full string-ID baseline,
using Iceberg's `binpack` strategy without a sort order. The profile sets
`validation.enabled=false`: it skips before/after fingerprint scans, independent
OSS validation, and Trino validation. It retains lightweight file metadata,
benchmark/job failure checks, and baseline restoration before and after each run.
Results distinguish execution success from validation: `validationStatus=skipped`,
`validatorsPassed=null`. The CSV records the strategy and validation status.

Add `--validate` to enable fingerprint scans and the configured validators, or
use `--skip-validation` with any local profile to disable them. The standard
sorted profiles keep validation enabled by default. In a configuration file,
`benchmark.compactionStrategy` accepts `sort` (default) or `binpack`, and
`validation.enabled` accepts `true` (default) or `false`.

## DataFusion Comet

`config/engines/comet.properties` pins Comet 1.1.0 for Spark 4.0 / Scala 2.13,
with Iceberg runtime and AWS bundle 1.11.0. It enables the Comet plugin, native
Iceberg scans, shuffle, and experimental Iceberg native Parquet writes. Both
`spark.comet.write.iceberg.splitOperator.enabled` and
`spark.comet.iceberg.write.enabled` are enabled. Iceberg extensions stay enabled.
The full profile keeps the same 14 GiB executor budget with 4 GiB heap, 8 GiB
off-heap, and 2 GiB overhead (two executors, six cores each).

```bash
scripts/run-local.sh full \
  --engine config/engines/oss.properties \
  --engine config/engines/comet.properties
```

Both engines run once against the same `events.machine_events_uuid_string` table
and `benchmark_input` tag. The v4 generator creates the string-ID baseline on the
first invocation and reuses it when schema and generator settings match. Earlier
binary-ID tables and results remain available. The OSS 1.12.0 control image
prepares, validates, and restores the baseline; the Comet benchmark uses Iceberg
1.11.0. Both OSS Spark and Trino independently validate output before restoration.

The old binary `cid` partition source caused Comet 1.1.0 to fall back to JVM scans;
string IDs remove that type restriction. Native writes still depend on Comet's
eligibility checks: inspect plans for `CometIcebergNativeScan`, `CometIcebergWrite`,
and `IcebergCommit`, and output Parquet footers for the `(Comet)` writer marker.
Metadata-table queries may fall back. Comparing OSS 1.12.0 with Comet/Iceberg 1.11.0
also includes an Iceberg-version difference and does not isolate Comet alone.

References: [Comet 1.1 installation](https://datafusion.apache.org/comet/user-guide/1.1/installation.html),
[Iceberg integration](https://datafusion.apache.org/comet/user-guide/1.1/iceberg.html),
[native writes](https://datafusion.apache.org/comet/user-guide/1.1/iceberg-writes.html),
[memory tuning](https://datafusion.apache.org/comet/user-guide/1.1/tuning/memory.html).

## Custom engines

Copy `config/engines/custom.example.properties`. It documents extra JAR directories, explicit Maven coordinates, native Dockerfile steps, `spark.conf.*`, image loading/pushing, and optional package installation. Maven coordinates download only the specified JARs; provide their transitive dependencies too. Dockerfile additions run as root before the final `USER 185`. The build context contains `extras/`, Spark defaults, and the job JAR. SQL extension overrides must retain IcebergSparkSessionExtensions.

```bash
scripts/bench image config/local.properties config/engines/my-engine.properties
scripts/bench run config/local.properties config/engines/oss.properties config/engines/my-engine.properties
```

Build the OSS control image even when benchmarking only custom engines; generation/restoration use it. Builds preserve Dockerfiles, contexts, and Docker inspection metadata under `build/images/<engine>`. Use immutable tags/digests and preserve this metadata with results. For externally built images, skip the image command; the image must include `/opt/spark/bench/spark-bench-jobs.jar` plus compatible Spark/Iceberg/AWS runtime libraries.

`benchmark.helm.*` / `benchmark.manifest` install benchmark-specific packages; `engine.helm.*` installs a pinned chart before an engine runs; add its repository alias first if needed. `engine.manifest` applies additional Kubernetes packages. A custom operator must support the emitted SparkApplication API. Configure operators to avoid reconciling the same applications. Operators using another API require an additional submission adapter. Engine dependencies and settings belong in their engine definitions; Comet has a pinned definition above.

When changing Spark versions, align `-PsparkVersion`, `-PscalaBinary`, `spark.version`, `image.base`, `image.sparkLine`, and `image.scalaBinary`. The OSS engine uses Iceberg 1.12.0; Comet uses 1.11.0. `-PcompileSparkVersion` permits an API compatibility check against a locally cached Spark version; the normal build uses the pinned version.

## Existing infrastructure

For an externally managed REST/S3 setup, set `storage.backend=s3` and configure context, namespace, catalog URI/warehouse, and S3 endpoint/region. Provision its catalog and storage separately. Provision compatible operator/RBAC separately. `k8s.credentialsSecret` injects AWS SDK environment credentials into driver/executors. To use SDK credentials, use a standalone config without local `s3.accessKey` and `s3.secretKey` properties. Additional Spark/catalog/auth settings go in `spark.conf.*`.

Local provisioning assumes a default storage class with ReadWriteOnce volumes suitable for single-node Minikube. Adapt storage for another cluster. Saved effective configs/manifests contain their configured credentials; use Secrets for production credentials.

## Results and validation

Each job saves its effective config, SparkApplication manifest/status, pod metadata with available image IDs, driver logs, and `results.jsonl` under timestamped `build/results` directories. Results include duration, input-byte throughput, file counts/sizes/percentiles, row counts, content fingerprints, restoration status, and Spark task counters: CPU/run/GC time, shuffle, spill, input/output bytes, and maximum per-task peak execution memory. Counters include task attempts; peak execution memory is not process RSS, and native engines may not expose all work through Spark task metrics.

`python3 scripts/summarize.py [RESULTS_DIRECTORY]` writes `build/summary.csv` with pod image IDs and prints sample counts, median/min/max timing, and failures.

`./gradlew build verify` compiles pinned dependencies and runs configuration/manifest and dataset checks. Dataset checks cover all seven concrete event classes, Spark encoding, nullability, 4/16/256 actual Iceberg buckets, skew, UUIDs, one-hour timestamps, determinism, and payload ratios. The real local Spark/Iceberg integration compacts 21 files into 7, validates all rows and statistics, detects a modified DNS field and missing metrics, and restores the baseline.

The Spark/Iceberg integration tests use temporary filesystem warehouses and require
neither Docker services nor an object-store emulator. Kubernetes/operator scheduling
remains a separate smoke run.

Restricted offline fallback:

```bash
python3 scripts/check-offline.py
python3 scripts/test-runner.py
```

The fallback compiles against cached APIs and runs self-tests. It does not replace the pinned Gradle build or live cluster validation. The simulated runner test checks engine sequencing, validation-before-restoration, Trino mismatch failures, and cleanup/restoration on failure; it does not exercise real Kubernetes, S3, or Iceberg SQL.

References: [Iceberg procedures](https://iceberg.apache.org/docs/latest/spark-procedures/), [Spark Operator chart](https://github.com/kubeflow/spark-operator/tree/v2.4.0/charts/spark-operator-chart), [Trino 476 Iceberg](https://github.com/trinodb/trino/blob/476/docs/src/main/sphinx/connector/iceberg.md).

### Scalar-only event comparison

`config/simple-event-name.properties` creates `bench.events.SimpleEventName` with
all 29 scalar event columns and no `attributes` map. `dataset.includeAttributes`
defaults to true; setting it to false selects this schema. Optional `dataset.rows`
generates an exact number of deterministic event indices instead of targeting
Parquet bytes. Reusing such input checks the row count in Iceberg file metadata.

The simple profile uses the same 33,097,164 event indices, seed, scalar values,
partitioning, compression, and small-file target as the map-bearing baseline.
Removing the map reduces physical bytes and can change input file counts; this
measures the overall effect of omitting the map, not just CPU cost at fixed bytes.
The warm-cache settings, three repetitions, executor sizes, and disabled
verification match the map-bearing measurements. Run all four cases with:

```bash
scripts/run-local.sh full --config config/simple-event-name.properties --skip-validation \
  --engine config/engines/oss.properties --engine config/engines/comet.properties \
  --engine config/engines/oss-binpack.properties --engine config/engines/comet-binpack.properties
```

### Onehouse Quanton distribution

The `quanton` and `quanton-binpack` engine definitions submit a
`QuantonSparkApplication` to the vendor operator. They follow the
[Quanton local quickstart](https://quanton.dev/docs/quickstart/local/) and use the
vendor Spark 4 image with its bundled Iceberg jars and native clustering option.
Quanton's current Spark 4 runtime is on the 4.1 line; OSS Spark and Comet here use
4.0.2. Treat this as a distribution comparison, including runtime differences.

Keep the downloaded `onehouse-values.yaml` in your home directory. The runner
passes that external path directly to Helm, never copies or templates its
contents into the repository, and rejects values files inside the project.
`quanton.valuesFile=/absolute/external/path.yaml` overrides the default location.
Do not put credentials in engine properties. The operator manages registry and
runtime authentication through Kubernetes secrets.

The runner installs chart 2.0.7 into `quanton-operator`, watches only `spark-bench`,
and stages the benchmark JAR on the shared filesystem PVC. It retains the vendor
image instead of building an image with extra Maven Iceberg jars, which could
conflict with the bundled runtime. The optional AI agent is disabled. Quanton
splits the 12 GiB executor memory budget into approximately 4 GiB JVM heap and
8 GiB native memory, with 2 GiB additional overhead; executor count/core settings
come from the benchmark profile. Operator and telemetry components also use the
same Minikube node. The vendor operator communicates with Onehouse services as
part of its normal authentication and telemetry operation.

Run three warm-cache sort and bin-pack repetitions against the scalar-only table:

```bash
scripts/run-local.sh full --config config/simple-event-name.properties --skip-validation \
  --engine config/engines/quanton.properties --engine config/engines/quanton-binpack.properties
```

Use `config/warm-filesystem.properties` for the original map-bearing table.
The operator-managed image is pulled through Kubernetes; `--reuse-images` applies
to the locally built OSS/control image only. Both profiles retain their existing
input baselines and remove only newly created compaction output snapshots.

In the tested `v0.43.0-spark4-al2023` runtime, bin-pack logs explicitly report
`Using the columnar rewriter for accelerated bin-pack compaction`. The bin-pack
profile sets `compaction.force-parquet-rewriter=true`, supported by the bundled
Iceberg action, so selecting this path does not depend on vendor runtime feature
defaults. Confirm `PARQUET-REWRITER` in the saved driver log for each measurement.
The captured
sort plans use Spark `Sort`/`AppendData` operators. Enabling the vendor clustering
flag does not by itself prove native sort execution. Keep the execution evidence
with the timings; the image also logs unavailable optional optimizer rules.
The shared-volume preparation grants group 185 write access and directory group
inheritance so OSS Spark's UID 185 and Quanton's UID 1001 can share table metadata
and warmup directories without changing table contents.

### Wide OCSF event input

`config/ocsf.properties` generates `bench.events.machine_events_ocsf_1_9_0` from
[pinned OCSF 1.9.0 definitions](https://schema.ocsf.io/1.9.0/). It enables all 86
concrete published classes, including deprecated and OS extension classes, and
samples each class's activity enumeration (616 class/activity combinations).
The abstract Base Event is not an event class to sample. The previous seven-event
and scalar-only profiles remain available for reproducing earlier results.

The stored schema has 200 promoted OCSF scalar columns, five benchmark columns
(`eventid`, `eventtime`, `cid`, `sid`, `eventtype`), and the `attributes`
`MAP<STRING,STRING>` column: 206 columns total. `eventtype` contains the canonical
OCSF class name; `class_uid`, `activity_id`, `category_uid`, and `type_uid` contain
OCSF identifiers, with `type_uid = class_uid * 100 + activity_id`. Existing UUID
strings, one-hour timestamps, customer bucket skew, and `(hour, cid bucket,
eventtype)` Iceberg partitioning are retained.

“Most common” means the number of concrete class definitions containing a scalar
path, not measured production traffic. Ties prefer shallower paths, then sort by
path name. [config/ocsf-top-fields.csv](config/ocsf-top-fields.csv) records each
selected path, SQL column, type, and class coverage. Dots in promoted paths become
`__` (for example, `metadata.version` becomes `metadata__version`). Numeric OCSF
integer/long/timestamp fields use Spark LONG, floats use DOUBLE, booleans use
BOOLEAN, and textual logical types use STRING. The helper `eventtime` remains a
Spark TIMESTAMP for hour partitioning.

Other populated fields use their original OCSF dotted paths as map keys. Scalars
are represented as strings; arrays and remaining objects are JSON values. Objects
are flattened through three object levels; recursive/deeper objects remain in
the map and are generated with bounded JSON nesting. These are synthetic,
OCSF-derived benchmark rows in a flattened layout, rather than native nested
OCSF JSON documents. Required paths are populated when their parent exists;
optional and recommended fields are sparse, controlled by
`dataset.ocsf.optionalFieldProbability` (0.15) and
`dataset.ocsf.recommendedFieldProbability` (0.8). Schema-backed enum IDs/captions,
UUIDs, addresses, hostnames, paths, schema version, profiles, and extension
metadata replace the old invented map padding fields. Legacy payload byte/fraction
quotas do not apply to this schema; the Parquet byte target still does.

Edit [config/ocsf-event-weights.properties](config/ocsf-event-weights.properties)
to set relative class frequencies:

```properties
dataset.eventWeight.process_activity=10
dataset.eventWeight.script_activity=1
```

This makes Process Activity ten times as likely as Script Activity, independently
of their different row sizes. Defaults include every class at weight 1, except
Process Activity at 10 as the example above. Zero disables a class. Omitted class
weights use `dataset.ocsf.defaultEventWeight` (1). Unknown names, duplicate aliases,
negative/non-finite weights, and an all-zero distribution fail before creation.
The `config.includes` directive loads the weights file relative to the profile;
effective job properties contain the resolved values, so workers do not need the
original file. Weights, selected schema, and pinned catalogue digest are included
in the input fingerprint. After changing the input contract, choose a fresh
`catalog.table`; existing input is never silently overwritten.

To generate or reuse input without running benchmarks, use the existing local
Minikube environment (and a cluster-matching kubectl on PATH):

```bash
scripts/bench image config/ocsf.properties config/engines/oss.properties
scripts/bench prepare config/ocsf.properties
```

To run the benchmarks later, use `--config config/ocsf.properties` with the normal
local orchestrator. The default target is approximately 20 GiB of committed
Parquet files with a 5% tolerance. `dataset.minBatches=8` keeps several small files
per class/bucket partition for compaction. Generation logs include exact per-class
record/file/byte counts derived from Iceberg metadata.

The official compiled schema, normalized catalogue, Apache license, source URL,
and checksums are bundled under `dataset-generator/src/main/resources/ocsf/`.
Generation does not call external schema services. `scripts/sync-ocsf-schema.py`
rebuilds the pinned resources and field/weight lists from the official API.
`verifyOcsf` tests all 616 class/activity combinations, field placement, Spark
encoding, enum classification, determinism, weight distribution, and input
fingerprints. `verifyOcsfTable` writes/reuses the real wide Iceberg table, checking
all 86 class UIDs and the map round trip.

For the wide OCSF workload, Comet's driver uses 4 GiB heap plus 4 GiB overhead
(an 8 GiB pod limit). The earlier 5 GiB limit was OOM-killed while planning/running
the many wide bin-pack rewrites. Executor count, cores, heap, and native memory
budgets remain unchanged. This driver allowance is part of the Comet engine
configuration and recorded with every result.

A stopped suite can skip already completed sample positions with
`benchmark.completedSequences=1,3,4`. Positions refer to the complete engine list,
repetition count, and alternating-order schedule; keep those and the input
baseline unchanged. Retain the original result artifacts separately and combine
only successful samples with matching configuration. A changed engine
configuration requires repeating that engine's affected samples.
