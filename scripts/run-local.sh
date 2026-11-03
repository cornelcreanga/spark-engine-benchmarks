#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PROJECT_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_DIR"

usage() {
    cat <<'USAGE'
Usage: scripts/run-local.sh smoke|full [options]

  --engine FILE       Benchmark this engine (repeatable; default: OSS).
  --config FILE       Override the profile's configuration file.
  --cluster NAME      Use/create this Minikube profile and Kubernetes context.
  --fresh-input       Generate a new uniquely named table; preserve existing data.
  --reuse-images      Require existing local images and load them into the cluster.
  --verify-code       Run Gradle build/verification before deployment.
  --skip-validation   Skip fingerprints and OSS/Trino data validators.
  --validate          Enable fingerprints and configured data validators.
  --plan-only         Build the runner and render manifests; do not contact Docker/Kubernetes.
  -h, --help          Show this help.

Smoke: 1 repetition, 6 cluster CPUs, 16 GiB cluster memory.
Full:  1 repetition, 12 cluster CPUs, 38 GiB cluster memory.
Infrastructure and input remain available afterward. No cluster/table is deleted.
USAGE
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    return 1
}

if [[ $# -eq 0 || ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi
PROFILE=$1
shift
case "$PROFILE" in
    smoke) CONFIG=config/smoke.properties ;;
    full | local)
        PROFILE=full
        CONFIG=config/local.properties
        ;;
    *)
        usage >&2
        die "Unknown profile: $PROFILE"
        ;;
esac

ENGINES=()
CLUSTER_OVERRIDE=
FRESH_INPUT=false
REUSE_IMAGES=false
VERIFY_CODE=false
PLAN_ONLY=false
VALIDATION_OVERRIDE=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --engine | --config | --cluster)
            [[ $# -ge 2 && -n $2 && $2 != --* ]] || die "$1 requires a value"
            case "$1" in
                --engine) ENGINES+=("$2") ;;
                --config) CONFIG=$2 ;;
                --cluster) CLUSTER_OVERRIDE=$2 ;;
            esac
            shift 2
            ;;
        --fresh-input)
            FRESH_INPUT=true
            shift
            ;;
        --reuse-images)
            REUSE_IMAGES=true
            shift
            ;;
        --verify-code)
            VERIFY_CODE=true
            shift
            ;;
        --skip-validation)
            VALIDATION_OVERRIDE=false
            shift
            ;;
        --validate)
            VALIDATION_OVERRIDE=true
            shift
            ;;
        --plan-only)
            PLAN_ONLY=true
            shift
            ;;
        --help | -h)
            usage
            exit 0
            ;;
        *) die "Unknown option: $1" ;;
    esac
done
if [[ ${#ENGINES[@]} -eq 0 ]]; then
    ENGINES=(config/engines/oss.properties)
fi
[[ -f $CONFIG ]] || die "Configuration does not exist: $CONFIG"
for engine in "${ENGINES[@]}"; do
    [[ -f $engine ]] || die "Engine definition does not exist: $engine"
done
if [[ -n $CLUSTER_OVERRIDE && ! $CLUSTER_OVERRIDE =~ ^[a-z][a-z0-9-]{0,39}$ ]]; then
    die "Cluster name must be a lowercase DNS label of at most 40 characters"
fi
for command in java python3 flock; do
    command -v "$command" >/dev/null || die "Required command is missing: $command"
done

# An existing docker-group membership can be activated without sudo or changing the host.
if [[ $PLAN_ONLY == false ]]; then
    for command in docker minikube timeout; do
        command -v "$command" >/dev/null || die "Required command is missing: $command"
    done
    if ! timeout 20 docker info >/dev/null 2>&1; then
        if [[ ${SPARKBENCH_GROUP_RETRY:-0} != 1 ]] && command -v sg >/dev/null &&
            timeout 20 sg docker -c 'docker info >/dev/null 2>&1'; then
            export SPARKBENCH_GROUP_RETRY=1
            # Preserve the original parsed arguments with explicit array construction.
            retry=("$BASH" "$SCRIPT_DIR/run-local.sh" "$PROFILE" --config "$CONFIG")
            for engine in "${ENGINES[@]}"; do
                retry+=(--engine "$engine")
            done
            [[ -z $CLUSTER_OVERRIDE ]] || retry+=(--cluster "$CLUSTER_OVERRIDE")
            [[ $FRESH_INPUT == false ]] || retry+=(--fresh-input)
            [[ $REUSE_IMAGES == false ]] || retry+=(--reuse-images)
            [[ $VERIFY_CODE == false ]] || retry+=(--verify-code)
            [[ $VALIDATION_OVERRIDE != false ]] || retry+=(--skip-validation)
            [[ $VALIDATION_OVERRIDE != true ]] || retry+=(--validate)
            quoted=$(python3 -c 'import shlex,sys; print(shlex.join(sys.argv[1:]))' "${retry[@]}")
            exec sg docker -c "$quoted"
        fi
        die "Docker is unavailable. Check 'docker info', start the daemon, or refresh Docker group membership."
    fi
fi

mkdir -p build/local-runs build/locks
exec 9>build/locks/local-orchestrator.lock
flock -n 9 || die "Another local orchestration is running; wait for it to finish."
RUN_ID="$PROFILE-$(date -u +%Y%m%dT%H%M%SZ)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
RUN_DIR="$PROJECT_DIR/build/local-runs/$RUN_ID"
mkdir -p "$RUN_DIR"
exec > >(tee -a "$RUN_DIR/console.log") 2>&1
CURRENT_STEP=initialization
CONTEXT=
NAMESPACE=
CLUSTER=

collect_diagnostics() {
    mkdir -p "$RUN_DIR/diagnostics"
    if [[ -n $CLUSTER ]] && command -v minikube >/dev/null; then
        timeout 20 minikube -p "$CLUSTER" status --output=json \
            >"$RUN_DIR/diagnostics/minikube.json" 2>&1 || true
    fi
    if [[ -n $CONTEXT && -n $NAMESPACE ]] && command -v kubectl >/dev/null; then
        for resource in pods deployments pvc sparkapplications events; do
            timeout 20 kubectl --context "$CONTEXT" -n "$NAMESPACE" --request-timeout=15s \
                get "$resource" -o yaml >"$RUN_DIR/diagnostics/$resource.yaml" 2>&1 || true
        done
        timeout 20 kubectl --context "$CONTEXT" -n "$NAMESPACE" --request-timeout=15s \
            describe pods >"$RUN_DIR/diagnostics/pods-describe.txt" 2>&1 || true
    fi
}

on_failure() {
    local status=$1
    local line=$2
    trap - ERR INT TERM
    set +e
    printf '\nFAILED: %s (exit %s, line %s)\n' "$CURRENT_STEP" "$status" "$line"
    printf '%s\n' "$CURRENT_STEP" >"$RUN_DIR/failed-step.txt"
    collect_diagnostics
    printf 'Logs and diagnostics: %s\n' "$RUN_DIR"
    printf 'Infrastructure and data were retained. Fix the cause and rerun the same command.\n'
    if [[ -n $CONTEXT ]]; then
        printf 'Inspect applications: kubectl --context %q -n %q get sparkapplications\n' "$CONTEXT" "$NAMESPACE"
        printf 'For incomplete/incompatible input, rerun with --fresh-input.\n'
        printf 'If a previous writer is still active, stop it before restoring: scripts/bench reset %q\n' "$RUNTIME_CONFIG"
        printf 'For an undersized existing cluster, use --cluster NEW_NAME; no cluster is deleted automatically.\n'
    fi
    exit "$status"
}
trap 'on_failure "$?" "$LINENO"' ERR
trap 'on_failure 130 "$LINENO"' INT
trap 'on_failure 143 "$LINENO"' TERM

step() {
    CURRENT_STEP=$1
    shift
    printf '\n== %s ==\n' "$CURRENT_STEP"
    "$@"
}

build_project() {
    if [[ $VERIFY_CODE == true ]]; then
        ./gradlew build verify :runner:installDist :jobs:jar --no-daemon
    else
        ./gradlew :runner:installDist :jobs:jar --no-daemon
    fi
}
step 'Build Java applications' build_project
RUNNER="$PROJECT_DIR/runner/build/install/runner/bin/runner"
CONFIG=$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$CONFIG")
RUNTIME_CONFIG="$RUN_DIR/config.properties"
printf 'config.parent=%s\nresults.directory=%s/results\n' "$CONFIG" "$RUN_DIR" >"$RUNTIME_CONFIG"
if [[ -n $VALIDATION_OVERRIDE ]]; then
    printf 'validation.enabled=%s\n' "$VALIDATION_OVERRIDE" >>"$RUNTIME_CONFIG"
fi
if [[ -n $CLUSTER_OVERRIDE ]]; then
    printf 'k8s.context=%s\nk8s.minikubeProfile=%s\n' "$CLUSTER_OVERRIDE" "$CLUSTER_OVERRIDE" >>"$RUNTIME_CONFIG"
fi
SETTINGS="$RUN_DIR/settings.json"
"$RUNNER" local-settings "$RUNTIME_CONFIG" "${ENGINES[@]}" >"$SETTINGS"
setting() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$SETTINGS" "$1"
}
if [[ $FRESH_INPUT == true ]]; then
    table=$(setting catalog.table)
    suffix=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')
    printf 'catalog.table=%s_%s\n' "$table" "$suffix" >>"$RUNTIME_CONFIG"
    "$RUNNER" local-settings "$RUNTIME_CONFIG" "${ENGINES[@]}" >"$SETTINGS"
fi
CLUSTER=$(setting k8s.minikubeProfile)
CONTEXT=$(setting k8s.context)
NAMESPACE=$(setting k8s.namespace)
CPUS=$(setting local.minikube.cpus)
MEMORY_MIB=$(setting local.minikube.memoryMiB)
START_TIMEOUT=$(setting local.minikube.timeoutSeconds)
DISK_GIB=$(setting local.minFreeDiskGiB)
[[ $CONTEXT == "$CLUSTER" ]] || die "Local Kubernetes context must match its Minikube profile; use --cluster NAME."
step 'Validate configuration and render manifests' "$RUNNER" plan "$RUNTIME_CONFIG" "${ENGINES[@]}"
if [[ $PLAN_ONLY == true ]]; then
    printf '\nPlan ready. No infrastructure changed. Artifacts: %s\n' "$RUN_DIR"
    exit 0
fi

check_host_capacity() {
    docker info --format '{{json .}}' >"$RUN_DIR/docker-info.json"
    python3 - "$RUN_DIR/docker-info.json" "$CPUS" "$MEMORY_MIB" <<'PY'
import json
import sys
info = json.load(open(sys.argv[1]))
cpus, memory_mib = map(int, sys.argv[2:])
if cpus > info['NCPU']:
    raise SystemExit(f"Requested {cpus} CPUs but Docker has {info['NCPU']}; adjust local.minikube.cpus.")
reserve_mib = 4096
available_mib = info['MemTotal'] // 1024**2
if memory_mib + reserve_mib > available_mib:
    raise SystemExit(f"Requested {memory_mib} MiB plus {reserve_mib} MiB host reserve exceeds Docker/host {available_mib} MiB; use smoke or a smaller profile.")
PY
}
step 'Check host CPU and memory budget' check_host_capacity

ensure_helm() {
    if command -v helm >/dev/null && helm version --short | grep -q '^v3\.'; then
        return
    fi
    for command in curl tar sha256sum; do
        command -v "$command" >/dev/null || die "Helm bootstrap requires $command"
    done
    local version os architecture directory archive expected actual
    version=$(setting local.helm.version)
    [[ $version =~ ^v3\.[0-9]+\.[0-9]+$ ]] || die "Invalid Helm version: $version"
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$(uname -m)" in
        x86_64) architecture=amd64 ;;
        aarch64 | arm64) architecture=arm64 ;;
        *) die "Unsupported Helm architecture: $(uname -m)" ;;
    esac
    directory="$PROJECT_DIR/build/tools/helm/$version"
    mkdir -p "$directory"
    export PATH="$directory:$PATH"
    if [[ ! -x $directory/helm ]]; then
        archive="$directory/helm.tar.gz"
        curl --fail --location --retry 3 --connect-timeout 20 --max-time 180 \
            "https://get.helm.sh/helm-$version-$os-$architecture.tar.gz" -o "$archive"
        curl --fail --location --retry 3 --connect-timeout 20 --max-time 60 \
            "https://get.helm.sh/helm-$version-$os-$architecture.tar.gz.sha256sum" -o "$archive.sha256"
        read -r expected _ <"$archive.sha256"
        actual=$(sha256sum "$archive")
        [[ ${actual%% *} == "$expected" ]] || die "Helm checksum mismatch; remove $archive and retry."
        tar -xzf "$archive" -C "$directory" "$os-$architecture/helm"
        mv "$directory/$os-$architecture/helm" "$directory/helm"
        chmod +x "$directory/helm"
    fi
    helm version --short
}
step 'Ensure Helm 3 is available' ensure_helm
step 'Start or resume Minikube' minikube -p "$CLUSTER" start --driver=docker \
    --cpus="$CPUS" --memory="$MEMORY_MIB" --keep-context --wait=apiserver,system_pods,node_ready --wait-timeout="${START_TIMEOUT}s"

ensure_matching_kubectl() {
    local tools_directory="$RUN_DIR/tools"
    mkdir -p "$tools_directory"
    printf '#!/usr/bin/env bash\nexec minikube -p %q kubectl -- "$@"\n' "$CLUSTER" >"$tools_directory/kubectl"
    chmod +x "$tools_directory/kubectl"
    export PATH="$tools_directory:$PATH"
    timeout 180 kubectl version --client --output=json
}
step 'Ensure kubectl matches the cluster' ensure_matching_kubectl

check_cluster() {
    kubectl --context "$CONTEXT" --request-timeout=30s wait --for=condition=Ready nodes --all --timeout=120s
    kubectl --context "$CONTEXT" --request-timeout=30s get nodes -o json >"$RUN_DIR/nodes.json"
    python3 - "$RUN_DIR/nodes.json" "$CPUS" "$MEMORY_MIB" <<'PY'
import json
import sys
nodes = json.load(open(sys.argv[1]))['items']
if len(nodes) != 1:
    raise SystemExit('Local orchestration expects a single Minikube node.')
capacity = nodes[0]['status']['capacity']
def memory_mib(value):
    for suffix, multiplier in [('Ki', 1/1024), ('Mi', 1), ('Gi', 1024)]:
        if value.endswith(suffix):
            return float(value[:-len(suffix)]) * multiplier
    return float(value) / 1024**2
if int(capacity['cpu']) < int(sys.argv[2]) or memory_mib(capacity['memory']) < int(sys.argv[3]) * .95:
    raise SystemExit('Existing cluster is too small. Use --cluster NEW_NAME with the desired profile; existing clusters/data will not be deleted.')
PY
    minikube -p "$CLUSTER" ssh -- 'df -Pk /var' >"$RUN_DIR/cluster-disk.txt"
    python3 - "$RUN_DIR/cluster-disk.txt" "$DISK_GIB" <<'PY'
import sys
rows = [line.split() for line in open(sys.argv[1]) if line.strip()]
try:
    free_gib = int(rows[-1][3]) / 1024**2
except (IndexError, ValueError) as error:
    raise SystemExit(f'Cannot determine free cluster disk: {error}')
if free_gib < int(sys.argv[2]):
    raise SystemExit(f'Only {free_gib:.1f} GiB free in cluster storage; need at least {sys.argv[2]} GiB. Free Docker storage or use a smaller dataset.')
PY
    docker inspect --type container "$CLUSTER" --format '{{json .HostConfig}}' >"$RUN_DIR/container-resources.json"
    python3 - "$RUN_DIR/container-resources.json" "$CPUS" "$MEMORY_MIB" <<'PYLIMITS'
import json
import sys
limits = json.load(open(sys.argv[1]))
cpus, memory = int(sys.argv[2]), int(sys.argv[3]) * 1024**2
if (limits.get('NanoCpus', 0) and limits['NanoCpus'] < cpus * 10**9) or (limits.get('Memory', 0) and limits['Memory'] < memory):
    raise SystemExit('Existing Docker cluster limits are too small; use --cluster NEW_NAME, or resize the stopped profile yourself. No data was deleted.')
PYLIMITS
    if kubectl --context "$CONTEXT" --request-timeout=15s get namespace "$NAMESPACE" >/dev/null 2>&1 &&
        kubectl --context "$CONTEXT" --request-timeout=15s get crd sparkapplications.sparkoperator.k8s.io >/dev/null 2>&1; then
        kubectl --context "$CONTEXT" -n "$NAMESPACE" --request-timeout=30s \
            get sparkapplications -o json >"$RUN_DIR/existing-applications.json"
        python3 - "$RUN_DIR/existing-applications.json" <<'PY'
import json
import sys
terminal = {'COMPLETED', 'FAILED', 'SUBMISSION_FAILED', 'FAILED_SUBMISSION'}
active = [app['metadata']['name'] for app in json.load(open(sys.argv[1])).get('items', [])
          if app.get('status', {}).get('applicationState', {}).get('state') not in terminal]
if active:
    raise SystemExit('Existing Spark applications may still be writing. Wait for or stop them before retrying: ' + ', '.join(active))
PY
    fi
}
step 'Check node capacity, storage, and existing jobs' check_cluster
step 'Provision LocalStack, catalog, Trino, and Spark Operator' "$RUNNER" provision "$RUNTIME_CONFIG"
step 'Wait for SparkApplication API' kubectl --context "$CONTEXT" wait \
    --for=condition=Established crd/sparkapplications.sparkoperator.k8s.io --timeout=120s

build_images() {
    python3 - "$SETTINGS" >"$RUN_DIR/images.tsv" <<'PY'
import json
import sys
seen = set()
for item in json.load(open(sys.argv[1]))['images']:
    if item['image'] not in seen:
        print(item['config'] + '\t' + item['image'])
        seen.add(item['image'])
PY
    while IFS=$'\t' read -r engine image; do
        if [[ $REUSE_IMAGES == true ]]; then
            docker image inspect "$image" >/dev/null || die "Image missing: $image. Rerun without --reuse-images."
        else
            "$RUNNER" image "$RUNTIME_CONFIG" "$engine"
        fi
        # Loading is required even for reused images or a newly created cluster.
        minikube -p "$CLUSTER" image load "$image"
    done <"$RUN_DIR/images.tsv"
}
step 'Build and load OSS/control and requested engine images' build_images
step 'Create input or verify the existing baseline' "$RUNNER" prepare "$RUNTIME_CONFIG"
step 'Run compaction, optional validation, and baseline restoration' "$RUNNER" run "$RUNTIME_CONFIG" "${ENGINES[@]}"
step 'Summarize this invocation only' python3 scripts/summarize.py "$RUN_DIR/results" --csv "$RUN_DIR/summary.csv"
printf '\nBenchmark completed.\nArtifacts: %s\nSummary: %s/summary.csv\nEffective configuration: %s\n' \
    "$RUN_DIR" "$RUN_DIR" "$RUNTIME_CONFIG"
