#!/usr/bin/env bash
#===============================================================================
# scripts/setup.sh
#
# Setup helper for the genexomics pipeline.
# (updated to reliably create S3 buckets in LocalStack using awslocal)
#===============================================================================
set -euo pipefail
IFS=$'\n\t'

# ----------------------
# Constants / defaults
# ----------------------
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$REPO_ROOT" || exit 1

ENV_FILE=".env"
ENV_EXAMPLE=".env.example"
S3_CONFIG="config/s3_config.yaml"

# Defaults
MODE="test"
WITH_SAMPLE_DATA=false
RUN_PIPELINE=false
BUILD_IMAGE=false
MINIMAL_DATASET=false

# Minimal dataset default URLs (can be overridden via environment variables MIN_R1_URL / MIN_R2_URL)
MIN_R1_URL="${MIN_R1_URL:-https://raw.githubusercontent.com/nf-core/test-datasets/scrnaseq/testdata/cellranger/Sample_X_S1_L001_R1_001.fastq.gz}"
MIN_R2_URL="${MIN_R2_URL:-https://raw.githubusercontent.com/nf-core/test-datasets/scrnaseq/testdata/cellranger/Sample_X_S1_L001_R2_001.fastq.gz}"

# Exit codes
EX_OK=0
EX_RUNTIME=1
EX_CONFIG=2
EX_PRECONDITION=3

# Detect whether to map host UID/GID into container.
DOCKER_USER_FLAG=""
if [[ "$(uname -s)" != "Darwin" ]]; then
  DOCKER_USER_FLAG="-u $(id -u):$(id -g)"
fi

# Trap to report error line
_on_error() {
  local rc=$?
  local line=${1:-"unknown"}
  >&2 printf -- "[ERROR] setup.sh failed at line %s (exit code: %s)\n" "${line}" "${rc}"
  exit "${rc}"
}
trap ' _on_error "$LINENO" ' ERR

# ----------------------
# Logging helpers
# ----------------------
log() {
  printf "\n[setup] %s\n" "$*"
}

warn() {
  printf "\n[setup][WARN] %s\n" "$*"
}

err() {
  printf "\n[ERROR] %s\n" "$*" >&2
  exit "${EX_RUNTIME}"
}

usage() {
  cat <<'EOF'
Usage:
  scripts/setup.sh --mode test [--with-sample-data] [--run-pipeline] [--minimal-dataset]
  scripts/setup.sh --mode prod [--build-image]

Options:
  --mode <test|prod>       Mode to setup (default: test)
  --with-sample-data       (test only) Generate sample data inside pipeline container
  --run-pipeline           (test only) Run Nextflow inside the pipeline container
  --minimal-dataset        (test only) Use a minimal dataset (two FASTQ files) instead of full sample generation
  --build-image            (prod only) Build the pipeline docker image
  -b, --bucket-key KEY     Named bucket key in YAML (default: "default")
  -s, --section SECTION    YAML top-level section (default: genexomics)
  -h, --help               Show this help
EOF
  exit "${EX_CONFIG}"
}

# ----------------------
# Argument parsing
# ----------------------
SECTION="genexomics"
BUCKET_KEY="default"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      shift
      MODE="${1:-}"
      ;;
    --with-sample-data)
      WITH_SAMPLE_DATA=true
      ;;
    --run-pipeline)
      RUN_PIPELINE=true
      ;;
    --build-image)
      BUILD_IMAGE=true
      ;;
    --minimal-dataset)
      MINIMAL_DATASET=true
      ;;
    -b|--bucket-key)
      shift
      BUCKET_KEY="${1:-}"
      ;;
    -s|--section)
      shift
      SECTION="${1:-genexomics}"
      ;;
    -h|--help)
      usage
      ;;
    *)
      printf "Unknown argument: %s\n" "$1" >&2
      usage
      ;;
  esac
  shift
done

# ----------------------
# Utility checks
# ----------------------
_check_cmd() {
  command -v "$1" >/dev/null 2>&1
}

ensure_docker() {
  if ! _check_cmd docker; then
    printf "[ERROR] docker not found. Install Docker and retry.\n" >&2
    exit "${EX_PRECONDITION}"
  fi

  if ! docker compose version >/dev/null 2>&1; then
    printf "[ERROR] docker compose (v2 plugin) not available. Ensure 'docker compose' works.\n" >&2
    exit "${EX_PRECONDITION}"
  fi
}

ensure_aws_cli() {
  if ! _check_cmd aws; then
    printf "[ERROR] aws CLI not found. Install and configure credentials first.\n" >&2
    exit "${EX_CONFIG}"
  fi
}

# ----------------------
# YAML extraction helper (unchanged)
# ----------------------
get_bucket_and_prefix() {
  local yaml_file="$S3_CONFIG"
  local section="$SECTION"
  local bucket_key="$BUCKET_KEY"

  if [[ ! -f "$yaml_file" ]]; then
    printf "[ERROR] S3 config not found: %s\n" "$yaml_file" >&2
    return 1
  fi

  if _check_cmd yq; then
    local bucket
    local prefix
    bucket="$(yq e ".${section}.buckets.\"${bucket_key}\".Bucket" "$yaml_file" 2>/dev/null || true)"
    prefix="$(yq e ".${section}.buckets.\"${bucket_key}\".Prefix" "$yaml_file" 2>/dev/null || true)"
    bucket="${bucket:-}"
    prefix="${prefix:-}"
    if [[ -n "$bucket" && "$bucket" != "null" ]]; then
      printf "%s %s\n" "$bucket" "${prefix:-.}"
      return 0
    fi
  fi

  if _check_cmd python3; then
    if python3 - <<'PY' 2>/dev/null
try:
    import yaml, sys
    sys.exit(0)
except Exception:
    sys.exit(2)
PY
    then
      local pyout
      pyout="$(python3 - <<PY
import sys, yaml
f = "$yaml_file"
section = "$section"
bucket_key = "$bucket_key"
try:
    with open(f) as fh:
        data = yaml.safe_load(fh)
except Exception as e:
    print("", end="")
    sys.exit(0)
if not isinstance(data, dict):
    sys.exit(0)
sec = data.get(section, {})
if not isinstance(sec, dict):
    sys.exit(0)
buckets = sec.get("buckets", {})
if not isinstance(buckets, dict):
    sys.exit(0)
cfg = buckets.get(bucket_key, {})
if not isinstance(cfg, dict):
    sys.exit(0)
bucket = cfg.get("Bucket", "") or ""
prefix = cfg.get("Prefix", "") or ""
print(bucket, prefix)
PY
)"
      pyout="$(echo "$pyout" | tr -d '\r' | sed -n '1p' || true)"
      if [[ -n "${pyout// /}" ]]; then
        read -r bucket prefix <<<"$pyout" || true
        prefix="${prefix:-.}"
        printf "%s %s\n" "$bucket" "$prefix"
        return 0
      fi
    fi
  fi

  # Awk fallback
  local awk_bucket awk_prefix
  awk_bucket="$(awk -v section="$SECTION" -v key="$BUCKET_KEY" '
    $0 ~ section ":" { in_section=1; next }
    in_section && $0 ~ /^[[:space:]]*buckets:/ { in_buckets=1; next }
    in_buckets && $0 ~ "^[[:space:]]*" key ":" { in_target=1; next }
    in_target && $0 ~ "^[[:space:]]*Bucket:" { print $2; exit }
  ' "$yaml_file" 2>/dev/null || true)"
  awk_prefix="$(awk -v section="$SECTION" -v key="$BUCKET_KEY" '
    $0 ~ section ":" { in_section=1; next }
    in_section && $0 ~ /^[[:space:]]*buckets:/ { in_buckets=1; next }
    in_buckets && $0 ~ "^[[:space:]]*" key ":" { in_target=1; next }
    in_target && $0 ~ "^[[:space:]]*Prefix:" { print $2; exit }
  ' "$yaml_file" 2>/dev/null || true)"

  awk_bucket="${awk_bucket:-}"
  awk_prefix="${awk_prefix:-}"
  if [[ -n "$awk_bucket" ]]; then
    printf "%s %s\n" "$awk_bucket" "${awk_prefix:-.}"
    return 0
  fi

  printf "[ERROR] Failed to extract Bucket from %s (section=%s, bucket_key=%s). Consider installing 'yq' or 'PyYAML'.\n" "$yaml_file" "$SECTION" "$BUCKET_KEY" >&2
  return 1
}

# ----------------------
# Helper: ensure .env exists and load it into shell
# ----------------------
ensure_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    if [[ -f "$ENV_EXAMPLE" ]]; then
      cp "$ENV_EXAMPLE" "$ENV_FILE"
      log "Copied $ENV_EXAMPLE -> $ENV_FILE. Edit $ENV_FILE if needed."
    else
      printf "[ERROR] %s not found; create %s with env vars (AWS keys, tokens).\n" "$ENV_EXAMPLE" "$ENV_FILE" >&2
      exit "${EX_CONFIG}"
    fi
  else
    log "$ENV_FILE already exists - leaving untouched."
  fi
}

load_env() {
  # Load variables from .env into the script environment, if present
  if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -o allexport
    # use a subshell-safe source of key=val lines
    # filter out comments and empty lines
    awk 'BEGIN{FS="="} $0!~/^[[:space:]]*#/ && NF{print}' "$ENV_FILE" > /tmp/.env.sh.$$ || true
    # shellcheck disable=SC1091
    source /tmp/.env.sh.$$
    rm -f /tmp/.env.sh.$$
    set +o allexport
  fi
}

# ----------------------
# Docker / LocalStack helpers
# ----------------------
build_image() {
  log "Building pipeline image with docker compose..."
  docker compose build pipeline
}

_wait_for_localstack() {
  local endpoint="http://localhost:4566/health"
  local tries=0
  local max_tries=30

  if _check_cmd curl; then
    log "Waiting for LocalStack health endpoint ${endpoint} (timeout ~${max_tries}s)"
    while [[ $tries -lt $max_tries ]]; do
      if curl -fsS "${endpoint}" >/dev/null 2>&1; then
        log "LocalStack health endpoint is responding."
        return 0
      fi
      tries=$((tries + 1))
      sleep 1
    done
    warn "Timed out waiting for LocalStack health endpoint (${endpoint}). Continuing, but LocalStack may not be ready."
    return 1
  else
    log "curl not available; falling back to docker logs checks for LocalStack readiness."
    tries=0
    until docker compose logs localstack --no-log-prefix --tail=50 | grep -q "Ready." || [[ $tries -ge 30 ]]; do
      sleep 1
      tries=$((tries+1))
    done
    if [[ $tries -ge 30 ]]; then
      warn "LocalStack did not show 'Ready.' in logs within timeout."
      return 1
    fi
    return 0
  fi
}

start_localstack() {
  log "Starting LocalStack service with docker compose..."
  docker compose up -d localstack
  if ! _wait_for_localstack; then
    warn "LocalStack readiness checks failed - proceed with caution."
  else
    log "LocalStack appears ready."
  fi
}

# Parse an s3:// URI and return only the bucket name (strip s3:// and any path)
_s3_uri_to_bucket() {
  local uri="$1"
  # remove s3://
  uri="${uri#s3://}"
  # extract until first slash
  echo "${uri%%/*}"
}

create_s3_bucket_local() {
  read -r BUCKET PREFIX < <(get_bucket_and_prefix) || {
    printf "[ERROR] Unable to determine target S3 bucket/prefix from %s\n" "$S3_CONFIG" >&2
    return 1
  }

  # determine LocalStack container name (allow override via env LOCALSTACK_DOCKER_NAME)
  local LOCALSTACK_CONTAINER="${LOCALSTACK_DOCKER_NAME:-localstack-main}"

  log "Creating S3 bucket in LocalStack: ${BUCKET} (prefix ${PREFIX}) using container ${LOCALSTACK_CONTAINER}"

  # Ensure container is running
  if ! docker ps --format '{{.Names}}' | grep -qE "^${LOCALSTACK_CONTAINER}\$"; then
    warn "LocalStack container '${LOCALSTACK_CONTAINER}' not running. Attempting to start it."
    docker compose up -d localstack
    sleep 2
  fi

  # Try using awslocal inside LocalStack container (no host credentials needed)
  if docker exec "${LOCALSTACK_CONTAINER}" awslocal --version >/dev/null 2>&1; then
    # create the main bucket (idempotent)
    if docker exec "${LOCALSTACK_CONTAINER}" awslocal s3 ls "s3://${BUCKET}" >/dev/null 2>&1; then
      log "Bucket already exists in LocalStack: ${BUCKET}"
    else
      log "Creating bucket ${BUCKET} via awslocal ..."
      if docker exec "${LOCALSTACK_CONTAINER}" awslocal s3 mb "s3://${BUCKET}"; then
        log "Created bucket: ${BUCKET}"
      else
        warn "awslocal failed to create bucket ${BUCKET}"
      fi
    fi
  else
    warn "awslocal not found in LocalStack container; falling back to boto3 inside pipeline container"
    # Fallback: attempt to create using boto3 inside pipeline container (existing approach)
    docker compose run --rm -T \
      ${DOCKER_USER_FLAG} \
      -e BUCKET="${BUCKET}" \
      -e PREFIX="${PREFIX}" \
      -e S3_ENDPOINT="${S3_ENDPOINT:-http://localstack:4566}" \
      pipeline micromamba run -n genexomics --no-capture-output python - <<'PY'
import os, sys
try:
    import boto3
except Exception as e:
    print("boto3 not available inside container:", e, file=sys.stderr)
    sys.exit(2)
ep = os.environ.get('S3_ENDPOINT', 'http://localstack:4566')
b = os.environ.get('BUCKET', '')
if not b:
    print("No BUCKET environment variable provided.", file=sys.stderr)
    sys.exit(2)
s3 = boto3.client('s3', endpoint_url=ep)
try:
    s3.head_bucket(Bucket=b)
    print('Bucket exists:', b)
except Exception:
    try:
        s3.create_bucket(Bucket=b)
        print('Created bucket:', b)
    except Exception as ex:
        print('Failed to create bucket:', ex, file=sys.stderr)
        sys.exit(2)
PY
  fi

  # Also create Quilt registry bucket if QUILT_REGISTRY is set and looks like s3://...
  if [[ -n "${QUILT_REGISTRY:-}" ]]; then
    local qbucket
    qbucket="$(_s3_uri_to_bucket "${QUILT_REGISTRY}")"
    if [[ -n "$qbucket" && "$qbucket" != "$BUCKET" ]]; then
      log "Ensuring Quilt registry bucket exists in LocalStack: ${qbucket}"
      if docker exec "${LOCALSTACK_CONTAINER}" awslocal --version >/dev/null 2>&1; then
        if docker exec "${LOCALSTACK_CONTAINER}" awslocal s3 ls "s3://${qbucket}" >/dev/null 2>&1; then
          log "Quilt registry bucket already exists: ${qbucket}"
        else
          docker exec "${LOCALSTACK_CONTAINER}" awslocal s3 mb "s3://${qbucket}" && log "Created quilt bucket ${qbucket}" || warn "Failed creating quilt bucket ${qbucket}"
        fi
      else
        # fallback via boto3 inside pipeline container
        docker compose run --rm -T \
          ${DOCKER_USER_FLAG} \
          -e BUCKET="${qbucket}" \
          -e S3_ENDPOINT="${S3_ENDPOINT:-http://localstack:4566}" \
          pipeline micromamba run -n genexomics --no-capture-output python - <<'PY'
import os, sys
try:
    import boto3
except Exception as e:
    print("boto3 not available inside container:", e, file=sys.stderr)
    sys.exit(2)
ep = os.environ.get('S3_ENDPOINT', 'http://localstack:4566')
b = os.environ.get('BUCKET', '')
s3 = boto3.client('s3', endpoint_url=ep)
try:
    s3.head_bucket(Bucket=b)
    print('Bucket exists:', b)
except Exception:
    try:
        s3.create_bucket(Bucket=b)
        print('Created bucket:', b)
    except Exception as ex:
        print('Failed to create bucket:', ex, file=sys.stderr)
        sys.exit(2)
PY
      fi
    fi
  else
    log "QUILT_REGISTRY not set; skipping Quilt bucket creation."
  fi

  return 0
}

create_s3_bucket_aws() {
  read -r BUCKET PREFIX < <(get_bucket_and_prefix) || {
    printf "[ERROR] Unable to determine target S3 bucket/prefix from %s\n" "$S3_CONFIG" >&2
    exit "${EX_CONFIG}"
  }

  log "Creating S3 bucket in AWS account: ${BUCKET} (region from aws config)"
  if ! _check_cmd aws; then
    printf "[ERROR] aws CLI not found. Install and configure credentials (aws configure).\n" >&2
    exit "${EX_CONFIG}"
  fi

  if ! aws sts get-caller-identity >/dev/null 2>&1; then
    printf "[ERROR] AWS credentials invalid or not configured. Run 'aws configure' or set env vars.\n" >&2
    exit "${EX_CONFIG}"
  fi

  local REGION
  REGION="$(aws configure get region || echo us-east-1)"
  if [[ "$REGION" == "us-east-1" ]]; then
    if aws s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
      log "Created bucket ${BUCKET} in us-east-1"
    else
      warn "Bucket create may have failed (already exists or insufficient permissions)."
    fi
  else
    if aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$REGION" >/dev/null 2>&1; then
      log "Created bucket ${BUCKET} in ${REGION}"
    else
      warn "Bucket create may have failed (already exists or insufficient permissions)."
    fi
  fi
  log "Check bucket with: aws s3 ls s3://${BUCKET}"
}

# ----------------------
# Sample data / Nextflow helpers
# ----------------------
_download_url() {
  local url="$1" dest="$2"
  if [[ -f "$dest" ]]; then
    log "File already exists: $dest - skipping download"
    return 0
  fi

  if _check_cmd curl; then
    log "Downloading $url -> $dest (curl)"
    curl -fSL --retry 3 -o "$dest" "$url"
    return $?
  fi
  if _check_cmd wget; then
    log "Downloading $url -> $dest (wget)"
    wget -q -O "$dest" "$url"
    return $?
  fi

  if _check_cmd python3; then
    log "Downloading $url -> $dest (python)"
    python3 - <<PY
import sys, urllib.request
url = sys.argv[1]
d = sys.argv[2]
try:
    urllib.request.urlretrieve(url, d)
except Exception as e:
    print('ERROR', e, file=sys.stderr)
    sys.exit(2)
PY
 "$url" "$dest"
    return $?
  fi

  warn "No downloader available (curl/wget/python3). Cannot fetch $url"
  return 2
}

generate_sample_data() {
  log "Generating sample data inside pipeline container (nf-core helper)"

  mkdir -p "${REPO_ROOT}/tests/nas/"

  if [[ "${MINIMAL_DATASET}" == "true" || "${MINIMAL_DATASET}" == "True" ]]; then
    log "Using minimal dataset: downloading two FASTQ files into tests/nas/run_example"
    mkdir -p tests/nas/run_example
    local r1_dest="tests/nas/run_example/Sample_X_S1_L001_R1_001.fastq.gz"
    local r2_dest="tests/nas/run_example/Sample_X_S1_L001_R2_001.fastq.gz"
    if ! _download_url "$MIN_R1_URL" "$r1_dest"; then
      warn "Failed to download R1 from $MIN_R1_URL"
    fi
    if ! _download_url "$MIN_R2_URL" "$r2_dest"; then
      warn "Failed to download R2 from $MIN_R2_URL"
    fi
    log "Minimal dataset placed in tests/nas/run_example/"
    return 0
  fi

  docker compose run --rm -T \
    ${DOCKER_USER_FLAG} \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${REPO_ROOT}":/workspace \
    pipeline bash -lc "set -euo pipefail; \
      if [ -f /workspace/tests/generate_sample_data.sh ]; then \
        chmod +x /workspace/tests/generate_sample_data.sh; \
        /workspace/tests/generate_sample_data.sh /workspace/tests/nas; \
      else \
        echo '[ERROR] /workspace/tests/generate_sample_data.sh not found' >&2; exit 2; \
      fi"

  log "Contents of tests/nas on host:"
  ls -la tests/nas || warn "No files created in tests/nas"
}

run_nextflow() {
  if [[ ! -S /var/run/docker.sock ]]; then
    printf "[ERROR] /var/run/docker.sock is not available on host. Required to run Nextflow with Docker executor inside container.\n" >&2
    exit "${EX_PRECONDITION}"
  fi

  log "Running Nextflow main.nf inside pipeline container (Docker executor)"

  docker compose run --rm -T \
    ${DOCKER_USER_FLAG} \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${REPO_ROOT}":/workspace \
    pipeline micromamba run -n genexomics --no-capture-output \
      nextflow run /workspace/main.nf -resume
}

# ----------------------
# Main execution
# ----------------------
log "Starting setup (mode=${MODE})"

ensure_docker
ensure_env
load_env

case "${MODE}" in
  test)
    log "MODE=test: preparing local test environment"

    # Build image by default in test mode to ensure pipeline container exists
    if [[ "${BUILD_IMAGE}" == "true" || "${BUILD_IMAGE}" == "True" ]]; then
      build_image
    else
      build_image
    fi

    start_localstack

    if ! create_s3_bucket_local; then
      warn "Local S3 bucket creation reported issues; please inspect LocalStack logs."
    fi

    if [[ "${WITH_SAMPLE_DATA}" == "true" || "${WITH_SAMPLE_DATA}" == "True" || "${MINIMAL_DATASET}" == "true" || "${MINIMAL_DATASET}" == "True" ]]; then
      generate_sample_data
      log "Sample data generation complete."
    fi

    if [[ "${RUN_PIPELINE}" == "true" || "${RUN_PIPELINE}" == "True" ]]; then
      run_nextflow
    fi

    log "TEST setup complete."
    ;;

  prod)
    log "MODE=prod: preparing production resources"

    if [[ "${BUILD_IMAGE}" == "true" || "${BUILD_IMAGE}" == "True" ]]; then
      build_image
    fi

    create_s3_bucket_aws
    log "PROD setup complete. Please update production deployment with your orchestration system as needed."
    ;;

  *)
    printf "[ERROR] Unknown mode: %s\n" "${MODE}" >&2
    usage
    ;;
esac

log "Done."
exit "${EX_OK}"
