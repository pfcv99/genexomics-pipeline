#!/usr/bin/env bash
#===============================================================================
# generate_sample_data.sh
#
# Generate nf-core test/sample data using Nextflow inside a micromamba environment.
#
# This script is intentionally documented with detailed, well-structured comments
# (acting as "docstrings" for shell) in English and follows best-practice patterns:
#  - fail-fast settings: set -euo pipefail
#  - defensive argument parsing and validation
#  - small, single-purpose functions with clear responsibilities
#  - timestamped, human-friendly log messages
#  - helpful usage / examples
#
# Purpose
#   Run the nf-core/scrnaseq pipeline in 'test' mode to produce example/sample
#   input data for the genexomics pipeline. The pipeline run is expected to be
#   executed inside a pre-built micromamba environment that contains Nextflow.
#
# Behavior
#   - Ensures micromamba is present on PATH.
#   - Ensures Nextflow is available inside the requested micromamba env.
#   - Creates the requested output directory (parents) if missing.
#   - Optionally allows customization of Nextflow profile, pipeline name and
#     micromamba environment via CLI flags.
#
# Exit codes
#   0 - success
#   1 - usage / argument error
#   2 - missing dependency (micromamba, nextflow)
#   3 - runtime failure (nextflow returned non-zero)
#
# Examples
#   # Use defaults (profile=test,docker; pipeline=nf-core/scrnaseq; env=genexomics)
#   ./generate_sample_data.sh ./tests/nas/sample_data
#
#   # Custom profile / pipeline / env
#   ./generate_sample_data.sh -o ./tests/nas/sample_data -p "test,docker" -P "nf-core/scrnaseq" -e genexomics
#
#===============================================================================
set -euo pipefail
IFS=$'\n\t'

# -------------------------
# Default configuration
# -------------------------
DEFAULT_PROFILE="test,docker"
DEFAULT_PIPELINE="nf-core/scrnaseq"
DEFAULT_ENV="genexomics"

# Exit codes (constants)
EXIT_OK=0
EXIT_USAGE=1
EXIT_MISSING_DEP=2
EXIT_RUNTIME=3

# -------------------------
# Helpers (logging / errors)
# -------------------------
_log_timestamp() {
  # Return ISO 8601 UTC timestamp (seconds precision)
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_info() {
  # Print an informational message with timestamp to stdout
  printf '%s [INFO] %s\n' "$(_log_timestamp)" "$*"
}

log_warn() {
  printf '%s [WARN] %s\n' "$(_log_timestamp)" "$*"
}

log_error() {
  printf '%s [ERROR] %s\n' "$(_log_timestamp)" "$*"
}

die() {
  # Print error and exit with provided code (or runtime code)
  local code=${2:-$EXIT_RUNTIME}
  log_error "$1"
  exit "$code"
}

usage() {
  cat <<'USAGE' >&2
Usage:
  generate_sample_data.sh [-o OUTDIR] [-p PROFILE] [-P PIPELINE] [-e ENV] [--force]
  generate_sample_data.sh -h | --help

Generate nf-core sample data.

Options:
  -o, --outdir PATH       Output directory for generated sample data (required).
  -p, --profile PROFILE   Nextflow profile(s), comma-separated (default: test,docker).
  -P, --pipeline NAME     Nextflow pipeline identifier (default: nf-core/scrnaseq).
  -e, --env NAME          micromamba environment name containing Nextflow (default: genexomics).
  -f, --force             Overwrite existing output directory contents without prompt.
  -h, --help              Show this help and exit.

Examples:
  ./generate_sample_data.sh -o ./tests/nas/sample_data -p "test,docker" -P "nf-core/scrnaseq" -e genexomics --force
USAGE
  exit $EXIT_USAGE
}

# -------------------------
# Validation helpers
# -------------------------
_require_command() {
  # Ensure a command exists on PATH; exit with missing dependency code otherwise.
  if ! command -v "$1" >/dev/null 2>&1; then
    die "Required command not found in PATH: $1" $EXIT_MISSING_DEP
  fi
}

_check_nextflow_in_env() {
  # Verify Nextflow is available when executed via micromamba run -n ENV.
  # Returns 0 if found, non-zero otherwise.
  local envname="$1"
  # micromamba run -n <env> nextflow -version returns 0 if nextflow exists
  if ! micromamba run -n "${envname}" nextflow -version >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

# -------------------------
# Core operations
# -------------------------
generate_sample_data() {
  # Generate sample data by invoking Nextflow inside the requested micromamba env.
  #
  # Args:
  #   $1 - output directory path
  #   $2 - nextflow profile string (comma-separated)
  #   $3 - pipeline identifier (e.g., nf-core/scrnaseq)
  #   $4 - micromamba env name
  #   $5 - force boolean (true/false)
  local outdir="$1"
  local profile="$2"
  local pipeline="$3"
  local envname="$4"
  local force="${5:-false}"

  # Ensure micromamba binary is present
  _require_command micromamba

  # Ensure Nextflow exists inside the environment
  if ! _check_nextflow_in_env "${envname}"; then
    die "Nextflow is not available inside micromamba environment '${envname}'. Ensure Nextflow is installed in that env." $EXIT_MISSING_DEP
  fi

  # Create output directory (parents allowed). If exists and not forced, prompt user.
  if [[ -d "${outdir}" && "${force}" != "true" ]]; then
    log_warn "Output directory '${outdir}' already exists."
    printf "Contents may be overwritten. Re-run with --force to proceed without prompt.\n"
    exit $EXIT_USAGE
  fi

  mkdir -p "${outdir}"
  log_info "Running pipeline: pipeline='${pipeline}', profile='${profile}', output='${outdir}', env='${envname}'"

  # Execute Nextflow via micromamba inside the specified environment.
  # The micromamba 'run' wrapper ensures the correct environment is active.
  if micromamba run -n "${envname}" nextflow run "${pipeline}" -profile "${profile}" --outdir "${outdir}"; then
    log_info "Sample data generation succeeded. Output written to: ${outdir}"
    return 0
  else
    log_error "Nextflow pipeline execution failed for pipeline='${pipeline}'. See Nextflow logs for details."
    return $EXIT_RUNTIME
  fi
}

# -------------------------
# Parse CLI arguments
# -------------------------
OUTDIR=""
PROFILE="$DEFAULT_PROFILE"
PIPELINE="$DEFAULT_PIPELINE"
ENV_NAME="$DEFAULT_ENV"
FORCE="false"

# Support both short and long options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--outdir)
      shift
      OUTDIR="${1:-}"
      ;;
    -p|--profile)
      shift
      PROFILE="${1:-$DEFAULT_PROFILE}"
      ;;
    -P|--pipeline)
      shift
      PIPELINE="${1:-$DEFAULT_PIPELINE}"
      ;;
    -e|--env)
      shift
      ENV_NAME="${1:-$DEFAULT_ENV}"
      ;;
    -f|--force)
      FORCE="true"
      ;;
    -h|--help)
      usage
      ;;
    --) # end options
      shift
      break
      ;;
    -*)
      log_error "Unknown option: $1"
      usage
      ;;
    *)
      # Positional argument: treat first positional as OUTDIR (backwards compat)
      if [[ -z "${OUTDIR}" ]]; then
        OUTDIR="$1"
      else
        log_warn "Ignoring extra positional argument: $1"
      fi
      ;;
  esac
  shift
done

# Validate required OUTDIR
if [[ -z "${OUTDIR}" ]]; then
  log_error "Output directory is required."
  usage
fi

# Make OUTDIR absolute for clarity in logs
OUTDIR="$(mkdir -p "${OUTDIR}" && cd "${OUTDIR}" && pwd -P)"

# -------------------------
# Main
# -------------------------
log_info "=== generate_sample_data.sh ==="
log_info "Output directory : ${OUTDIR}"
log_info "Nextflow profile : ${PROFILE}"
log_info "Pipeline         : ${PIPELINE}"
log_info "Micromamba env   : ${ENV_NAME}"
log_info "Force overwrite  : ${FORCE}"

# Dependency checks
_require_command micromamba

# Attempt to run generator and capture return code
if ! generate_sample_data "${OUTDIR}" "${PROFILE}" "${PIPELINE}" "${ENV_NAME}" "${FORCE}"; then
  die "Sample data generation failed." $EXIT_RUNTIME
fi

log_info "✔ Sample data generated in ${OUTDIR}"
exit $EXIT_OK
