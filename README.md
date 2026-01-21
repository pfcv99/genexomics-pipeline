
# genexomics-pipeline

## Overview
`genexomics-pipeline` is a Nextflow-based workflow that automates ingestion of sequencing run directories, uploads raw outputs to S3, creates Quilt packages that reference those S3 objects, and optionally attaches metadata from Benchling or Smartsheet.

This README assumes you will clone the repository and prefer Docker for production deployment. A LocalStack workflow is included for fast local testing.

---

## Core features (short)
- **Async S3 uploader**: high-performance uploads using `aioboto3`.
- **Quilt packaging**: versioned packages referencing S3 objects.
- **Metadata integrator**: attach Benchling or Smartsheet metadata to packages.
- **Local testing**: LocalStack-based test flow to avoid real AWS during development.

---

## Quick start (recommended — Docker)

1. **Clone**
```bash
git clone https://github.com/your-org/genexomics-pipeline.git
cd genexomics-pipeline
```

2. **Prepare environment**
```bash
cp .env.example .env
# Edit .env for your environment. For production set ENVIRONMENT=prod and provide real AWS credentials.
```

3. **Use the setup script (preferred)**
The repository provides `scripts/setup.sh`, which automates common tasks.

- Production (validate creds, optionally build image, create S3 bucket in AWS):
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh --mode prod --build-image
```

- Local test (LocalStack, create buckets, optional sample data and run pipeline):
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh --mode test --with-sample-data
# Optional flags:
#   --minimal-dataset  (use instead of --with-sample-data to download two small FASTQ files into tests/nas/)
#   --run-pipeline     (run Nextflow after setup)
```

`setup.sh` will:
- Build the pipeline image (unless skipped),
- Start LocalStack (test mode),
- Create the S3 bucket defined in `config/s3_config.yaml` inside LocalStack (test mode),
- Create the Quilt registry bucket (if `QUILT_REGISTRY` is set),
- Optionally generate sample data and run the pipeline.

**Important:** `setup.sh` uses `awslocal` inside the LocalStack container to create buckets — this avoids needing AWS credentials on the host when testing locally.

---

## Local test (LocalStack) — notes
- `.env.example` already contains suitable test values:
  ```
  ENVIRONMENT=test
  AWS_ACCESS_KEY_ID=test
  AWS_SECRET_ACCESS_KEY=test
  AWS_DEFAULT_REGION=us-east-1
  S3_ENDPOINT=http://localhost:4566
  QUILT_REGISTRY=s3://genexomics-quilt
  ```
- Start services:
  ```bash
  docker-compose up -d
  ```
- Run setup/test:
  ```bash
  ./scripts/setup.sh --mode test --with-sample-data
  ```
- If you need to create buckets manually from the host (less preferred):
  ```bash
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
  aws --endpoint-url=http://localhost:4566 s3 mb s3://genexomics-runs
  aws --endpoint-url=http://localhost:4566 s3 mb s3://genexomics-quilt
  ```

---

## Verify buckets and basic checks

- From host (LocalStack):
```bash
aws --endpoint-url=http://localhost:4566 s3 ls
aws --endpoint-url=http://localhost:4566 s3 ls s3://genexomics-runs
```

- From the pipeline container:
```bash
docker exec -it genexomics-pipeline bash
aws --endpoint-url=http://localstack:4566 s3 ls
```

- From LocalStack container (recommended for idempotent operations):
```bash
docker exec -it localstack-main awslocal s3 ls
docker exec -it localstack-main awslocal s3 ls s3://genexomics-runs
```

- Check LocalStack logs:
```bash
docker logs localstack-main --tail 200
```
Look for lines indicating bucket creation or readiness.

---

## Running the pipeline (Nextflow)

Run Nextflow from repository root (host or inside pipeline container):
```bash
nextflow run main.nf \
  --nas_dir /path/to/run_directories \
  --s3_config config/s3_config.yaml \
  --attach_metadata false
```

Important parameters:
- `--nas_dir` — folder containing `run_*` directories (sample default: `./tests/nas`)
- `--s3_config` — YAML with bucket/registry definitions
- `--attach_metadata` — `true` to call metadata integrator
- `--metadata_source` — `benchling` or `smartsheet`

---

## Configuration files (where to edit)
- `.env` — environment variables (copy from `.env.example` and edit)
- `config/s3_config.yaml` — S3 bucket names and Quilt registry:
```yaml
genexomics:
  buckets:
    default:
      Bucket: genexomics-runs
      Prefix: runs/
  quilt:
    namespace: genexomics
    registry: s3://genexomics-quilt
```
Adjust `Bucket` and `registry` for production.

---

## How `scripts/setup.sh` creates buckets (summary)
- **Test mode:** starts LocalStack (docker-compose), then uses `awslocal` inside the LocalStack container to create the bucket(s) defined in `config/s3_config.yaml` and the Quilt bucket (from `QUILT_REGISTRY`). This avoids requiring host AWS credentials.
- **Prod mode:** validates AWS credentials (`aws sts get-caller-identity`), then attempts to create the bucket in your AWS account using `aws s3api create-bucket`.

---

## Best practices
- Keep `.env.example` as template; never commit real credentials.
- Use Docker + `setup.sh` for reproducible setup and LocalStack for CI/local testing.
- For production, use IAM roles or secure credential management; avoid long-lived access keys in files.

---
