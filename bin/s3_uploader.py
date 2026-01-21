#!/usr/bin/env python3
"""
async_s3_uploader.py

Asynchronous S3 Uploader.

This module provides an asynchronous uploader that reads AWS credentials
and bucket/prefix details from a YAML configuration file and uploads a
local file or directory to a chosen S3 bucket using aioboto3.

Features and best-practice decisions:
- Detailed, structured docstrings (Google style) for maintainability and
  automatic documentation generation.
- Type annotations for clarity and static analysis.
- Config-driven AWS credentials and target buckets (YAML section -> buckets).
- Multipart upload optimized for large files (TransferConfig).
- Server-side checksum request using CRC32C and botocore checksum validation enabled.
- Clear, contextual logging using a project `shared_log` module; logs include
  structured context via the `extra` dict where helpful.
- Clear exit codes:
    0 - success
    1 - upload failure / runtime error
    2 - configuration error (missing file, invalid YAML, missing bucket key)
    3 - invalid input path (file/directory not found)

Usage:
    python async_s3_uploader.py -i /path/to/file -c config.yaml -s genexomics -b raw_uploads
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

import aioboto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from botocore.config import Config
import yaml

import shared_log

# Constants
GB = 1024 ** 3

# Transfer configuration tuned for large / high-bandwidth objects.
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=5 * GB,
    multipart_chunksize=5 * GB,
    max_concurrency=10,
    use_threads=True
)


# ----------------------------
# Helpers
# ----------------------------
def _now_iso() -> str:
    """Return current UTC timestamp in ISO format with 'Z' suffix."""
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def human_size_bytes(n: int) -> str:
    """
    Convert bytes to a human-friendly string (GB with 2 decimal places if large).

    Args:
        n: Number of bytes.

    Returns:
        Human-readable size string.
    """
    if n >= GB:
        return f"{n / GB:.2f} GB"
    if n >= 1024 ** 2:
        return f"{n / (1024 ** 2):.2f} MB"
    if n >= 1024:
        return f"{n / 1024:.2f} KB"
    return f"{n} B"


def _normalize_prefix(prefix: Optional[str]) -> str:
    """
    Normalize an S3 prefix: remove leading/trailing slashes and return empty string when None.
    """
    if not prefix:
        return ""
    return str(prefix).lstrip("/").rstrip("/")


def _make_s3_key(prefix: str, relative_path: str) -> str:
    """
    Build an S3 object key using POSIX separators and an optional prefix.

    Args:
        prefix: normalized prefix (no leading/trailing '/').
        relative_path: relative path (posix or platform path) to place under prefix.

    Returns:
        S3 object key string.
    """
    rp = relative_path.replace("\\", "/").lstrip("/")
    return f"{prefix}/{rp}" if prefix else rp


# ----------------------------
# Configuration loader
# ----------------------------
class S3BucketConfig:
    """
    Load AWS and bucket configuration from a YAML file.

    The YAML is expected to have the following shape:

    genexomics:
      config:
        aws_access_key_id: ...
        aws_secret_access_key: ...
        aws_session_token: ...
        region_name: ...
      buckets:
        raw_uploads:
          Bucket: my-bucket-name
          Prefix: some/prefix/

    Attributes:
        config (dict): AWS credential and region keys (may be empty).
        buckets (Dict[str, object]): mapping of bucket_key -> lightweight object with
            attributes 'Bucket' (str) and 'Prefix' (str).
    """

    def __init__(self, yaml_file: str, section: str = "genexomics"):
        """
        Initialize and validate the YAML configuration.

        Args:
            yaml_file: Path to YAML file.
            section: Top-level section name to read (default 'genexomics').

        Raises:
            FileNotFoundError: If yaml_file does not exist.
            ValueError: If the YAML structure is invalid or required keys are missing.
        """
        yaml_path = Path(yaml_file) if yaml_file else None
        if yaml_path is None or not yaml_path.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")

        with yaml_path.open("r") as fh:
            raw = yaml.load(fh, Loader=yaml.FullLoader)

        if not isinstance(raw, dict):
            raise ValueError("Invalid YAML format: top-level mapping/dictionary expected.")

        if section not in raw:
            raise ValueError(f"Section '{section}' not found in YAML. Available sections: {sorted(raw.keys())}")

        section_data = raw[section]
        if not isinstance(section_data, dict):
            raise ValueError(f"Invalid section '{section}' format: expected mapping/dictionary.")

        cfg = section_data.get("config", {}) or {}
        if not isinstance(cfg, dict):
            raise ValueError("'config' must be a mapping/dictionary.")

        raw_buckets = section_data.get("buckets", {}) or {}
        if not isinstance(raw_buckets, dict):
            raise ValueError("'buckets' must be a mapping/dictionary.")

        self.config: Dict[str, Any] = dict(cfg)
        self.buckets: Dict[str, Any] = {}

        for key, bucket_cfg in raw_buckets.items():
            if not isinstance(bucket_cfg, dict) or "Bucket" not in bucket_cfg:
                raise ValueError(f"Invalid bucket '{key}' config; each bucket must contain a 'Bucket' key.")
            # ensure Prefix exists and is normalized
            prefix = bucket_cfg.get("Prefix", "") or ""
            # create a simple object with attributes Bucket and Prefix
            self.buckets[key] = type("BucketObj", (), {
                "Bucket": bucket_cfg["Bucket"],
                "Prefix": _normalize_prefix(prefix)
            })()

    def get_bucket(self, key: str):
        """
        Return the bucket object for a named key.

        Args:
            key: Named bucket key to lookup.

        Returns:
            Lightweight object with attributes 'Bucket' and 'Prefix'.

        Raises:
            KeyError: If the key is not present in configuration.
        """
        if key not in self.buckets:
            raise KeyError(f"Bucket key '{key}' not found. Available keys: {sorted(self.buckets.keys())}")
        return self.buckets[key]

    def get_aws_kwargs(self) -> Dict[str, Any]:
        """
        Return kwargs suitable for aioboto3.Session(**kwargs).

        Supported keys: aws_access_key_id, aws_secret_access_key, aws_session_token, region_name

        Returns:
            Mapping of AWS kwargs (may be empty).
        """
        allowed = ("aws_access_key_id", "aws_secret_access_key", "aws_session_token", "region_name")
        return {k: v for k, v in self.config.items() if k in allowed and v is not None}


# ----------------------------
# Logging helpers
# ----------------------------
def setup_logging_for_run(input_path: str, log_dir: Optional[str], basename_prefix: str = "upload") -> str:
    """
    Configure shared_log and open a run header.

    This function uses the project's shared_log API (assumed to expose Logging, log_header).

    Args:
        input_path: path to the input file/directory being uploaded.
        log_dir: optional log directory; if None uses ./logs.
        basename_prefix: prefix for log filename.

    Returns:
        header token/identifier returned by shared_log.log_header (opaque to caller).

    Notes:
        This function intentionally keeps coupling to shared_log minimal: it calls
        shared_log.Logging(...) and shared_log.log_header(metadata) like your original code.
    """
    log_folder = Path(log_dir) if log_dir else Path.cwd() / "logs"
    clean_name = re.sub(r"[^A-Za-z0-9_]+", "_", Path(input_path).stem)

    # Initialize the shared logger object and console handler.
    # shared_log.Logging is left as in your original codebase: it typically
    # configures log file basename, folder and console toggles.
    shared_log.Logging(basename=f"{basename_prefix}_{clean_name}", foldername=str(log_folder), console=True)

    # Compose run metadata and call log header helper.
    try:
        size_bytes = os.path.getsize(input_path) if os.path.exists(input_path) else None
    except Exception:
        size_bytes = None

    metadata = {
        "Start_Time": _now_iso(),
        "Source_File": input_path,
        "File_Size": human_size_bytes(size_bytes) if size_bytes is not None else "N/A",
    }
    return shared_log.log_header(metadata)


# ----------------------------
# Core upload logic
# ----------------------------
async def upload_path_to_s3(
    path: str,
    bucket: str,
    prefix: Optional[str] = None,
    aws_kwargs: Optional[Dict[str, Any]] = None
) -> List[str]:
    """
    Upload a file or directory to S3 asynchronously using aioboto3.

    Args:
        path: Local path to a file or directory to upload.
        bucket: S3 bucket name (string).
        prefix: Optional S3 prefix under which to place uploaded object keys.
        aws_kwargs: Optional kwargs forwarded to aioboto3.Session(...) (credentials, region).

    Returns:
        List of uploaded S3 object keys (relative keys under the bucket).

    Raises:
        ValueError: If path does not exist or is not a file/directory.
        botocore.exceptions.ClientError: For AWS client errors.
        Exception: For other unexpected errors.
    """
    aws_kwargs = aws_kwargs or {}
    session = aioboto3.Session(**aws_kwargs)
    prefix_clean = _normalize_prefix(prefix)

    # Additional botocore config enabling checksum validation on the client side.
    client_config = Config(signature_version="s3v4", s3={"checksum_validation": "ENABLED"})
    extra_args = {"ChecksumAlgorithm": "CRC32C"}

    uploaded_keys: List[str] = []

    # Use asynchronous context manager for the client
    async with session.client("s3", region_name=aws_kwargs.get("region_name"), config=client_config) as s3_client:
        if os.path.isfile(path):
            object_key = _make_s3_key(prefix_clean, os.path.basename(path))
            shared_log.logger.info("Uploading file", extra={"local_path": path, "bucket": bucket, "object_key": object_key})
            await s3_client.upload_file(path, bucket, object_key, Config=S3_TRANSFER_CONFIG, ExtraArgs=extra_args)
            uploaded_keys.append(object_key)
            shared_log.logger.debug("Upload completed for file", extra={"object_key": object_key})
            return uploaded_keys

        if os.path.isdir(path):
            # Walk directory and upload files preserving relative structure
            for root, _, files in os.walk(path):
                for fname in files:
                    local = os.path.join(root, fname)
                    rel = os.path.relpath(local, path)
                    object_key = _make_s3_key(prefix_clean, rel)
                    shared_log.logger.debug("Uploading file from directory", extra={"local_path": local, "object_key": object_key})
                    await s3_client.upload_file(local, bucket, object_key, Config=S3_TRANSFER_CONFIG, ExtraArgs=extra_args)
                    uploaded_keys.append(object_key)
            shared_log.logger.info("Directory upload complete", extra={"local_path": path, "num_objects": len(uploaded_keys)})
            return uploaded_keys

        raise ValueError(f"Path does not exist or is not a file/directory: {path}")


# ----------------------------
# CLI / orchestration
# ----------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    """
    Construct and return the argparse.ArgumentParser for the CLI.

    Returns:
        Configured ArgumentParser instance.
    """
    p = argparse.ArgumentParser(description="Asynchronous S3 uploader that reads YAML config for buckets and AWS credentials.")
    p.add_argument("-i", "--input", required=True, help="Path to local file or directory to upload.")
    p.add_argument("-c", "--config", required=True, help="YAML config file containing AWS and bucket definitions.")
    p.add_argument("-s", "--section", default="genexomics", help="Top-level YAML section to read (default: 'genexomics').")
    p.add_argument("-b", "--bucket-key", required=True, help="Named bucket key within the YAML under section.buckets.")
    p.add_argument("-l", "--log-dir", help="Directory in which to store logs (defaults to ./logs).")
    return p


async def _run(argv: Optional[List[str]] = None) -> int:
    """
    Internal coroutine that implements the CLI logic.

    Args:
        argv: Optional list of CLI args (defaults to sys.argv[1:]).

    Returns:
        Exit code to be returned by the program.
    """
    parser = build_arg_parser()
    parsed = parser.parse_args(argv)

    # Load YAML config
    try:
        cfg = S3BucketConfig(parsed.config, section=parsed.section)
    except FileNotFoundError as exc:
        # Logging before we initialize project logs (create minimal console logger)
        shared_log.Logging(basename="upload_config_error", foldername=None, console=True)
        shared_log.logger.error("Configuration file not found", exc_info=True, extra={"config": parsed.config})
        return 2
    except Exception as exc:
        shared_log.Logging(basename="upload_config_error", foldername=None, console=True)
        shared_log.logger.error("Failed to load/validate YAML config", exc_info=True, extra={"config": parsed.config})
        return 2

    # Resolve named bucket
    try:
        bucket_obj = cfg.get_bucket(parsed.bucket_key)
    except KeyError as exc:
        shared_log.Logging(basename="upload_config_error", foldername=None, console=True)
        shared_log.logger.error("Invalid bucket key", exc_info=False, extra={"bucket_key": parsed.bucket_key, "available": list(cfg.buckets.keys())})
        return 2

    # Start structured logging for the run
    header_token = setup_logging_for_run(parsed.input, parsed.log_dir, basename_prefix="async_upload")

    # Validate input path early
    if not os.path.exists(parsed.input):
        shared_log.logger.error("Input path not found", extra={"input": parsed.input})
        shared_log.log_footer(header_token, success=False, error_message="Path Not Found")
        return 3

    # Prepare AWS kwargs
    aws_kwargs = cfg.get_aws_kwargs()

    # Attempt the upload
    uploaded_keys: List[str] = []
    try:
        shared_log.logger.info("Starting upload", extra={
            "input": parsed.input,
            "bucket": bucket_obj.Bucket,
            "prefix": bucket_obj.Prefix,
            "section": parsed.section
        })
        uploaded_keys = await upload_path_to_s3(parsed.input, bucket_obj.Bucket, bucket_obj.Prefix, aws_kwargs=aws_kwargs)
        shared_log.logger.info("Upload successful", extra={"num_objects": len(uploaded_keys)})
        for key in uploaded_keys:
            shared_log.logger.info("Uploaded object", extra={"object_key": key})
        shared_log.log_footer(header_token, success=True, objects_uploaded=len(uploaded_keys))
        return 0

    except ClientError as exc:
        shared_log.logger.error("AWS client error during upload", exc_info=True, extra={"bucket": bucket_obj.Bucket})
        shared_log.log_footer(header_token, success=False, error_message=str(exc))
        return 1

    except ValueError as exc:
        shared_log.logger.error("Invalid input or runtime error", exc_info=True, extra={"error": str(exc)})
        shared_log.log_footer(header_token, success=False, error_message=str(exc))
        return 3

    except Exception as exc:
        shared_log.logger.error("Unexpected failure during upload", exc_info=True, extra={"error": str(exc)})
        shared_log.log_footer(header_token, success=False, error_message=str(exc))
        return 1


def main(argv: Optional[List[str]] = None) -> int:
    """
    Synchronous entrypoint wrapper for the CLI that runs the async workflow.

    Args:
        argv: Optional argv list for testing or programmatic invocation.

    Returns:
        Exit code integer.
    """
    try:
        return asyncio.run(_run(argv))
    except KeyboardInterrupt:
        shared_log.logger.warning("Upload interrupted by user (KeyboardInterrupt).", extra={})
        return 1
    except Exception:
        # Safety net — although _run already handles most errors, guard the top-level call.
        shared_log.logger.exception("Fatal error in main", exc_info=True, extra={})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
