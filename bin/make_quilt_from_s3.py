#!/usr/bin/env python3
"""
make_quilt_from_s3.py

Create a Quilt package from existing S3 objects.

This script supports two basic modes of operation:
- Option A (default "list" mode): list objects under a given S3 bucket/prefix
  using boto3 and use those keys to construct a package.
- Option B ("stdin" mode): read newline-separated S3 object keys from STDIN.

The script creates a Quilt package whose entries point to s3://<bucket>/<key>
and pushes the package using Quilt's selector function that avoids re-copying
objects already available in the remote (i.e., use `selector_fn_copy_local`).

Example usage:
    python make_quilt_from_s3.py --bucket my-bucket --prefix nfcore/run123/ \
        --namespace myteam --registry s3://my-quilt-bucket --message "snapshot"

Notes:
- Docstrings and type hints are provided for easier maintainability and for
  programmatic introspection.
- Logging is delegated to the `shared_log` module; the script emits structured,
  informative messages at appropriate levels (INFO, DEBUG, WARNING, ERROR).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import boto3
import quilt3
import shared_log

logger = shared_log.logger  # keep local alias for convenience


def list_s3_keys(bucket: str, prefix: str = "", aws_kwargs: Optional[Dict] = None) -> List[str]:
    """
    List all object keys under a given S3 bucket and prefix.

    This function uses the boto3 S3 client paginator for list_objects_v2 to
    efficiently iterate through potentially large listings.

    Args:
        bucket: Name of the S3 bucket to list.
        prefix: Optional key prefix to filter the listing (default: "").
        aws_kwargs: Optional keyword arguments forwarded to boto3.client()
            (for example: region_name, aws_access_key_id, aws_secret_access_key).
            If None, the default boto3 configuration is used.

    Returns:
        A list of object keys (strings). If no objects are found, returns an
        empty list.

    Raises:
        botocore.exceptions.BotoCoreError / botocore.exceptions.ClientError:
            Propagates underlying boto3/botocore exceptions (these should be
            logged by the caller if desired).
    """
    aws_kwargs = aws_kwargs or {}
    s3 = boto3.client("s3", **aws_kwargs)
    paginator = s3.get_paginator("list_objects_v2")

    keys: List[str] = []
    logger.debug("Starting S3 listing: bucket=%s prefix=%s boto3_kwargs=%s", bucket, prefix, aws_kwargs)

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                logger.debug("S3 list_objects_v2 page contained no 'Contents' (empty page).")
                continue
            for obj in contents:
                key = obj.get("Key")
                if key:
                    keys.append(key)
                else:
                    # Unexpected shape - log at debug so we don't pollute INFO.
                    logger.debug("Encountered object without Key in page: %s", obj)
    except Exception:
        logger.error("Failed to list objects in s3://%s/%s", bucket, prefix, exc_info=True)
        raise

    logger.info("Listed %d objects from s3://%s/%s", len(keys), bucket, prefix)
    return keys


def make_package_from_keys(
    bucket: str,
    keys: Iterable[str],
    namespace: str,
    package_base: str = "from-s3",
    registry: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, str]:
    """
    Construct a Quilt package that references existing S3 objects and push it.

    The package entries will point to "s3://{bucket}/{key}". The function uses
    `quilt3.Package.selector_fn_copy_local` as the default selector function to
    avoid re-copying objects that are already hosted in the remote registry.

    Args:
        bucket: Name of source S3 bucket for the objects referenced by the package.
        keys: Iterable of object keys (relative to the bucket). Keys should be
            strings and will be used as the logical paths in the package unless
            transformed beforehand.
        namespace: Quilt namespace (team or username) where the package will be pushed.
        package_base: Base name for the package; a timestamp is appended to ensure uniqueness.
        registry: Optional remote registry URL where the package should be pushed
            (for example: "s3://my-quilt-bucket"). If provided, `quilt3.config`
            will be updated to use it as the default remote registry.
        message: Optional commit/push message.

    Returns:
        A dictionary with a single key "package" whose value is the full package name
        that was pushed (e.g., "myteam/from-s3-20250101T123456Z").

    Raises:
        ValueError: If `keys` is empty.
        Exception: Any exception from quilt3 when setting entries or pushing will be propagated
            after being logged.
    """
    # Materialize keys into a list (so we can check length and iterate multiple times).
    key_list = list(keys)
    if not key_list:
        logger.warning("No keys provided to make_package_from_keys(bucket=%s). Aborting package creation.", bucket)
        raise ValueError("No S3 keys provided to create a package.")

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    packagename = f"{package_base}-{timestamp}"
    full_name = f"{namespace}/{packagename}"
    logger.info("Preparing new package: %s (num_objects=%d)", full_name, len(key_list))

    p = quilt3.Package()

    try:
        for key in key_list:
            # By default use the key as the logical path inside the package.
            # If desired, this is the place to normalize, strip prefixes, or
            # otherwise map S3 keys to package logical paths.
            logical_path = key
            s3_url = f"s3://{bucket}/{key}"
            logger.debug("Adding package entry: logical_path=%s -> %s", logical_path, s3_url)
            # quilt3.Package.set returns a new Package or modifies in-place depending on API;
            # using assignment keeps the code robust to either behavior.
            p = p.set(logical_path, s3_url)

        # Set metadata to help with provenance and debugging.
        p = p.set_meta({
            "created_at": datetime.utcnow().isoformat() + "Z",
            "source_bucket": bucket,
            "num_objects": len(key_list),
            "package_base": package_base,
        })

        # Use selector function to avoid re-upload when objects are already in the remote.
        selector = quilt3.Package.selector_fn_copy_local

        if registry:
            logger.info("Configuring default remote registry: %s", registry)
            try:
                quilt3.config(default_remote_registry=registry)
            except Exception:
                logger.error("Failed to configure quilt3 default_remote_registry=%s", registry, exc_info=True)
                raise

        push_message = message or f"Created from existing S3 objects ({len(key_list)} entries)"
        logger.info("Pushing package %s to registry=%s message=%s", full_name, registry, push_message)
        p.push(full_name, registry=registry, message=push_message, selector_fn=selector)

    except Exception:
        logger.error("Failed to create or push package %s", full_name, exc_info=True)
        raise

    logger.info("Successfully created and pushed package: %s", full_name)
    return {"package": full_name}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.

    Args:
        argv: Optional list of arguments (defaults to sys.argv[1:] when None).
    Returns:
        The populated argparse Namespace.
    """
    parser = argparse.ArgumentParser(
        description="Create a Quilt package that references existing S3 objects and push it to a registry."
    )

    parser.add_argument("--bucket", required=True, help="S3 bucket name containing the objects.")
    parser.add_argument("--prefix", default="", help="Prefix to filter S3 objects (e.g. nfcore/scrnaseq/run123/).")
    parser.add_argument("--namespace", required=True, help="Quilt namespace (team or username) to push the package into.")
    parser.add_argument("--registry", help="Optional remote registry (e.g. s3://my-quilt-bucket).")
    parser.add_argument("--message", help="Optional push message / commit message.")
    parser.add_argument("--mode", choices=["list", "stdin"], default="list",
                        help="Mode for obtaining keys: 'list' to list keys from S3; 'stdin' to read newline-separated keys from stdin.")
    parser.add_argument("--package-base", default="from-s3", help="Base name for the generated package (timestamp appended).")
    # Allow AWS-related kwargs to be passed through environment in boto3 default chain; keep CLI surface small.
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point.

    Returns:
        Exit code: 0 on success, non-zero on failure.
    """
    args = parse_args(argv)
    logger.debug("Command-line arguments: %s", args)

    try:
        if args.mode == "list":
            logger.info("Listing keys from s3://%s/%s", args.bucket, args.prefix)
            keys = list_s3_keys(args.bucket, args.prefix)
        else:
            logger.info("Reading keys from stdin (mode=stdin).")
            keys = [line.strip() for line in sys.stdin if line.strip()]

        logger.info("Found %d object(s) to package.", len(keys))

        if not keys:
            logger.warning("No objects discovered. Nothing to package. Exiting with code 2.")
            return 2

        result = make_package_from_keys(
            bucket=args.bucket,
            keys=keys,
            namespace=args.namespace,
            package_base=args.package_base,
            registry=args.registry,
            message=args.message,
        )

        logger.info("Created package: %s", result["package"])
        return 0

    except ValueError as ve:
        logger.error("Input validation error: %s", ve, exc_info=False)
        return 3
    except Exception:
        # Generic catch-all to ensure non-zero exit on unexpected failures.
        logger.error("Unhandled error while creating package.", exc_info=True)
        return 1


if __name__ == "__main__":
    # Run main and exit with returned code to support shell-level checks.
    raise SystemExit(main())
