#!/usr/bin/env python3
"""
metadata_integrator.py

Attach metadata from Benchling or Smartsheet to an existing Quilt package
(stored in an S3-backed Quilt registry). The tool does not copy or move data —
it only updates package metadata and pushes the package back to the registry.

Design goals and behavior:
- Single shared CLI for common args, with subcommands for source-specific options.
- Minimal, well-documented clients for Benchling and Smartsheet sufficient for
  the operations required by this tool.
- Metadata is merged at the package top-level; existing unrelated keys are preserved.
- Provenance is maintained/extended under the 'provenance' key.
- Uses `shared_log.logger` for all logging with clear, contextual messages.
- Dry-run support that shows the merged metadata without pushing.

Examples:
    # Benchling
    python metadata_integrator.py \
      --package myorg/mypkg \
      --registry s3://quilt-bucket \
      benchling \
      --benchling-entity-id BE-abc123 \
      --benchling-api-key "$BENCHLING_API_KEY"

    # Smartsheet (row id)
    python metadata_integrator.py \
      --package myorg/mypkg \
      --registry s3://quilt-bucket \
      smartsheet \
      --smartsheet-sheet-id 123456789 \
      --smartsheet-row-id 987654321 \
      --smartsheet-token "$SMARTSHEET_TOKEN"

    # Smartsheet (scan by run column)
    python metadata_integrator.py \
      --package myorg/mypkg \
      --registry s3://quilt-bucket \
      smartsheet \
      --smartsheet-sheet-id 123456789 \
      --smartsheet-run-column "Run ID" \
      --run-id R0001 \
      --smartsheet-token "$SMARTSHEET_TOKEN"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import quilt3
import requests

import shared_log

# Local logger alias (uses the project's shared logging configuration).
logger = shared_log.logger


def now_iso_z() -> str:
    """
    Return current UTC time as an ISO 8601 string ending with 'Z'.

    Returns:
        A string in the format 'YYYY-MM-DDTHH:MM:SS.ssssssZ'.
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class BenchlingClient:
    """
    Minimal Benchling client to fetch a custom entity.

    This client supports two header styles:
      - 'bearer' (default): Authorization: Bearer <api_key>
      - 'x-api-key': X-API-Key: <api_key>

    Usage:
        client = BenchlingClient(api_key="...", header_type="bearer", base_url="https://api.benchling.com/v2")
        entity = client.get_entity("BE-abc123")

    Args:
        api_key: Benchling API key or token.
        header_type: 'bearer' or 'x-api-key'. Case-insensitive.
        base_url: Base API URL (default is the Benchling v2 API).

    Raises:
        ValueError: If api_key is not provided.

    Methods:
        get_entity(entity_id, timeout): Fetches and normalizes the Benchling custom entity.
    """

    def __init__(self, api_key: str, header_type: str = "bearer", base_url: str = "https://api.benchling.com/v2"):
        if not api_key:
            raise ValueError("Benchling API key required")
        self.api_key = api_key
        self.header_type = header_type.lower()
        self.base_url = base_url.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        """
        Build the authentication header mapping for Benchling requests.

        Returns:
            A dict of HTTP headers used for Benchling API calls.
        """
        if self.header_type == "x-api-key":
            return {"X-API-Key": self.api_key}
        return {"Authorization": f"Bearer {self.api_key}"}

    def get_entity(self, entity_id: str, timeout: int = 30) -> Dict[str, Any]:
        """
        Fetch a Benchling custom entity and return a normalized dictionary.

        The function is defensive: it examines common layout variants (customFields,
        custom_fields, custom_fields_map) and normalizes them under the 'fields' key.

        Args:
            entity_id: Benchling custom entity identifier.
            timeout: HTTP request timeout in seconds.

        Returns:
            A dictionary with keys:
                - entity_id: returned id or requested id
                - name: entity name when available
                - schema_id: schema id if present
                - fields: normalized mapping of custom fields -> values
                - url: web URL to the entity if present
                - not_found: True if the API returned 404 (in that case other keys may be absent)

        Raises:
            requests.HTTPError: If the request fails with a non-404 status.
            requests.RequestException: For network-related errors.
        """
        url = f"{self.base_url}/custom-entities/{entity_id}"
        logger.debug("Fetching Benchling entity: url=%s", url)
        try:
            resp = requests.get(url, headers=self._headers(), timeout=timeout)
        except requests.RequestException:
            logger.exception("Network error when requesting Benchling entity entity_id=%s", entity_id)
            raise

        if resp.status_code == 404:
            logger.info("Benchling entity not found: entity_id=%s url=%s", entity_id, url)
            return {"entity_id": entity_id, "not_found": True}

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            logger.exception("Benchling API returned error for entity_id=%s status=%s", entity_id, resp.status_code)
            raise

        payload = resp.json()
        fields: Dict[str, Any] = {}

        # Normalise tenant-specific shapes for custom fields
        for cand in ("customFields", "custom_fields", "custom_fields_map"):
            cf = payload.get(cand)
            if isinstance(cf, dict):
                for k, v in cf.items():
                    # Benchling sometimes nests value under {"value": ...}
                    if isinstance(v, dict) and "value" in v:
                        fields[k] = v["value"]
                    else:
                        fields[k] = v
                break

        entity = {
            "entity_id": payload.get("id") or entity_id,
            "name": payload.get("name"),
            "schema_id": (payload.get("schema") or {}).get("id"),
            "fields": fields,
            "url": payload.get("webUrl") or payload.get("web_url"),
        }
        logger.debug("Benchling entity fetched: entity_id=%s normalized_fields=%d", entity_id, len(fields))
        return entity


class SmartsheetClient:
    """
    Minimal Smartsheet client for fetching sheet rows.

    Provides:
      - get_row_by_rowid: fetch a specific row by its row id (fast).
      - get_row_by_run_column: fetch the sheet and scan a named column to find a row
        whose cell equals the provided run id.

    Args:
        token: Smartsheet API token.

    Raises:
        ValueError: If token is not provided.
    """

    def __init__(self, token: str):
        if not token:
            raise ValueError("Smartsheet token required")
        self.token = token
        self.base = "https://api.smartsheet.com/2.0"
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def get_row_by_rowid(self, sheet_id: str, row_id: str, timeout: int = 30) -> Dict[str, Any]:
        """
        Fetch a row by its row id and include column titles to produce a mapping.

        Args:
            sheet_id: Smartsheet sheet id.
            row_id: Row id to fetch.
            timeout: HTTP timeout in seconds.

        Returns:
            A dict: {"sheet_id": sheet_id, "row": {column_title: value, ..., "_rowId": row_id}}

        Raises:
            requests.RequestException / requests.HTTPError: On request failure.
        """
        row_url = f"{self.base}/sheets/{sheet_id}/rows/{row_id}"
        logger.debug("Fetching Smartsheet row by id: sheet_id=%s row_id=%s", sheet_id, row_id)
        try:
            resp = requests.get(row_url, headers=self.headers, timeout=timeout)
            resp.raise_for_status()
            row = resp.json()
        except requests.RequestException:
            logger.exception("Failed to fetch Smartsheet row sheet_id=%s row_id=%s", sheet_id, row_id)
            raise

        # Fetch sheet columns to map columnId -> title
        sheet_url = f"{self.base}/sheets/{sheet_id}?include=columns"
        logger.debug("Fetching Smartsheet sheet columns for mapping: sheet_id=%s", sheet_id)
        try:
            sresp = requests.get(sheet_url, headers=self.headers, timeout=timeout)
            sresp.raise_for_status()
            sheet = sresp.json()
        except requests.RequestException:
            logger.exception("Failed to fetch Smartsheet sheet metadata sheet_id=%s", sheet_id)
            raise

        col_map = {c["id"]: c["title"] for c in sheet.get("columns", [])}
        mapped: Dict[str, Any] = {}
        for cell in row.get("cells", []):
            mapped[col_map.get(cell.get("columnId"), str(cell.get("columnId")))] = cell.get("value")
        mapped["_rowId"] = row.get("id")
        logger.debug("Smartsheet row mapped: sheet_id=%s row_id=%s mapped_columns=%d", sheet_id, row_id, len(mapped))
        return {"sheet_id": sheet_id, "row": mapped}

    def get_row_by_run_column(self, sheet_id: str, run_column: str, run_id: str, timeout: int = 60) -> Dict[str, Any]:
        """
        Scan the sheet for the row where the given run_column equals run_id.

        This performs a sheet-level fetch and iterates rows; it is suitable for
        sheets that are small-to-moderate in size.

        Args:
            sheet_id: Smartsheet sheet id.
            run_column: The column title to inspect (case-sensitive as returned by the API).
            run_id: Value to search for inside run_column.
            timeout: HTTP timeout in seconds.

        Returns:
            A dict {"sheet_id": sheet_id, "row": mapped_row} or {"sheet_id": sheet_id, "row": None}
            if no matching row is found. If the column cannot be found, returns
            {"sheet_id": sheet_id, "row": None, "error": "column_not_found"}.

        Raises:
            requests.RequestException / requests.HTTPError: On request failure.
        """
        sheet_url = f"{self.base}/sheets/{sheet_id}"
        logger.debug("Fetching Smartsheet sheet for scanning: sheet_id=%s", sheet_id)
        try:
            resp = requests.get(sheet_url, headers=self.headers, timeout=timeout)
            resp.raise_for_status()
            sheet = resp.json()
        except requests.RequestException:
            logger.exception("Failed to fetch Smartsheet sheet sheet_id=%s", sheet_id)
            raise

        columns = sheet.get("columns", [])
        title_to_id = {c["title"]: c["id"] for c in columns}
        if run_column not in title_to_id:
            logger.warning("Run column not found in sheet: sheet_id=%s requested_column=%s", sheet_id, run_column)
            return {"sheet_id": sheet_id, "row": None, "error": "column_not_found"}

        run_col_id = title_to_id[run_column]
        id_to_title = {c["id"]: c["title"] for c in columns}

        for row in sheet.get("rows", []):
            for cell in row.get("cells", []):
                if cell.get("columnId") == run_col_id and cell.get("value") == run_id:
                    mapped: Dict[str, Any] = {}
                    for c in row.get("cells", []):
                        mapped[id_to_title.get(c.get("columnId"), str(c.get("columnId")))] = c.get("value")
                    mapped["_rowId"] = row.get("id")
                    logger.debug("Found matching row in sheet: sheet_id=%s run_column=%s run_id=%s row_id=%s",
                                 sheet_id, run_column, run_id, mapped["_rowId"])
                    return {"sheet_id": sheet_id, "row": mapped}
        logger.info("No matching row in sheet: sheet_id=%s run_column=%s run_id=%s", sheet_id, run_column, run_id)
        return {"sheet_id": sheet_id, "row": None}


class MetadataIntegrator:
    """
    Integrate metadata into an existing Quilt package and push the update.

    Merge strategy:
      - Preserve existing package metadata keys except 'benchling' and 'smartsheet',
        which will be set to the provided metadata (overwrite).
      - Update or add a 'provenance' object with integration details.

    Args:
        package: Quilt package identifier (namespace/name).
        registry: Quilt registry URI (e.g. s3://quilt-bucket).
        top_hash: Optional top_hash to base update on; forwarded to browse().

    Methods:
        load(): loads the package object (sets self.pkg).
        merge_meta(benchling_meta, smartsheet_meta): returns merged metadata dict.
        attach_and_push(...): applies metadata and pushes the package (unless dry_run).
    """

    def __init__(self, package: str, registry: str, top_hash: Optional[str] = None):
        self.package = package
        self.registry = registry
        self.top_hash = top_hash
        self.pkg: Optional[quilt3.Package] = None

    def load(self) -> None:
        """
        Browse and load the package into memory (assigns to self.pkg).

        Raises:
            Exception: If quilt3.Package.browse fails (network/registry issues or package not found).
        """
        logger.info("Loading package for update: package=%s registry=%s top_hash=%s", self.package, self.registry, self.top_hash)
        try:
            self.pkg = quilt3.Package.browse(self.package, registry=self.registry, top_hash=self.top_hash)
        except Exception:
            logger.exception("Failed to load package: package=%s registry=%s", self.package, self.registry)
            raise

    def merge_meta(self, benchling_meta: Optional[Dict[str, Any]] = None,
                   smartsheet_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Merge provided source metadata into the package's existing metadata.

        Args:
            benchling_meta: Benchling metadata mapping or None.
            smartsheet_meta: Smartsheet metadata mapping or None.

        Returns:
            The merged metadata dictionary ready to be set on the Quilt package.

        Raises:
            Exception: If loading the package fails.
        """
        if self.pkg is None:
            logger.debug("Package not loaded yet; calling load() before merge.")
            self.load()

        existing = getattr(self.pkg, "meta", None) or {}
        logger.debug("Existing metadata keys before merge: %s", list(existing.keys()) if isinstance(existing, dict) else type(existing))

        merged = dict(existing) if isinstance(existing, dict) else {}

        if benchling_meta is not None:
            logger.info("Merging benchling metadata into package: package=%s benchling_entity=%s",
                        self.package, benchling_meta.get("entity_id") if isinstance(benchling_meta, dict) else None)
            merged["benchling"] = benchling_meta

        if smartsheet_meta is not None:
            logger.info("Merging smartsheet metadata into package: package=%s smartsheet_sheet=%s",
                        self.package, smartsheet_meta.get("sheet_id") if isinstance(smartsheet_meta, dict) else None)
            merged["smartsheet"] = smartsheet_meta

        # Build or extend provenance info
        prov = merged.get("provenance")
        prov = prov if isinstance(prov, dict) else {}
        prov_entry = {
            "integrated_at": now_iso_z(),
            "integrator": "metadata_integrator.py",
            "sources": []
        }
        if benchling_meta:
            prov_entry["sources"].append("benchling")
        if smartsheet_meta:
            prov_entry["sources"].append("smartsheet")

        # Update provenance while preserving other provenance fields if present
        prov.update(prov_entry)
        merged["provenance"] = prov

        logger.debug("Merged metadata keys: %s", list(merged.keys()))
        return merged

    def attach_and_push(self, benchling_meta: Optional[Dict[str, Any]] = None,
                        smartsheet_meta: Optional[Dict[str, Any]] = None,
                        message: str = "Attach metadata",
                        dry_run: bool = False) -> Dict[str, Any]:
        """
        Apply merged metadata to the package and push it to the registry.

        Args:
            benchling_meta: Benchling metadata mapping or None.
            smartsheet_meta: Smartsheet metadata mapping or None.
            message: Push/commit message.
            dry_run: If True, do not modify or push the package; return the merged metadata.

        Returns:
            A dictionary describing the result. If dry_run: includes 'merged_meta'.
            Otherwise includes 'package' and 'new_hash' (returned from push).

        Raises:
            Exception: If push or set_meta fails (unless dry_run=True).
        """
        merged = self.merge_meta(benchling_meta=benchling_meta, smartsheet_meta=smartsheet_meta)
        logger.debug("Prepared merged metadata for package=%s dry_run=%s", self.package, dry_run)

        if dry_run:
            logger.info("Dry-run enabled; not pushing changes. package=%s", self.package)
            return {"package": self.package, "dry_run": True, "merged_meta": merged}

        if self.pkg is None:
            logger.debug("Package object missing; loading prior to set_meta/push.")
            self.load()

        try:
            logger.info("Setting metadata on package: package=%s meta_keys=%s", self.package, list(merged.keys()))
            self.pkg.set_meta(merged)
        except Exception:
            logger.exception("Failed to set metadata on package=%s", self.package)
            raise

        try:
            logger.info("Pushing package to registry: package=%s registry=%s message=%s", self.package, self.registry, message)
            new_hash = self.pkg.push(self.package, registry=self.registry, message=message)
            logger.info("Package pushed successfully: package=%s new_hash=%s", self.package, new_hash)
        except Exception:
            logger.exception("Failed to push package=%s to registry=%s", self.package, self.registry)
            raise

        return {"package": self.package, "new_hash": new_hash, "meta_keys": list(merged.keys())}


def build_parser() -> argparse.ArgumentParser:
    """
    Build the command-line argument parser for the tool.

    Returns:
        An argparse.ArgumentParser configured with shared arguments and subparsers
        for 'benchling' and 'smartsheet' sources.
    """
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--package", required=True, help="Quilt package name (namespace/name) to update.")
    parent.add_argument("--registry", required=True, help="Quilt registry URI (e.g. s3://quilt-bucket).")
    parent.add_argument("--top-hash", help="Optional package top_hash to base the update on.")
    parent.add_argument("--message", default="Attach metadata", help="Push/commit message for the package update.")
    parent.add_argument("--dry-run", action="store_true", help="Do not push changes; print merged metadata instead.")

    parser = argparse.ArgumentParser(description="Attach Benchling or Smartsheet metadata to an existing Quilt package")
    sub = parser.add_subparsers(dest="source", required=True, help="Choice of metadata source")

    # Benchling subparser
    pb = sub.add_parser("benchling", parents=[parent], help="Attach Benchling metadata")
    pb.add_argument("--benchling-entity-id", required=True, help="Benchling custom entity id to fetch metadata from.")
    pb.add_argument("--benchling-api-key", help="Benchling API key (or set BENCHLING_API_KEY environment variable).")
    pb.add_argument("--benchling-header-type", choices=["bearer", "x-api-key"],
                    default=os.environ.get("BENCHLING_HEADER_TYPE", "bearer"),
                    help="Header style used with Benchling (default: bearer).")

    # Smartsheet subparser
    ps = sub.add_parser("smartsheet", parents=[parent], help="Attach Smartsheet metadata")
    ps.add_argument("--smartsheet-sheet-id", required=True, help="Smartsheet sheet id to query.")
    group = ps.add_mutually_exclusive_group(required=True)
    group.add_argument("--smartsheet-row-id", help="Direct Smartsheet row id to fetch.")
    group.add_argument("--smartsheet-run-column", help="Column title containing the run id (use with --run-id).")
    ps.add_argument("--run-id", help="Run id to find in the run column (required with --smartsheet-run-column).")
    ps.add_argument("--smartsheet-token", help="Smartsheet API token (or set SMARTSHEET_TOKEN environment variable).")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """
    Main entrypoint for command-line execution.

    Args:
        argv: Optional list of command-line arguments (defaults to sys.argv[1:]).

    Returns:
        Exit code integer:
            0 - success (push completed or dry-run displayed)
            1 - unexpected error
            2 - invalid/missing credentials or arguments
            3 - user-level error (e.g., no matching row found when required)
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    logger.debug("Starting metadata_integration run with args: %s", vars(args))

    integrator = MetadataIntegrator(package=args.package, registry=args.registry, top_hash=args.top_hash)

    try:
        if args.source == "benchling":
            api_key = args.benchling_api_key or os.environ.get("BENCHLING_API_KEY")
            if not api_key:
                logger.error("Benchling API key not provided (env or --benchling-api-key). Aborting.", extra={"package": args.package})
                return 2

            client = BenchlingClient(api_key=api_key, header_type=args.benchling_header_type)
            try:
                bench_meta = client.get_entity(args.benchling_entity_id)
            except Exception as e:
                logger.warning("Benchling fetch failed; recording error in metadata. entity_id=%s error=%s",
                               args.benchling_entity_id, str(e), exc_info=True)
                bench_meta = {"entity_id": args.benchling_entity_id, "error": str(e)}

            result = integrator.attach_and_push(benchling_meta=bench_meta, smartsheet_meta=None,
                                                message=args.message, dry_run=args.dry_run)
            # Pretty-print results as JSON for easier machine parsing
            logger.info("Integration result: %s", json.dumps(result, indent=2))
            return 0

        elif args.source == "smartsheet":
            token = args.smartsheet_token or os.environ.get("SMARTSHEET_TOKEN")
            if not token:
                logger.error("Smartsheet token not provided (env or --smartsheet-token). Aborting.", extra={"package": args.package})
                return 2

            client = SmartsheetClient(token=token)
            try:
                if args.smartsheet_row_id:
                    sm_meta = client.get_row_by_rowid(sheet_id=args.smartsheet_sheet_id, row_id=args.smartsheet_row_id)
                else:
                    if not args.run_id:
                        logger.error("--run-id is required when using --smartsheet-run-column", extra={"package": args.package})
                        return 2
                    sm_meta = client.get_row_by_run_column(sheet_id=args.smartsheet_sheet_id,
                                                          run_column=args.smartsheet_run_column,
                                                          run_id=args.run_id)
                    if sm_meta.get("row") is None:
                        logger.warning("No matching Smartsheet row found: sheet_id=%s run_column=%s run_id=%s",
                                       args.smartsheet_sheet_id, args.smartsheet_run_column, args.run_id)
                        # We consider "no matching row" a user-level issue (exit code 3)
                        return 3
            except Exception as e:
                logger.warning("Smartsheet fetch failed; recording error in metadata. sheet_id=%s error=%s",
                               args.smartsheet_sheet_id, str(e), exc_info=True)
                sm_meta = {"sheet_id": args.smartsheet_sheet_id, "error": str(e)}

            result = integrator.attach_and_push(benchling_meta=None, smartsheet_meta=sm_meta,
                                                message=args.message, dry_run=args.dry_run)
            logger.info("Integration result: %s", json.dumps(result, indent=2))
            return 0

        else:
            logger.error("Unknown source requested: %s", args.source)
            parser.print_help()
            return 2

    except Exception:
        logger.exception("Unhandled exception in metadata_integration")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
