"""Input ingestion pipeline for the raw data layer."""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import re
import socket
import uuid
from datetime import datetime, timezone
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname, urlopen

from pipelines.utils import normalize_name, resolve_project_root

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


LOGGER = logging.getLogger(__name__)

SUPPORTED_SOURCE_TYPES = {"csv", "parquet"}
SUPPORTED_URL_SCHEMES = {"http", "https"}


def ingest_input(
    source_path: str | Path,
    source_name: str | None = None,
    project_root: str | Path | None = None,
    snapshot_name: str | None = None,
    source_format: str | None = None,
    read_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load a CSV or Parquet input, write a canonical parquet snapshot, and persist metadata."""
    original_input = str(source_path)
    source_location_type = _detect_source_location_type(source_path)
    source_type = _detect_source_type(source_path, source_format=source_format)
    resolved_source = _resolve_source_reference(source_path, source_location_type)

    root_dir = resolve_project_root(project_root, __file__)
    source = _derive_source_name(source_path, source_name)
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)

    raw_dir = root_dir / "raw"
    metadata_dir = root_dir / "metadata"
    raw_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    if snapshot_name is None:
        snapshot_filename = (
            f"snapshot_{run_timestamp.strftime('%Y_%m_%d_%H%M%S')}.parquet"
        )
    else:
        snapshot_filename = f"{normalize_name(snapshot_name)}.parquet"
    snapshot_path = raw_dir / snapshot_filename
    manifest_path = metadata_dir / f"run_manifest_{run_id}.json"

    LOGGER.info("Ingestion started for %s", resolved_source)
    dataframe = _load_input_dataframe(
        source_reference=resolved_source,
        source_location_type=source_location_type,
        source_type=source_type,
        read_options=read_options,
    )
    schema = infer_schema(dataframe)
    schema_summary = {
        column_schema["name"]: column_schema["dtype"]
        for column_schema in schema["columns"]
    }

    LOGGER.info(
        "Dataset size: %s rows x %s columns",
        schema["row_count"],
        schema["column_count"],
    )
    LOGGER.debug("Schema summary: %s", schema_summary)

    _write_parquet_snapshot(dataframe, snapshot_path)
    manifest = _build_run_manifest(
        run_id=run_id,
        run_timestamp=run_timestamp,
        project_root=root_dir,
        source=source,
        original_input=original_input,
        source_reference=resolved_source,
        source_type=source_type,
        source_location_type=source_location_type,
        snapshot_path=snapshot_path,
        schema=schema,
    )

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    LOGGER.info("Run metadata written to %s", manifest_path)
    LOGGER.info("Ingestion completed. Snapshot saved to %s", snapshot_path)

    return manifest


def ingest_dataframe(
    dataframe: pd.DataFrame,
    source_name: str,
    original_input: str,
    project_root: str | Path | None = None,
    snapshot_name: str | None = None,
    source_type: str = "dataframe",
    source_location_type: str = "custom_loader",
) -> dict[str, Any]:
    """Persist an already-loaded DataFrame into the canonical raw layer."""
    root_dir = resolve_project_root(project_root, __file__)
    source = normalize_name(source_name)
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)

    raw_dir = root_dir / "raw"
    metadata_dir = root_dir / "metadata"
    raw_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    if snapshot_name is None:
        snapshot_filename = (
            f"snapshot_{run_timestamp.strftime('%Y_%m_%d_%H%M%S')}.parquet"
        )
    else:
        snapshot_filename = f"{normalize_name(snapshot_name)}.parquet"
    snapshot_path = raw_dir / snapshot_filename
    manifest_path = metadata_dir / f"run_manifest_{run_id}.json"

    LOGGER.info("Ingestion started for custom-loaded source %s", original_input)
    schema = infer_schema(dataframe)
    _write_parquet_snapshot(dataframe, snapshot_path)
    manifest = _build_run_manifest(
        run_id=run_id,
        run_timestamp=run_timestamp,
        project_root=root_dir,
        source=source,
        original_input=original_input,
        source_reference=original_input,
        source_type=source_type,
        source_location_type=source_location_type,
        snapshot_path=snapshot_path,
        schema=schema,
    )
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    LOGGER.info("Run metadata written to %s", manifest_path)
    LOGGER.info("Ingestion completed. Snapshot saved to %s", snapshot_path)
    return manifest


def ingest_csv(
    csv_path: str | Path,
    source_name: str | None = None,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Backward-compatible wrapper for the original CSV ingestion entrypoint."""
    return ingest_input(
        source_path=csv_path,
        source_name=source_name,
        project_root=project_root,
    )


def infer_schema(dataframe: pd.DataFrame) -> dict[str, Any]:
    """Build a lightweight schema summary from a pandas DataFrame."""
    columns: list[dict[str, Any]] = []

    for column_name in dataframe.columns:
        series = dataframe[column_name]
        columns.append(
            {
                "name": column_name,
                "dtype": str(series.dtype),
                "nullable": bool(series.isna().any()),
            }
        )

    return {
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": columns,
    }


def _load_input_dataframe(
    source_reference: str,
    source_location_type: str,
    source_type: str,
    read_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Route local and URL inputs to the correct pandas loader."""
    if source_location_type == "local":
        return _load_local_dataframe(Path(source_reference), source_type, read_options)

    return _load_url_dataframe(source_reference, source_type, read_options)


def _load_local_dataframe(
    source_path: Path,
    source_type: str,
    read_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load a local CSV or Parquet file with pandas."""
    if source_type == "csv":
        options = {"low_memory": False, **(read_options or {})}
        return pd.read_csv(source_path, **options)

    return pd.read_parquet(source_path)


def _load_url_dataframe(
    source_url: str,
    source_type: str,
    read_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Download a CSV or Parquet file to a temporary file and load it with pandas."""
    _assert_url_is_public(source_url)

    suffix = ".csv" if source_type == "csv" else ".parquet"
    with tempfile.TemporaryDirectory() as temporary_dir:
        temporary_path = Path(temporary_dir) / f"remote_source{suffix}"
        with urlopen(source_url) as response, temporary_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)

        if source_type == "csv":
            options = {"low_memory": False, **(read_options or {})}
            return pd.read_csv(temporary_path, **options)

        return pd.read_parquet(temporary_path)


def _write_parquet_snapshot(dataframe: pd.DataFrame, snapshot_path: Path) -> str:
    """Persist the canonical raw snapshot as parquet using pyarrow."""
    table = pa.Table.from_pandas(dataframe, preserve_index=False)
    pq.write_table(table, snapshot_path, compression="snappy")
    return "pyarrow"


def _build_run_manifest(
    run_id: str,
    run_timestamp: datetime,
    project_root: Path,
    source: str,
    original_input: str,
    source_reference: str,
    source_type: str,
    source_location_type: str,
    snapshot_path: Path,
    schema: dict[str, Any],
) -> dict[str, Any]:
    """Create the metadata manifest for a single ingestion run."""
    return {
        "run_id": run_id,
        "timestamp": run_timestamp.isoformat(),
        "source": source,
        "original_input": original_input,
        "source_file": source_reference,
        "source_type": source_type,
        "source_location_type": source_location_type,
        "rows_processed": schema["row_count"],
        "pipeline_steps": [
            "load_input",
            "infer_schema",
            "save_parquet_snapshot",
            "generate_run_metadata",
        ],
        "snapshot_path": snapshot_path.relative_to(project_root).as_posix(),
        "schema": schema["columns"],
        "parquet_engine": "pyarrow",
        "parquet_compression": "snappy",
    }


def _resolve_source_reference(
    source_path: str | Path,
    source_location_type: str,
) -> str:
    """Return a normalized local path or URL string for the source input."""
    if source_location_type == "local":
        resolved_path = _coerce_local_source_path(source_path).expanduser().resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Input file not found: {resolved_path}")
        return str(resolved_path)

    return str(source_path)


def _derive_source_name(
    source_path: str | Path,
    source_name: str | None,
) -> str:
    """Normalize the source name for use as a project folder name."""
    if source_name:
        candidate = source_name
    else:
        source_value = str(source_path)
        if _detect_source_location_type(source_value) == "url":
            candidate = Path(urlparse(source_value).path).stem
        else:
            candidate = Path(_coerce_local_source_path(source_value)).stem

    return normalize_name(candidate)


def _detect_source_location_type(source_path: str | Path) -> str:
    """Classify an input as a local path or a supported URL."""
    parsed = urlparse(str(source_path))
    scheme = parsed.scheme.lower()

    if scheme == "file":
        return "local"
    if len(scheme) > 1 and scheme in SUPPORTED_URL_SCHEMES:
        return "url"

    return "local"


def _detect_source_type(
    source_path: str | Path,
    source_format: str | None = None,
) -> str:
    """Infer CSV vs Parquet from the input path or URL extension."""
    if source_format is not None:
        normalized_format = source_format.lower()
        if normalized_format not in SUPPORTED_SOURCE_TYPES:
            raise ValueError(
                "Unsupported input type override. Only CSV and Parquet files are supported."
            )
        return normalized_format

    source_value = str(source_path)
    parsed = urlparse(source_value)
    scheme = parsed.scheme.lower()

    if scheme == "file":
        path_value = url2pathname(parsed.path or "")
    elif _detect_source_location_type(source_value) == "url":
        path_value = parsed.path
    else:
        path_value = source_value

    suffix = Path(path_value).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".parquet", ".pq"}:
        return "parquet"

    raise ValueError(
        "Unsupported input type. Only CSV and Parquet files are supported."
    )


def _coerce_local_source_path(source_path: str | Path) -> Path:
    """Convert local file references, including file:// URIs, into filesystem paths."""
    source_value = str(source_path)
    parsed = urlparse(source_value)

    if parsed.scheme.lower() != "file":
        return Path(source_value)

    local_path = url2pathname(parsed.path or "")
    if parsed.netloc and parsed.netloc not in {"", "localhost"}:
        local_path = f"//{parsed.netloc}{local_path}"
    if re.match(r"^/[a-zA-Z]:", local_path):
        local_path = local_path[1:]

    return Path(local_path)


def _assert_url_is_public(source_url: str) -> None:
    """Reject URL inputs that resolve to non-public network addresses."""
    parsed = urlparse(source_url)
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL input must include a hostname.")

    try:
        address_infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as error:
        raise ValueError(f"Could not resolve URL hostname: {hostname}") from error

    for _, _, _, _, sockaddr in address_infos:
        address = ipaddress.ip_address(sockaddr[0])
        if (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
            or address.is_multicast
            or address.is_unspecified
        ):
            raise ValueError(f"URL resolves to a blocked non-public address: {address}")


def main() -> None:
    """Provide a small CLI for local ingestion runs."""
    parser = argparse.ArgumentParser(
        description="Ingest a CSV or Parquet file into the raw layer."
    )
    parser.add_argument(
        "source_path",
        help="Path or direct URL for the source CSV or Parquet file.",
    )
    parser.add_argument(
        "--source-name",
        dest="source_name",
        help="Optional override for the source folder name.",
    )
    parser.add_argument(
        "--project-root",
        dest="project_root",
        help="Optional repository root for data and metadata outputs.",
    )

    args = parser.parse_args()
    ingest_input(
        source_path=args.source_path,
        source_name=args.source_name,
        project_root=args.project_root,
    )


if __name__ == "__main__":
    main()
