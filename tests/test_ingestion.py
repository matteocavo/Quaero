from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pandas as pd

from pipelines.ingestion import (
    _assert_url_is_public,
    _detect_source_location_type,
    _detect_source_type,
    ingest_input,
)


class _DummyResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self) -> _DummyResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_ingest_input_local_csv_creates_snapshot_and_manifest(tmp_path: Path) -> None:
    csv_path = tmp_path / "Sales Export.csv"
    csv_path.write_text(
        "customer_id,amount,region\n1,10.5,North\n2,12.0,\n3,9.75,South\n",
        encoding="utf-8",
    )

    manifest = ingest_input(source_path=csv_path, project_root=tmp_path)

    snapshot_dir = tmp_path / "raw"
    snapshot_files = list(snapshot_dir.glob("snapshot_*.parquet"))
    manifest_path = tmp_path / "metadata" / f"run_manifest_{manifest['run_id']}.json"

    assert len(snapshot_files) == 1
    assert manifest_path.exists()
    assert manifest["source"] == "sales_export"
    assert manifest["source_type"] == "csv"
    assert manifest["source_location_type"] == "local"
    assert manifest["original_input"] == str(csv_path)
    assert manifest["source_file"] == str(csv_path.resolve())
    assert manifest["rows_processed"] == 3
    assert manifest["parquet_engine"] == "pyarrow"
    assert manifest["parquet_compression"] == "snappy"
    assert Path(manifest["snapshot_path"]) == Path("raw") / snapshot_files[0].name

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert saved_manifest == manifest

    snapshot_frame = pd.read_parquet(snapshot_files[0])
    assert snapshot_frame.shape == (3, 3)
    assert snapshot_frame["customer_id"].tolist() == [1, 2, 3]

    schema_by_column = {column["name"]: column for column in manifest["schema"]}
    assert schema_by_column["customer_id"]["dtype"] == "int64"
    assert schema_by_column["amount"]["dtype"] == "float64"
    assert schema_by_column["region"]["nullable"] is True


def test_ingest_input_local_parquet_creates_snapshot_and_manifest(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "sales_export.parquet"
    source_frame = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "amount": [10.5, 12.0, 9.75],
            "region": ["North", None, "South"],
        }
    )
    source_frame.to_parquet(parquet_path, index=False, engine="pyarrow")

    manifest = ingest_input(source_path=parquet_path, project_root=tmp_path)

    snapshot_path = tmp_path / manifest["snapshot_path"]
    assert snapshot_path.exists()
    assert manifest["source"] == "sales_export"
    assert manifest["source_type"] == "parquet"
    assert manifest["source_location_type"] == "local"
    assert manifest["original_input"] == str(parquet_path)
    assert manifest["source_file"] == str(parquet_path.resolve())

    snapshot_frame = pd.read_parquet(snapshot_path)
    assert snapshot_frame.to_dict(orient="records") == source_frame.to_dict(
        orient="records"
    )


def test_detect_source_type_and_location_for_local_paths_and_urls() -> None:
    assert _detect_source_location_type("dataset.csv") == "local"
    assert _detect_source_type("dataset.csv") == "csv"

    assert _detect_source_location_type("dataset.parquet") == "local"
    assert _detect_source_type("dataset.parquet") == "parquet"

    assert _detect_source_location_type("file:///tmp/data.csv") == "local"
    assert _detect_source_type("file:///tmp/data.csv") == "csv"

    assert _detect_source_location_type("https://example.com/path/data.csv") == "url"
    assert _detect_source_type("https://example.com/path/data.csv") == "csv"

    assert (
        _detect_source_location_type("https://example.com/path/data.parquet") == "url"
    )
    assert _detect_source_type("https://example.com/path/data.parquet") == "parquet"


def test_ingest_input_reads_csv_url_without_network_dependency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    csv_bytes = b"customer_id,amount\n1,10.5\n2,12.0\n"

    monkeypatch.setattr("pipelines.ingestion._assert_url_is_public", lambda url: None)
    monkeypatch.setattr(
        "pipelines.ingestion.urlopen",
        lambda url: _DummyResponse(csv_bytes),
    )

    manifest = ingest_input(
        source_path="https://example.com/sales_export.csv",
        source_name="Sales Export URL",
        project_root=tmp_path,
    )

    snapshot_path = tmp_path / manifest["snapshot_path"]
    assert snapshot_path.exists()
    assert manifest["source"] == "sales_export_url"
    assert manifest["source_type"] == "csv"
    assert manifest["source_location_type"] == "url"
    assert manifest["original_input"] == "https://example.com/sales_export.csv"
    assert manifest["source_file"] == "https://example.com/sales_export.csv"


def test_ingest_input_reads_parquet_url_without_network_dependency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataframe = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "amount": [10.5, 12.0],
        }
    )
    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False, engine="pyarrow")

    monkeypatch.setattr("pipelines.ingestion._assert_url_is_public", lambda url: None)
    monkeypatch.setattr(
        "pipelines.ingestion.urlopen",
        lambda url: _DummyResponse(buffer.getvalue()),
    )

    manifest = ingest_input(
        source_path="https://example.com/sales_export.parquet",
        source_name="Sales Export URL",
        project_root=tmp_path,
    )

    snapshot_path = tmp_path / manifest["snapshot_path"]
    assert snapshot_path.exists()
    assert manifest["source_type"] == "parquet"
    assert manifest["source_location_type"] == "url"

    snapshot_frame = pd.read_parquet(snapshot_path)
    assert snapshot_frame.to_dict(orient="records") == dataframe.to_dict(
        orient="records"
    )


def test_ingest_input_treats_file_uri_as_local_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "sales_export.csv"
    csv_path.write_text("customer_id,amount\n1,10.5\n", encoding="utf-8")

    manifest = ingest_input(
        source_path=csv_path.resolve().as_uri(), project_root=tmp_path
    )

    assert manifest["source_location_type"] == "local"
    assert manifest["source_type"] == "csv"
    assert manifest["source_file"] == str(csv_path.resolve())


def test_assert_url_is_public_blocks_private_addresses(monkeypatch) -> None:
    monkeypatch.setattr(
        "pipelines.ingestion.socket.getaddrinfo",
        lambda hostname, port: [(None, None, None, None, ("127.0.0.1", 0))],
    )

    try:
        _assert_url_is_public("https://example.com/data.csv")
    except ValueError as error:
        assert "blocked non-public address" in str(error)
    else:
        raise AssertionError("Expected private URL resolution to be blocked.")


def test_ingest_input_uses_explicit_snapshot_name_for_multi_dataset_projects(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "temperature.csv"
    csv_path.write_text("year,mean\n2020,0.8\n2021,0.9\n", encoding="utf-8")

    manifest = ingest_input(
        source_path=csv_path,
        source_name="temperature",
        project_root=tmp_path,
        snapshot_name="temperature",
    )

    assert manifest["snapshot_path"] == "raw/temperature.parquet"
    assert (tmp_path / "raw" / "temperature.parquet").exists()
