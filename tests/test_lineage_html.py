"""Tests for the Mermaid-based lineage HTML renderer."""

from __future__ import annotations

import json

from pipelines.lineage_html import render_lineage_html, _sanitize_id, _build_mermaid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_lineage(tmp_path, nodes=None, edges=None, source="test_source"):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": source,
        "generated_at": "2024-01-01T00:00:00+00:00",
        "node_count": len(nodes or []),
        "edge_count": len(edges or []),
        "nodes": nodes or [],
        "edges": edges or [],
    }
    (metadata_dir / "lineage.json").write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _minimal_nodes():
    return [
        {"id": "raw::streams", "layer": "raw", "name": "streams"},
        {"id": "staging::streams", "layer": "staging", "name": "streams"},
        {
            "id": "mart::mart_trend::avg_streams",
            "layer": "mart",
            "name": "avg_streams",
            "mart": "mart_trend",
        },
    ]


def _minimal_edges():
    return [
        {"from": "raw::streams", "to": "staging::streams", "type": "passthrough"},
        {
            "from": "staging::streams",
            "to": "mart::mart_trend::avg_streams",
            "type": "aggregated",
        },
    ]


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestSanitizeId:
    def test_double_colon_replaced(self):
        assert "::" not in _sanitize_id("raw::col_name")

    def test_spaces_replaced(self):
        assert " " not in _sanitize_id("raw::col name")

    def test_dashes_replaced(self):
        assert "-" not in _sanitize_id("raw::col-name")

    def test_round_trip_stable(self):
        result = _sanitize_id("mart::mart_trend::avg_streams")
        assert result == "mart__mart_trend__avg_streams"


class TestBuildMermaid:
    def test_contains_graph_lr(self):
        lineage = (
            _write_lineage(
                tmp_path=None,  # not used here; we call _build_mermaid directly
                nodes=_minimal_nodes(),
                edges=_minimal_edges(),
            )
            if False
            else {
                "nodes": _minimal_nodes(),
                "edges": _minimal_edges(),
            }
        )
        result = _build_mermaid(lineage)
        assert "graph LR" in result

    def test_contains_subgraphs(self):
        lineage = {"nodes": _minimal_nodes(), "edges": _minimal_edges()}
        result = _build_mermaid(lineage)
        assert "RAW" in result
        assert "STAGING" in result
        assert "MART" in result

    def test_edge_labels_present(self):
        lineage = {"nodes": _minimal_nodes(), "edges": _minimal_edges()}
        result = _build_mermaid(lineage)
        assert "passthrough" in result
        assert "aggregated" in result


# ---------------------------------------------------------------------------
# Integration tests (file I/O)
# ---------------------------------------------------------------------------


class TestRenderLineageHtml:
    def test_file_created(self, tmp_path):
        _write_lineage(tmp_path, nodes=_minimal_nodes(), edges=_minimal_edges())
        render_lineage_html(project_root=tmp_path)
        assert (tmp_path / "metadata" / "lineage.html").exists()

    def test_returns_correct_output_path(self, tmp_path):
        _write_lineage(tmp_path, nodes=_minimal_nodes(), edges=_minimal_edges())
        result = render_lineage_html(project_root=tmp_path)
        assert result["output_path"] == "metadata/lineage.html"

    def test_html_contains_mermaid_script(self, tmp_path):
        _write_lineage(tmp_path, nodes=_minimal_nodes(), edges=_minimal_edges())
        render_lineage_html(project_root=tmp_path)
        content = (tmp_path / "metadata" / "lineage.html").read_text(encoding="utf-8")
        assert "mermaid" in content.lower()

    def test_html_contains_node_names(self, tmp_path):
        _write_lineage(tmp_path, nodes=_minimal_nodes(), edges=_minimal_edges())
        render_lineage_html(project_root=tmp_path)
        content = (tmp_path / "metadata" / "lineage.html").read_text(encoding="utf-8")
        assert "streams" in content
        assert "avg_streams" in content

    def test_html_contains_all_edge_types_in_legend(self, tmp_path):
        _write_lineage(tmp_path)
        render_lineage_html(project_root=tmp_path)
        content = (tmp_path / "metadata" / "lineage.html").read_text(encoding="utf-8")
        for edge_type in ("passthrough", "normalized", "type_cast", "aggregated"):
            assert edge_type in content

    def test_skips_gracefully_when_no_lineage_json(self, tmp_path):
        (tmp_path / "metadata").mkdir(parents=True, exist_ok=True)
        result = render_lineage_html(project_root=tmp_path)
        assert result.get("skipped") is True
        assert result["output_path"] == "metadata/lineage.html"

    def test_empty_lineage_produces_valid_html(self, tmp_path):
        _write_lineage(tmp_path, nodes=[], edges=[])
        render_lineage_html(project_root=tmp_path)
        content = (tmp_path / "metadata" / "lineage.html").read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content
