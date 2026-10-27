"""Tests for the pure console helpers."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from console import formatting as fmt


class TestValidation:
    @pytest.mark.parametrize("name", ["demo", "release_impact", "Demo-1", "a" * 80])
    def test_valid_identifiers(self, name: str) -> None:
        assert fmt.validate_identifier(name) is None

    @pytest.mark.parametrize("name", ["", "a b", "a.b", "../x", "a/b", "a;b", "é", "a\nb"])
    def test_invalid_identifiers(self, name: str) -> None:
        assert fmt.validate_identifier(name)

    def test_valid_request(self) -> None:
        assert fmt.validate_run_request("demo", "What?", "", has_config=True) == []

    def test_blank_question(self) -> None:
        errors = fmt.validate_run_request("demo", "   ", "", has_config=True)
        assert any("question" in e for e in errors)

    def test_question_too_long(self) -> None:
        errors = fmt.validate_run_request("demo", "x" * 1001, "", has_config=True)
        assert any("1000" in e for e in errors)

    def test_url_is_required_without_config(self) -> None:
        errors = fmt.validate_run_request("demo", "q?", "", has_config=False)
        assert any("project_config.json" in e for e in errors)

    def test_url_not_required_with_config(self) -> None:
        assert fmt.validate_run_request("demo", "q?", "", has_config=True) == []

    @pytest.mark.parametrize("url", ["ftp://x/y", "javascript:alert(1)", "not a url", "http://"])
    def test_invalid_urls(self, url: str) -> None:
        errors = fmt.validate_run_request("demo", "q?", url, has_config=True)
        assert any("URL" in e for e in errors)

    def test_valid_url_without_config(self) -> None:
        assert fmt.validate_run_request("demo", "q?", "https://e.com/d.csv", False) == []

    def test_invalid_project_in_request(self) -> None:
        assert fmt.validate_run_request("bad name", "q?", "", has_config=True)


class TestRuns:
    def test_duration_formats(self) -> None:
        assert fmt.format_duration(5) == "5s"
        assert fmt.format_duration(125) == "2m 05s"
        assert fmt.format_duration(3725) == "1h 02m 05s"
        assert fmt.format_duration(-3) == "0s"

    def test_run_duration(self) -> None:
        run = {
            "started_at": "2026-10-27T10:00:00+00:00",
            "completed_at": "2026-10-27T10:02:15+00:00",
        }
        assert fmt.run_duration(run) == "2m 15s"

    @pytest.mark.parametrize(
        "run",
        [
            {},
            {"started_at": "2026-10-27T10:00:00+00:00"},
            {"started_at": "garbage", "completed_at": "garbage"},
        ],
    )
    def test_duration_not_computable(self, run: dict) -> None:
        assert fmt.run_duration(run) == "-"

    def test_latest_active_run(self) -> None:
        runs = [{"status": "COMPLETED"}, {"status": "RUNNING", "run_id": "r"}, {"status": "QUEUED"}]
        assert fmt.latest_active_run(runs)["run_id"] == "r"
        assert fmt.latest_active_run([{"status": "FAILED"}]) is None
        assert fmt.latest_active_run([]) is None

    def test_rows_include_duration_and_error(self) -> None:
        rows = fmt.runs_to_rows(
            [
                {
                    "run_id": "r1",
                    "status": "FAILED",
                    "created_at": "c",
                    "started_at": "2026-10-27T10:00:00+00:00",
                    "completed_at": "2026-10-27T10:00:30+00:00",
                    "error": "boom",
                    "question": "q?",
                }
            ]
        )
        assert rows[0]["duration"] == "30s"
        assert rows[0]["error"] == "boom"
        assert "FAILED" in rows[0]["status"]

    def test_status_label_unknown_passthrough(self) -> None:
        assert fmt.status_label("WEIRD") == "WEIRD"


class TestPolling:
    ACTIVE: ClassVar[list[dict[str, Any]]] = [{"status": "RUNNING"}]
    DONE: ClassVar[list[dict[str, Any]]] = [{"status": "COMPLETED"}]

    def test_polls_only_when_enabled_and_active(self) -> None:
        assert fmt.should_poll(True, self.ACTIVE, 0)
        assert not fmt.should_poll(False, self.ACTIVE, 0)
        assert not fmt.should_poll(True, self.DONE, 0)
        assert not fmt.should_poll(True, [], 0)

    def test_poll_budget_is_bounded(self) -> None:
        assert fmt.should_poll(True, self.ACTIVE, fmt.MAX_POLLS - 1)
        assert not fmt.should_poll(True, self.ACTIVE, fmt.MAX_POLLS)


class TestTables:
    ROWS: ClassVar[list[dict[str, Any]]] = [{"a": "Alpha", "b": 1}, {"a": "beta", "b": 22}]

    def test_filter_is_case_insensitive_across_columns(self) -> None:
        assert fmt.filter_rows(self.ROWS, "ALPHA") == [self.ROWS[0]]
        assert fmt.filter_rows(self.ROWS, "22") == [self.ROWS[1]]

    def test_blank_filter_returns_everything(self) -> None:
        assert fmt.filter_rows(self.ROWS, "  ") == self.ROWS

    def test_no_match(self) -> None:
        assert fmt.filter_rows(self.ROWS, "zzz") == []

    @pytest.mark.parametrize(
        ("total", "size", "expected"),
        [(0, 50, 1), (1, 50, 1), (50, 50, 1), (51, 50, 2), (250, 100, 3)],
    )
    def test_total_pages(self, total: int, size: int, expected: int) -> None:
        assert fmt.total_pages(total, size) == expected

    def test_total_pages_rejects_bad_size(self) -> None:
        with pytest.raises(ValueError):
            fmt.total_pages(10, 0)

    def test_anomaly_rows_v1_have_no_framing_columns(self) -> None:
        report = {
            "version": "v1",
            "anomalies": [
                {"column": "x", "method": "iqr", "anomaly_count": 2, "example_values": [1, 2]}
            ],
        }
        row = fmt.anomalies_to_rows(report)[0]
        assert "impact" not in row
        assert row["example_values"] == "1, 2"

    def test_anomaly_rows_v2_have_framing_columns(self) -> None:
        report = {
            "version": "v2",
            "anomalies": [
                {
                    "column": "x",
                    "method": "iqr",
                    "anomaly_count": 2,
                    "frequency": 0.1,
                    "impact": "high",
                    "suggested_action": "check",
                }
            ],
        }
        row = fmt.anomalies_to_rows(report)[0]
        assert row["impact"] == "high"
        assert row["suggested_action"] == "check"

    def test_dq_rows(self) -> None:
        report = {
            "columns": [
                {
                    "column": "c",
                    "dtype": "int64",
                    "completeness": 100.0,
                    "uniqueness": 50.0,
                    "outlier_flag": False,
                    "format_score": 100.0,
                    "dq_score": 90.0,
                }
            ]
        }
        assert fmt.dq_columns_to_rows(report)[0]["dq_score"] == 90.0


class TestLineageDot:
    LINEAGE: ClassVar[dict[str, Any]] = {
        "nodes": [
            {"id": "raw::a", "layer": "raw", "name": "a"},
            {"id": "staging::a", "layer": "staging", "name": "a"},
            {"id": "mart::m::a", "layer": "mart", "mart": "m", "name": "a"},
        ],
        "edges": [
            {"from": "raw::a", "to": "staging::a", "type": "passthrough"},
            {"from": "staging::a", "to": "mart::m::a", "type": "aggregated"},
            {"from": "ghost", "to": "staging::a", "type": "passthrough"},
        ],
    }

    def test_builds_a_digraph_and_skips_unknown_nodes(self) -> None:
        dot = fmt.lineage_to_dot(self.LINEAGE)
        assert dot.startswith("digraph lineage {")
        assert dot.count("->") == 2
        assert "ghost" not in dot

    def test_edge_type_filter(self) -> None:
        dot = fmt.lineage_to_dot(self.LINEAGE, ["aggregated"])
        assert dot.count("->") == 1
        assert "aggregated" in dot and "passthrough" not in dot

    def test_empty_filter_draws_no_edges(self) -> None:
        assert "->" not in fmt.lineage_to_dot(self.LINEAGE, [])

    def test_edge_types_listing(self) -> None:
        assert fmt.lineage_edge_types(self.LINEAGE) == ["aggregated", "passthrough"]

    def test_hostile_labels_cannot_break_out_of_the_dot_string(self) -> None:
        hostile = 'x"]; evil [label="y\n</script><img src=x onerror=alert(1)>\\'
        lineage = {
            "nodes": [{"id": "raw::x", "layer": "raw", "name": hostile}],
            "edges": [{"from": "raw::x", "to": "raw::x", "type": 'a"b'}],
        }
        dot = fmt.lineage_to_dot(lineage)
        assert "\n</script>" not in dot
        assert '\\"]; evil [label=\\"y' in dot
        assert dot.count("\n") == len(dot.splitlines()) - 1
        # node ids are synthetic, never taken from artifact content
        assert "n0 [" in dot and "raw::x" not in dot
