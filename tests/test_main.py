from __future__ import annotations

import json
import sys

from app import main as main_module


def test_run_pipeline_wrapper_delegates_to_pipeline_runner(
    tmp_path, monkeypatch
) -> None:
    expected = {"final_summary_report": {"summary": {"source": "demo"}}}
    captured: dict[str, object] = {}

    def fake_run_dataset_pipeline(**kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(main_module, "run_dataset_pipeline", fake_run_dataset_pipeline)

    result = main_module.run_pipeline(
        dataset_path="dataset.csv",
        source_name="demo_source",
        question="Which years perform best?",
        project_root=str(tmp_path),
    )

    assert result == expected
    assert captured == {
        "source_path": "dataset.csv",
        "source_name": "demo_source",
        "question": "Which years perform best?",
        "project_root": str(tmp_path),
    }


def test_main_cli_keeps_single_dataset_overrides_backward_compatible(
    monkeypatch, capsys
) -> None:
    expected = {"final_summary_report": {"summary": {"status": "ok"}}}
    captured: dict[str, object] = {}

    def fake_run_dataset_pipeline(**kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(main_module, "run_dataset_pipeline", fake_run_dataset_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "dataset.csv",
            "--source-name",
            "demo_source",
            "--question",
            "Which providers perform best?",
            "--metric-column",
            "streams",
            "--dimension-column",
            "provider",
            "--catalog-column",
            "catalog",
            "--provider-column",
            "publisher",
            "--release-year-column",
            "year",
            "--project-root",
            ".",
        ],
    )

    main_module.main()

    assert captured == {
        "source_path": "dataset.csv",
        "source_name": "demo_source",
        "question": "Which providers perform best?",
        "metric_column": "streams",
        "dimension_column": "provider",
        "catalog_column": "catalog",
        "provider_column": "publisher",
        "release_year_column": "year",
        "project_root": ".",
    }
    assert (
        json.loads(capsys.readouterr().out)
        == expected["final_summary_report"]["summary"]
    )
