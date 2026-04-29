"""Simple orchestration entrypoint for the analytics pipeline."""

from __future__ import annotations

import json
import logging
import shutil
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from kpi_engine.analytics_metadata import build_analytics_metadata
from kpi_engine.anomaly_detection import detect_anomalies
from kpi_engine.metric_inference import (
    infer_aggregation_intent,
    infer_dimension_column,
    infer_metric_column,
    score_dimension_candidates,
    score_metric_candidates,
)
from kpi_engine.metrics_definitions import write_metrics_definitions
from kpi_engine.semantic_contract import infer_metric_unit
from kpi_engine.suggest_kpis import suggest_kpis
from pipelines.cleaning import clean_dataframe
from pipelines.dashboard_suggestions import generate_dashboard_suggestions
from pipelines.ingestion import ingest_dataframe, ingest_input
from pipelines.integration import (
    build_integrated_master_dataset,
    standardize_dataset_for_integration,
)
from pipelines.mart_builder import (
    build_catalog_summary_mart,
    build_climate_correlation_mart,
    build_distribution_mart,
    build_integrated_correlation_mart,
    build_integrated_decade_mart,
    build_integrated_top_ranked_mart,
    build_integrated_trend_mart,
    build_provider_performance_mart,
    build_question_answer_mart,
    build_release_impact_mart,
    build_summary_stats_mart,
    build_time_trend_mart,
    build_top_entities_mart,
    find_integrated_metric_columns,
    find_time_column,
)
from pipelines.project_loaders import load_dataset_from_config
from pipelines.profiling import profile_dataframe
from pipelines.utils import format_analysis_title, normalize_name, resolve_project_root
from pipelines.dq_scorer import score_dataframe
from pipelines.sql_export import export_marts_to_sql
from pipelines.pbi_export import export_pbi_schema
from pipelines.lineage import build_lineage
from pipelines.lineage_html import render_lineage_html
from pipelines.anomaly_framing import frame_anomalies
from kpi_engine.metrics_yaml import write_metrics_yaml


LOGGER = logging.getLogger(__name__)


def run_project_from_config(
    config_path: str | Path,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run a reusable config-driven project with one or more datasets."""
    repo_root = resolve_project_root(project_root, __file__)
    config = _load_project_config(config_path, repo_root)
    project_name = config["project"]
    question = config["question"]
    project_mode = "multi_dataset" if len(config["datasets"]) > 1 else "single_dataset"
    normalized_source_name, project_dir = _resolve_project_dir(
        repo_root=repo_root,
        source_name=project_name,
    )
    _prepare_config_project_outputs(project_dir, project_mode=project_mode)

    dataset_runs: list[dict[str, Any]] = []
    standardized_datasets: list[dict[str, Any]] = []
    prepared_datasets: list[dict[str, Any]] = []

    for dataset in config["datasets"]:
        dataset_name = normalize_name(dataset["name"])
        dataset_source = _resolve_config_dataset_source(dataset)
        loader_warnings: list[str] = []
        if dataset.get("loader"):
            loaded_dataframe = load_dataset_from_config(dataset)
            loader_warnings = list(loaded_dataframe.attrs.get("loader_warnings", []))
            ingestion_result = ingest_dataframe(
                dataframe=loaded_dataframe,
                source_name=dataset_name,
                original_input=dataset_source,
                project_root=project_dir,
                snapshot_name=dataset_name,
                source_type="custom_loader",
            )
        else:
            ingestion_result = ingest_input(
                source_path=dataset_source,
                source_name=dataset_name,
                project_root=project_dir,
                snapshot_name=dataset_name,
                source_format=dataset.get("source_format"),
                read_options=dataset.get("read_options"),
            )
        raw_dataframe = pd.read_parquet(project_dir / ingestion_result["snapshot_path"])
        profile_result = profile_dataframe(
            dataframe=raw_dataframe,
            source_name=dataset_name,
            project_root=project_dir,
        )
        cleaning_result = clean_dataframe(
            dataframe=raw_dataframe,
            source_name=dataset_name,
            project_root=project_dir,
        )
        time_col = normalize_name(dataset["time_column"])
        coverage = _compute_dataset_coverage(cleaning_result["dataframe"], time_col)
        prepared_datasets.append(
            {
                "config": dataset,
                "name": dataset_name,
                "source": dataset_source,
                "ingestion_result": ingestion_result,
                "profile_result": profile_result,
                "cleaning_result": cleaning_result,
            }
        )
        if project_mode == "multi_dataset":
            standardized = standardize_dataset_for_integration(
                dataframe=cleaning_result["dataframe"],
                dataset_name=dataset_name,
                time_column=dataset["time_column"],
                metric_column=dataset.get("metric_column"),
            )
            standardized_datasets.append(standardized)
        dataset_runs.append(
            {
                "name": dataset_name,
                "source": dataset_source,
                "time_column": time_col,
                "metric_column": (
                    normalize_name(dataset["metric_column"])
                    if dataset.get("metric_column")
                    else None
                ),
                "unit": None,
                "loader_warnings": loader_warnings,
                "coverage": coverage,
                "raw_snapshot_path": _prefixed_path(
                    f"projects/{normalized_source_name}",
                    ingestion_result["snapshot_path"],
                ),
                "profile_path": _prefixed_path(
                    f"projects/{normalized_source_name}", profile_result["output_path"]
                ),
                "cleaned_data_path": _prefixed_path(
                    f"projects/{normalized_source_name}",
                    cleaning_result["summary"]["output_path"],
                ),
                "cleaning_metadata_path": _prefixed_path(
                    f"projects/{normalized_source_name}",
                    cleaning_result["summary"]["metadata_path"],
                ),
            }
        )

    project_prefix = f"projects/{normalized_source_name}"
    if project_mode == "multi_dataset":
        integrated_output_name = normalize_name(
            str(config.get("integrated_output_name") or "master_dataset")
        )
        integrated_result = build_integrated_master_dataset(
            standardized_datasets=standardized_datasets,
            project_root=project_dir,
            output_name=integrated_output_name,
        )
        integrated_dataframe = integrated_result["dataframe"]
        mart_results = _build_generic_multi_dataset_marts(
            dataframe=integrated_dataframe,
            dataset_trend_aggregations={
                normalize_name(dataset["name"]): dataset.get(
                    "trend_aggregation", "average"
                )
                for dataset in config["datasets"]
            },
            project_root=project_dir,
        )
        mart_results.update(
            _build_climate_specialization_marts(
                dataframe=integrated_dataframe,
                project_root=project_dir,
            )
        )
        question_guidance = _build_question_guidance(
            question=question,
            mart_results=mart_results,
            dataset_metrics=integrated_result["dataset_metrics"],
        )
        metrics_definitions = write_metrics_definitions(
            mart_names=list(mart_results),
            project_root=project_dir,
        )
        analytics_metadata = build_analytics_metadata(
            dataframe=integrated_dataframe,
            source_name=normalized_source_name,
            project_root=project_dir,
        )
        anomaly_result = detect_anomalies(
            dataframe=integrated_dataframe,
            source_name=normalized_source_name,
            project_root=project_dir,
        )
        dashboard_result = generate_dashboard_suggestions(
            dataframe=integrated_dataframe,
            source_name=normalized_source_name,
            question=question,
            primary_mart_name=question_guidance["primary_mart"],
            project_root=project_dir,
        )
        for dataset_run, dataset_metric in zip(
            dataset_runs,
            integrated_result["dataset_metrics"],
            strict=False,
        ):
            dataset_run["time_column"] = dataset_metric["time_column"]
            dataset_run["metric_column"] = dataset_metric["metric_column"]
            dataset_run["unit"] = dataset_metric.get("unit")

        results = {
            "project_mode": project_mode,
            "config_path": _relative_to_repo(repo_root, config["config_path"]),
            "datasets": dataset_runs,
            "integrated_dataset": {
                "output_path": _prefixed_path(
                    project_prefix, integrated_result["output_path"]
                ),
                "dataset_metrics": integrated_result["dataset_metrics"],
            },
            "marts": {
                name: _prefixed_path(project_prefix, result["output_path"])
                for name, result in mart_results.items()
            },
            "metrics_definitions": _with_project_prefix(
                metrics_definitions, project_prefix, "output_path"
            ),
            "anomaly_detection": anomaly_result["report"],
            "anomaly_detection_path": _prefixed_path(
                project_prefix, anomaly_result["report_path"]
            ),
            "analytics_metadata": _with_project_prefix(
                analytics_metadata, project_prefix, "output_path"
            ),
            "dashboard_suggestions": _with_project_prefix(
                dashboard_result, project_prefix, "output_path"
            ),
            "question_guidance": question_guidance,
        }
    else:
        primary_dataset = prepared_datasets[0]
        dataset_config = primary_dataset["config"]
        cleaned_dataframe = primary_dataset["cleaning_result"]["dataframe"]
        query_selection = _resolve_query_selection(
            dataframe=cleaned_dataframe,
            question=question,
            metric_column=dataset_config.get("metric_column"),
            dimension_column=dataset_config.get("dimension_column"),
        )
        resolved_metric_column = query_selection["metric"]["metric_column"]
        mart_build = _build_available_marts(
            dataframe=cleaned_dataframe,
            metric_column=resolved_metric_column,
            dimension_column=query_selection["dimension"]["dimension_column"],
            aggregation=query_selection["aggregation"]["aggregation"],
            ordering=query_selection["aggregation"]["ordering"],
            catalog_column=dataset_config.get("catalog_column"),
            provider_column=dataset_config.get("provider_column", "provider"),
            release_year_column=dataset_config.get(
                "release_year_column", "release_year"
            ),
            project_root=project_dir,
        )
        mart_results = mart_build["marts"]
        question_guidance = _build_question_guidance(
            question=question,
            mart_results=mart_results,
        )
        metrics_definitions = write_metrics_definitions(
            mart_names=list(mart_results),
            project_root=project_dir,
        )
        kpi_result = suggest_kpis(
            question=question,
            dataframe=cleaned_dataframe,
            metric_column=resolved_metric_column,
            aggregation_intent=query_selection["aggregation"]["aggregation"],
        )
        analytics_metadata = build_analytics_metadata(
            dataframe=cleaned_dataframe,
            source_name=normalized_source_name,
            project_root=project_dir,
        )
        anomaly_input = _select_primary_output(cleaned_dataframe, mart_results)
        anomaly_result = detect_anomalies(
            dataframe=anomaly_input,
            source_name=normalized_source_name,
            project_root=project_dir,
        )
        dashboard_result = generate_dashboard_suggestions(
            dataframe=anomaly_input,
            source_name=normalized_source_name,
            question=question,
            primary_mart_name=_select_primary_mart_name(
                mart_results=mart_results,
                question=question,
            ),
            project_root=project_dir,
        )
        dataset_runs[0]["metric_column"] = resolved_metric_column
        dataset_runs[0]["time_column"] = dataset_config.get("time_column")
        dataset_runs[0]["unit"] = infer_metric_unit(resolved_metric_column)

        results = {
            "project_mode": project_mode,
            "config_path": _relative_to_repo(repo_root, config["config_path"]),
            "datasets": dataset_runs,
            "ingestion": _with_project_prefix(
                primary_dataset["ingestion_result"], project_prefix, "snapshot_path"
            ),
            "profiling": _with_project_prefix(
                primary_dataset["profile_result"], project_prefix, "output_path"
            ),
            "cleaning": _with_project_prefix(
                primary_dataset["cleaning_result"]["summary"],
                project_prefix,
                "output_path",
                "metadata_path",
            ),
            "marts": {
                name: _prefixed_path(project_prefix, result["output_path"])
                for name, result in mart_results.items()
            },
            "skipped_marts": mart_build["skipped_marts"],
            "metrics_definitions": _with_project_prefix(
                metrics_definitions, project_prefix, "output_path"
            ),
            "metric_selection": query_selection["metric"],
            "dimension_selection": query_selection["dimension"],
            "aggregation_intent": query_selection["aggregation"],
            "kpi_suggestions": kpi_result,
            "anomaly_detection": anomaly_result["report"],
            "anomaly_detection_path": _prefixed_path(
                project_prefix, anomaly_result["report_path"]
            ),
            "analytics_metadata": _with_project_prefix(
                analytics_metadata, project_prefix, "output_path"
            ),
            "dashboard_suggestions": _with_project_prefix(
                dashboard_result, project_prefix, "output_path"
            ),
            "question_guidance": question_guidance,
        }

    final_summary = _write_config_project_final_summary_report(
        normalized_source_name=normalized_source_name,
        question=question,
        results=results,
        project_dir=project_dir,
        repo_root=repo_root,
    )
    results["final_summary_report"] = final_summary
    project_readme = _write_config_project_readme(
        normalized_source_name=normalized_source_name,
        question=question,
        results=results,
        mart_results=mart_results,
        project_dir=project_dir,
        repo_root=repo_root,
    )
    results["project_readme"] = project_readme
    results["final_summary_report"]["summary"]["project_readme_path"] = project_readme[
        "path"
    ]
    (project_dir / "metadata" / "final_summary_report.json").write_text(
        json.dumps(results["final_summary_report"]["summary"], indent=2),
        encoding="utf-8",
    )
    return results


def run_pipeline(
    source_path: str | Path,
    source_name: str,
    question: str,
    metric_column: str | None = None,
    dimension_column: str | None = None,
    catalog_column: str | None = None,
    provider_column: str = "provider",
    release_year_column: str = "release_year",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run the current pipeline sequentially with simple configurable defaults."""
    repo_root = resolve_project_root(project_root, __file__)
    normalized_source_name, project_dir = _resolve_project_dir(
        repo_root=repo_root,
        source_name=source_name,
    )
    ingestion_result = ingest_input(
        source_path=source_path,
        source_name=source_name,
        project_root=project_dir,
    )
    raw_dataframe = pd.read_parquet(project_dir / ingestion_result["snapshot_path"])

    profile_result = profile_dataframe(
        dataframe=raw_dataframe,
        source_name=source_name,
        project_root=project_dir,
    )
    cleaning_result = clean_dataframe(
        dataframe=raw_dataframe,
        source_name=source_name,
        project_root=project_dir,
    )
    cleaned_dataframe = cleaning_result["dataframe"]
    query_selection = _resolve_query_selection(
        dataframe=cleaned_dataframe,
        question=question,
        metric_column=metric_column,
        dimension_column=dimension_column,
    )
    resolved_metric_column = query_selection["metric"]["metric_column"]

    mart_build = _build_available_marts(
        dataframe=cleaned_dataframe,
        metric_column=resolved_metric_column,
        dimension_column=query_selection["dimension"]["dimension_column"],
        aggregation=query_selection["aggregation"]["aggregation"],
        ordering=query_selection["aggregation"]["ordering"],
        catalog_column=catalog_column,
        provider_column=provider_column,
        release_year_column=release_year_column,
        project_root=project_dir,
    )
    mart_results = mart_build["marts"]
    metrics_definitions = write_metrics_definitions(
        mart_names=list(mart_results),
        project_root=project_dir,
    )

    kpi_result = suggest_kpis(
        question=question,
        dataframe=cleaned_dataframe,
        metric_column=resolved_metric_column,
        aggregation_intent=query_selection["aggregation"]["aggregation"],
    )
    analytics_metadata = build_analytics_metadata(
        dataframe=cleaned_dataframe,
        source_name=source_name,
        project_root=project_dir,
    )

    anomaly_input = _select_primary_output(cleaned_dataframe, mart_results)
    anomaly_result = detect_anomalies(
        dataframe=anomaly_input,
        source_name=source_name,
        project_root=project_dir,
    )

    dashboard_result = generate_dashboard_suggestions(
        dataframe=anomaly_input,
        source_name=source_name,
        question=question,
        primary_mart_name=_select_primary_mart_name(
            mart_results=mart_results,
            question=question,
        ),
        project_root=project_dir,
    )

    # v1 additions: DQ score, SQL export, Power BI schema, lineage, metrics YAML
    mart_dataframes = {
        name: r["dataframe"] for name, r in mart_results.items() if "dataframe" in r
    }
    dq_score_result = score_dataframe(
        dataframe=cleaned_dataframe,
        source_name=source_name,
        project_root=project_dir,
    )
    sql_paths = export_marts_to_sql(mart_dataframes, project_root=project_dir)
    pbi_schema_result = export_pbi_schema(mart_dataframes, project_root=project_dir)
    cleaning_log = cleaning_result.get("cleaning_log", {})
    lineage_result = build_lineage(
        raw_columns=ingestion_result.get("columns", list(cleaned_dataframe.columns)),
        staging_dataframe=cleaned_dataframe,
        marts=mart_dataframes,
        cleaning_log=cleaning_log,
        source_name=source_name,
        project_root=project_dir,
    )
    metrics_yaml_path = write_metrics_yaml(mart_dataframes, project_root=project_dir)

    # v0.4 additions: lineage HTML + anomaly framing v2
    lineage_html_result = render_lineage_html(
        source_name=source_name,
        project_root=project_dir,
    )
    anomaly_framing_result = frame_anomalies(
        source_name=source_name,
        total_row_count=len(anomaly_input),
        project_root=project_dir,
    )

    project_prefix = f"projects/{normalized_source_name}"
    results = {
        "ingestion": _with_project_prefix(
            ingestion_result, project_prefix, "snapshot_path"
        ),
        "profiling": _with_project_prefix(
            profile_result, project_prefix, "output_path"
        ),
        "cleaning": _with_project_prefix(
            cleaning_result["summary"],
            project_prefix,
            "output_path",
            "metadata_path",
        ),
        "cleaning_log": cleaning_log,
        "marts": {
            name: _prefixed_path(project_prefix, result["output_path"])
            for name, result in mart_results.items()
        },
        "skipped_marts": mart_build["skipped_marts"],
        "metrics_definitions": _with_project_prefix(
            metrics_definitions, project_prefix, "output_path"
        ),
        "metric_selection": query_selection["metric"],
        "dimension_selection": query_selection["dimension"],
        "aggregation_intent": query_selection["aggregation"],
        "kpi_suggestions": kpi_result,
        "anomaly_detection": anomaly_result["report"],
        "anomaly_detection_path": _prefixed_path(
            project_prefix, anomaly_result["report_path"]
        ),
        "analytics_metadata": _with_project_prefix(
            analytics_metadata, project_prefix, "output_path"
        ),
        "dashboard_suggestions": _with_project_prefix(
            dashboard_result, project_prefix, "output_path"
        ),
        "dq_score": _with_project_prefix(
            dq_score_result, project_prefix, "output_path"
        ),
        "sql_exports": {
            name: _prefixed_path(project_prefix, p) for name, p in sql_paths.items()
        },
        "pbi_schema": _with_project_prefix(
            pbi_schema_result, project_prefix, "output_path"
        ),
        "lineage": _with_project_prefix(lineage_result, project_prefix, "output_path"),
        "lineage_html": _with_project_prefix(
            lineage_html_result, project_prefix, "output_path"
        ),
        "anomaly_framing_v2": _with_project_prefix(
            anomaly_framing_result, project_prefix, "output_path"
        ),
        "metrics_yaml_path": _prefixed_path(project_prefix, metrics_yaml_path),
    }

    final_summary = _write_final_summary_report(
        normalized_source_name=normalized_source_name,
        question=question,
        results=results,
        project_dir=project_dir,
        repo_root=repo_root,
    )
    results["final_summary_report"] = final_summary
    project_readme = _write_project_readme(
        normalized_source_name=normalized_source_name,
        source_name=source_name,
        question=question,
        results=results,
        analytics_metadata=analytics_metadata,
        mart_results=mart_results,
        project_dir=project_dir,
        repo_root=repo_root,
        source_path=str(source_path),
    )
    results["project_readme"] = project_readme
    results["final_summary_report"]["summary"]["project_readme_path"] = project_readme[
        "path"
    ]
    (project_dir / "metadata" / "final_summary_report.json").write_text(
        json.dumps(results["final_summary_report"]["summary"], indent=2),
        encoding="utf-8",
    )

    return results


def _build_available_marts(
    dataframe: pd.DataFrame,
    metric_column: str,
    dimension_column: str,
    aggregation: str,
    ordering: str,
    catalog_column: str | None,
    provider_column: str,
    release_year_column: str,
    project_root: Path,
) -> dict[str, Any]:
    """Build only the marts supported by the available columns."""
    mart_results: dict[str, dict[str, Any]] = {}
    skipped_marts: list[dict[str, str]] = []
    columns = set(dataframe.columns)

    metric_is_record_count = metric_column == "record_count"
    effective_catalog_column = catalog_column or dimension_column
    physical_metric_exists = metric_column in columns
    time_column = find_time_column(dataframe)

    mart_results["mart_question_answer"] = build_question_answer_mart(
        dataframe=dataframe,
        dimension_column=dimension_column,
        metric_column=metric_column,
        aggregation=aggregation,
        ordering=ordering,
        project_root=project_root,
    )
    mart_results["mart_top_entities"] = build_top_entities_mart(
        dataframe=dataframe,
        dimension_column=dimension_column,
        metric_column=metric_column,
        aggregation=aggregation,
        ordering=ordering,
        project_root=project_root,
    )

    if metric_is_record_count and release_year_column in columns:
        skipped_marts.append(
            {
                "mart_name": "mart_release_impact",
                "reason": (
                    "Skipped because inferred metric 'record_count' is virtual and "
                    f"no physical metric column exists for aggregation with {release_year_column}."
                ),
            }
        )
    if metric_is_record_count and provider_column in columns:
        skipped_marts.append(
            {
                "mart_name": "mart_provider_performance",
                "reason": (
                    "Skipped because inferred metric 'record_count' is virtual and "
                    f"no physical metric column exists for aggregation with {provider_column}."
                ),
            }
        )
    if metric_is_record_count:
        skipped_marts.append(
            {
                "mart_name": "mart_distribution",
                "reason": (
                    "Skipped because inferred metric 'record_count' is virtual and "
                    "no physical numeric metric column exists for distribution bins."
                ),
            }
        )
        skipped_marts.append(
            {
                "mart_name": "mart_summary_stats",
                "reason": (
                    "Skipped because inferred metric 'record_count' is virtual and "
                    "no physical numeric metric column exists for summary statistics."
                ),
            }
        )

    if not metric_is_record_count and {release_year_column, metric_column}.issubset(
        columns
    ):
        mart_results["mart_release_impact"] = build_release_impact_mart(
            dataframe=dataframe,
            metric_column=metric_column,
            release_year_column=release_year_column,
            project_root=project_root,
        )

    if not metric_is_record_count and {provider_column, metric_column}.issubset(
        columns
    ):
        mart_results["mart_provider_performance"] = build_provider_performance_mart(
            dataframe=dataframe,
            metric_column=metric_column,
            provider_column=provider_column,
            project_root=project_root,
        )

    if effective_catalog_column is not None and {effective_catalog_column}.issubset(
        columns
    ):
        mart_results["mart_catalog_summary"] = build_catalog_summary_mart(
            dataframe=dataframe,
            catalog_column=effective_catalog_column,
            metric_column=metric_column if metric_column in columns else None,
            project_root=project_root,
        )

    if physical_metric_exists:
        mart_results["mart_distribution"] = build_distribution_mart(
            dataframe=dataframe,
            metric_column=metric_column,
            project_root=project_root,
        )
        mart_results["mart_summary_stats"] = build_summary_stats_mart(
            dataframe=dataframe,
            metric_column=metric_column,
            project_root=project_root,
        )

    if time_column is not None:
        mart_results["mart_time_trend"] = build_time_trend_mart(
            dataframe=dataframe,
            metric_column=metric_column,
            aggregation=aggregation,
            ordering=ordering,
            time_column=time_column,
            project_root=project_root,
        )

    for skipped in skipped_marts:
        LOGGER.warning(skipped["reason"])

    return {"marts": mart_results, "skipped_marts": skipped_marts}


def _select_primary_output(
    cleaned_dataframe: pd.DataFrame,
    mart_results: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Pick the best available dataset for downstream analytical modules."""
    for mart_name in (
        "mart_question_answer",
        "mart_top_entities",
        "mart_time_trend",
        "mart_release_impact",
        "mart_provider_performance",
        "mart_catalog_summary",
    ):
        if mart_name in mart_results:
            return mart_results[mart_name]["dataframe"]

    return cleaned_dataframe


def _resolve_query_selection(
    dataframe: pd.DataFrame,
    question: str,
    metric_column: str | None,
    dimension_column: str | None,
) -> dict[str, Any]:
    """Resolve metric, dimension, and aggregation intent for the question."""
    metric_candidates = score_metric_candidates(question=question, dataframe=dataframe)
    metric_error: str | None = None

    if metric_column is not None:
        if metric_column not in dataframe.columns:
            raise ValueError(f"Metric column not found in dataset: {metric_column}")
        metric_selection: dict[str, Any] | None = {
            "metric_column": metric_column,
            "selection_mode": "user_provided",
            "candidates": [{"column": metric_column, "score": None}],
        }
    else:
        try:
            metric_selection = infer_metric_column(
                question=question, dataframe=dataframe
            )
        except ValueError as error:
            metric_error = str(error)
            metric_selection = None

    resolved_metric_column = (
        metric_selection["metric_column"] if metric_selection is not None else None
    )
    dimension_candidates = score_dimension_candidates(
        question=question,
        dataframe=dataframe,
        metric_column=resolved_metric_column,
    )
    dimension_error: str | None = None

    if dimension_column is not None:
        if dimension_column not in dataframe.columns:
            raise ValueError(
                f"Dimension column not found in dataset: {dimension_column}"
            )
        dimension_selection: dict[str, Any] | None = {
            "dimension_column": dimension_column,
            "selection_mode": "user_provided",
            "candidates": [{"column": dimension_column, "score": None}],
        }
    else:
        try:
            dimension_selection = infer_dimension_column(
                question=question,
                dataframe=dataframe,
                metric_column=resolved_metric_column,
            )
        except ValueError as error:
            dimension_error = str(error)
            dimension_selection = None

    if metric_error or dimension_error:
        raise ValueError(
            "Could not confidently infer the analytical grouping from the "
            f"question. {_combine_error_messages(metric_error, dimension_error)} "
            f"Top candidate metric columns: {_candidate_columns(metric_candidates)}. "
            "Top candidate dimension columns: "
            f"{_candidate_columns(dimension_candidates)}. "
            "Provide --metric-column and/or --dimension-column manually."
        )

    aggregation_intent = infer_aggregation_intent(question)

    if (
        metric_column is None
        and metric_selection is not None
        and metric_selection["metric_column"] != "record_count"
    ):
        metric_selection["candidates"] = metric_candidates
    if dimension_column is None and dimension_selection is not None:
        dimension_selection["candidates"] = dimension_candidates

    return {
        "metric": metric_selection,
        "dimension": dimension_selection,
        "aggregation": aggregation_intent,
    }


def _combine_error_messages(*messages: str | None) -> str:
    """Join optional inference errors into a compact single sentence."""
    return " ".join(message for message in messages if message)


def _candidate_columns(candidates: list[dict[str, Any]], limit: int = 5) -> str:
    """Return a short comma-separated preview of candidate columns."""
    if not candidates:
        return "none"

    return ", ".join(candidate["column"] for candidate in candidates[:limit])


def _resolve_project_dir(repo_root: Path, source_name: str) -> tuple[str, Path]:
    """Normalize the source name and build the single project directory for the run."""
    normalized_source_name = normalize_name(source_name)
    project_dir = _ensure_project_dir(repo_root, normalized_source_name)
    return normalized_source_name, project_dir


def _ensure_project_dir(repo_root: Path, normalized_source_name: str) -> Path:
    """Create the project-scoped artifact directories for a source."""
    project_dir = repo_root / "projects" / normalized_source_name
    for directory_name in ("raw", "staging", "marts", "metadata"):
        (project_dir / directory_name).mkdir(parents=True, exist_ok=True)
    return project_dir


def _write_final_summary_report(
    normalized_source_name: str,
    question: str,
    results: dict[str, Any],
    project_dir: Path,
    repo_root: Path,
) -> dict[str, Any]:
    """Write a lightweight latest-run summary report for demos and debugging."""
    summary = {
        "source": normalized_source_name,
        "question": question,
        "metric_column": results["metric_selection"]["metric_column"],
        "metric_unit": infer_metric_unit(results["metric_selection"]["metric_column"]),
        "metric_selection_mode": results["metric_selection"]["selection_mode"],
        "metric_candidates": [
            candidate["column"]
            for candidate in results["metric_selection"]["candidates"][:5]
        ],
        "dimension_column": results["dimension_selection"]["dimension_column"],
        "dimension_selection_mode": results["dimension_selection"]["selection_mode"],
        "dimension_candidates": [
            candidate["column"]
            for candidate in results["dimension_selection"]["candidates"][:5]
        ],
        "aggregation_intent": {
            "aggregation": results["aggregation_intent"]["aggregation"],
            "ordering": results["aggregation_intent"]["ordering"],
        },
        "aggregation_selection_mode": results["aggregation_intent"]["selection_mode"],
        "profile_path": results["profiling"]["output_path"],
        "cleaned_data_path": results["cleaning"]["output_path"],
        "cleaning_metadata_path": results["cleaning"]["metadata_path"],
        "mart_paths": results["marts"],
        "skipped_marts": results["skipped_marts"],
        "metrics_definitions_path": results["metrics_definitions"]["output_path"],
        "anomaly_report_path": results["anomaly_detection_path"],
        "anomaly_report_v2_path": results["anomaly_framing_v2"]["output_path"],
        "analytics_metadata_path": results["analytics_metadata"]["output_path"],
        "dashboard_suggestions_path": results["dashboard_suggestions"]["output_path"],
        "lineage_html_path": results["lineage_html"]["output_path"],
        "primary_kpi": results["kpi_suggestions"]["primary_kpi"],
    }

    summary_path = project_dir / "metadata" / "final_summary_report.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return {
        "path": summary_path.relative_to(repo_root).as_posix(),
        "summary": summary,
    }


def _write_project_readme(
    normalized_source_name: str,
    source_name: str,
    question: str,
    results: dict[str, Any],
    analytics_metadata: dict[str, Any],
    mart_results: dict[str, dict[str, Any]],
    project_dir: Path,
    repo_root: Path,
    source_path: str,
) -> dict[str, str]:
    """Write a human-readable README for a single project run."""
    project_title = format_analysis_title(source_name)
    summary = results["final_summary_report"]["summary"]
    profile = _load_profile_metadata(repo_root, summary["profile_path"])
    primary_mart_name = _select_primary_mart_name(
        mart_results=mart_results,
        question=question,
    )
    primary_mart = mart_results.get(primary_mart_name) if primary_mart_name else None
    key_result_table = _build_key_result_table(
        primary_mart["dataframe"] if primary_mart is not None else None
    )
    original_input = results["ingestion"]["original_input"]
    dataset_overview_lines = _build_dataset_overview_lines(profile, original_input)
    dataset_statistics_lines = _build_dataset_statistics_lines(profile)
    dataset_description_lines = _build_dataset_description_lines(
        question=question,
        profile=profile,
        original_input=original_input,
    )
    metric_interpretation_lines = _build_metric_interpretation_lines(
        summary=summary,
        original_input=original_input,
        profile=profile,
    )
    column_summary_table = _build_profile_column_summary_table(profile)
    column_meaning_table = _build_column_meaning_table(
        profile=profile,
        original_input=original_input,
    )
    artifact_lines = _build_project_artifact_lines(
        ingestion_snapshot_path=results["ingestion"]["snapshot_path"],
        summary=summary,
    )
    reproduce_command = _build_reproduce_command(
        source_path=source_path,
        source_name=source_name,
        question=question,
        metric_selection=results["metric_selection"],
        dimension_selection=results["dimension_selection"],
    )

    readme_content = (
        "\n".join(
            [
                f"# {project_title}",
                "",
                "## Question",
                "",
                question,
                "",
                "## Dataset Overview",
                "",
                *dataset_overview_lines,
                "",
                column_summary_table,
                "",
                "## Dataset Statistics",
                "",
                *dataset_statistics_lines,
                "",
                "## Dataset Description",
                "",
                *dataset_description_lines,
                "",
                "## Metric Interpretation",
                "",
                *metric_interpretation_lines,
                "",
                "## Column Meaning",
                "",
                column_meaning_table,
                "",
                "## Analysis Results",
                "",
                (
                    f"Primary mart: `{primary_mart_name}`"
                    if primary_mart_name is not None
                    else "No mart table was generated for this run."
                ),
                *(
                    [
                        _build_question_focus_line(
                            question=question,
                            primary_mart_name=primary_mart_name,
                        )
                    ]
                    if primary_mart_name is not None
                    else []
                ),
                "",
                key_result_table,
                "",
                "## Generated Artifacts",
                "",
                "Analytical marts live under `marts/` and can be loaded directly into BI tools for dashboarding.",
                "",
                *artifact_lines,
                "",
                "## Reproducibility",
                "",
                "```bash",
                reproduce_command,
                "```",
                "",
                "## Supporting Metadata",
                "",
                f"- Possible KPIs: {', '.join(analytics_metadata.get('possible_kpis', [])[:5]) or 'none'}",
                f"- Suggested dimensions: {', '.join(analytics_metadata.get('suggested_dimensions', [])[:5]) or 'none'}",
                "",
            ]
        ).strip()
        + "\n"
    )

    readme_path = project_dir / "README.md"
    readme_path.write_text(readme_content, encoding="utf-8")
    return {"path": readme_path.relative_to(repo_root).as_posix()}


def _load_profile_metadata(repo_root: Path, profile_path: str) -> dict[str, Any]:
    """Load the saved profiling metadata used by project README generation."""
    return json.loads((repo_root / profile_path).read_text(encoding="utf-8"))


def _build_dataset_overview_lines(
    profile: dict[str, Any],
    original_input: str,
) -> list[str]:
    """Build high-level dataset overview bullets from profiling metadata."""
    numeric_columns = _classify_profile_columns(profile, "numeric")
    categorical_columns = _classify_profile_columns(profile, "categorical")
    datetime_columns = _classify_profile_columns(profile, "datetime")

    return [
        f"- Dataset source: `{original_input}`",
        f"- Dataset name: `{profile['source']}`",
        f"- Rows processed: `{profile['row_count']}`",
        f"- Number of columns: `{profile['column_count']}`",
        f"- Detected numeric columns: {_format_column_list(numeric_columns)}",
        f"- Detected categorical columns: {_format_column_list(categorical_columns)}",
        f"- Detected datetime columns: {_format_column_list(datetime_columns)}",
    ]


def _build_dataset_statistics_lines(profile: dict[str, Any]) -> list[str]:
    """Build simple dataset statistics from profiling metadata."""
    numeric_columns = _classify_profile_columns(profile, "numeric")
    categorical_columns = _classify_profile_columns(profile, "categorical")
    datetime_columns = _classify_profile_columns(profile, "datetime")

    return [
        f"- Total rows: `{profile['row_count']}`",
        f"- Total columns: `{profile['column_count']}`",
        f"- Numeric columns count: `{len(numeric_columns)}`",
        f"- Categorical columns count: `{len(categorical_columns)}`",
        f"- Datetime columns count: `{len(datetime_columns)}`",
    ]


def _build_dataset_description_lines(
    question: str,
    profile: dict[str, Any],
    original_input: str,
) -> list[str]:
    """Generate a reusable dataset description for the project README."""
    topics = _infer_dataset_topics(profile=profile, original_input=original_input)
    numeric_columns = _classify_profile_columns(profile, "numeric")
    categorical_columns = _classify_profile_columns(profile, "categorical")
    datetime_columns = _classify_profile_columns(profile, "datetime")

    description = [
        f"- This dataset includes `{profile['row_count']}` rows across `{profile['column_count']}` columns.",
        (
            "- The analysis combines numeric measures such as "
            f"{_format_plain_column_list(numeric_columns)} with grouping columns like "
            f"{_format_plain_column_list(categorical_columns + datetime_columns)}."
        ),
        f"- The current project answers the question: {_shell_quote(question)}.",
    ]

    if "temperature" in topics:
        description[0] = "- This dataset contains temperature observations across time."
        description.insert(
            1,
            "- The data captures temperature anomalies over time and compares periods against a historical baseline.",
        )

    return description


def _build_metric_interpretation_lines(
    summary: dict[str, Any],
    original_input: str,
    profile: dict[str, Any],
) -> list[str]:
    """Explain the inferred metric, aggregation, and unit for the README."""
    metric_column = summary["metric_column"]
    aggregation = summary["aggregation_intent"]["aggregation"]
    unit = _infer_metric_unit_for_readme(
        metric_column=metric_column,
        original_input=original_input,
        profile=profile,
    )
    explanation = _build_metric_explanation(
        metric_column=metric_column,
        aggregation=aggregation,
        original_input=original_input,
        profile=profile,
    )

    return [
        f"- Metric column: `{metric_column}`",
        f"- Dimension column: `{summary['dimension_column']}`",
        f"- Aggregation: `{aggregation}`",
        f"- Unit: `{unit}`",
        f"- Explanation: {explanation}",
    ]


def _build_column_meaning_table(
    profile: dict[str, Any],
    original_input: str,
) -> str:
    """Build a markdown table of inferred column meanings when available."""
    header = "| Column | Meaning |"
    divider = "| ------ | ------- |"
    rows: list[str] = []

    for column in profile.get("columns", []):
        meaning = _infer_column_meaning(
            column_name=str(column.get("column_name", "")),
            original_input=original_input,
            profile=profile,
        )
        if meaning is None:
            continue
        rows.append(
            "| {column_name} | {meaning} |".format(
                column_name=_escape_markdown_cell(column["column_name"]),
                meaning=_escape_markdown_cell(meaning),
            )
        )

    if not rows:
        rows.append(
            "| - | No reliable column meanings were inferred automatically for this dataset. |"
        )

    return "\n".join([header, divider, *rows])


def _infer_metric_unit_for_readme(
    metric_column: str,
    original_input: str,
    profile: dict[str, Any],
) -> str:
    """Infer a human-readable metric unit for the project README."""
    topics = _infer_dataset_topics(profile=profile, original_input=original_input)
    normalized_metric = normalize_name(metric_column)

    if "temperature" in topics and normalized_metric in {
        "mean",
        "temperature",
        "temp",
        "anomaly",
    }:
        return "°C temperature anomaly relative to the baseline."

    inferred_unit = infer_metric_unit(metric_column)
    if inferred_unit == "count":
        return "count"
    if inferred_unit == "persons":
        return "persons"
    if inferred_unit == "celsius":
        return "°C"
    if inferred_unit == "fahrenheit":
        return "°F"
    if inferred_unit == "kelvin":
        return "K"

    if any(
        token in normalized_metric
        for token in ("distance", "miles", "km", "kilometer", "kilometre")
    ):
        return "distance unit unknown"
    if any(
        token in normalized_metric
        for token in ("price", "revenue", "fare", "amount", "sales", "tip")
    ):
        return "currency unknown"
    if any(
        token in normalized_metric
        for token in ("duration", "time", "seconds", "minutes", "hours")
    ):
        return "time unit unknown"

    return "unknown"


def _build_metric_explanation(
    metric_column: str,
    aggregation: str,
    original_input: str,
    profile: dict[str, Any],
) -> str:
    """Build a deterministic explanation of the chosen metric."""
    topics = _infer_dataset_topics(profile=profile, original_input=original_input)
    normalized_metric = normalize_name(metric_column)

    if "temperature" in topics and normalized_metric in {
        "mean",
        "temperature",
        "temp",
        "anomaly",
    }:
        return (
            "The `mean` column represents global temperature anomaly in degrees Celsius relative to the baseline. "
            "Positive values indicate warmer-than-baseline periods, while negative values indicate cooler-than-baseline periods."
        )
    if metric_column == "record_count":
        return "This analysis counts records per inferred dimension to answer the question."

    unit = _infer_metric_unit_for_readme(
        metric_column=metric_column,
        original_input=original_input,
        profile=profile,
    )
    if unit != "unknown":
        return (
            f"This analysis uses the `{metric_column}` column with the `{aggregation}` aggregation. "
            f"The resulting metric is interpreted using the unit `{unit}`."
        )

    return (
        f"This analysis uses the `{metric_column}` column and the `{aggregation}` aggregation "
        "to rank the inferred dimension in the generated marts."
    )


def _build_multi_dataset_metric_line(metric: dict[str, Any]) -> str:
    """Describe one integrated metric for config-driven multi-dataset READMEs."""
    dataset_name = metric["dataset_name"]
    metric_column = metric["metric_column"]
    unit = _humanize_multi_dataset_unit(metric.get("unit"))

    if dataset_name == "temperature_anomaly":
        return f"- `{dataset_name}` comes from `{metric_column}` and represents global temperature anomaly in °C relative to the Berkeley Earth baseline. Unit: `{unit}`."
    if dataset_name == "co2":
        return f"- `{dataset_name}` comes from `{metric_column}` and represents annual world CO2 emissions from the OWID World series. Unit: `{unit}`. The trend mart uses `total_co2` because each yearly value already represents a full-year global emissions total."
    if dataset_name == "sea_level":
        return f"- `{dataset_name}` comes from `{metric_column}` and represents global mean sea level variation from NASA satellite observations. Unit: `{unit}`."
    if dataset_name == "ice_extent":
        return f"- `{dataset_name}` comes from `{metric_column}` and represents Northern Hemisphere sea ice extent from NSIDC monthly observations. Unit: `{unit}`."

    return (
        f"- `{dataset_name}` comes from `{metric_column}` with inferred unit `{unit}`."
    )


def _humanize_multi_dataset_unit(unit: str | None) -> str:
    """Render integrated metric units in user-facing README text."""
    if unit == "celsius":
        return "°C"
    if unit == "million_tonnes_co2":
        return "million tonnes of CO2"
    if unit == "percent":
        return "percent"
    if unit == "twh":
        return "TWh"
    if unit == "usd":
        return "USD"
    if unit == "current_usd":
        return "current USD"
    if unit == "count":
        return "count"
    if unit == "millimeters":
        return "millimeters"
    if unit == "million_square_km":
        return "million square kilometers"
    return unit or "unknown"


def _is_climate_multi_dataset(summary: dict[str, Any]) -> bool:
    """Detect the first climate multi-dataset use case from integrated metric names."""
    metric_names = {
        metric["dataset_name"] for metric in summary.get("dataset_metrics", [])
    }
    return {
        "temperature_anomaly",
        "co2",
        "sea_level",
        "ice_extent",
    }.issubset(metric_names)


def _build_climate_bi_page_lines(summary: dict[str, Any]) -> list[str]:
    """Build the climate-specific BI page recommendations for the README."""
    available_marts = set(summary.get("mart_paths", {}))
    page_lines = [
        "PAGE 1 — Global Warming",
        "- temperature anomaly trend"
        if "mart_temperature_anomaly_trend" in available_marts
        else "- warming trend",
        "- warmest years"
        if "mart_top_warmest_years" in available_marts
        else "- ranked warm years",
        "- temperature by decade if available"
        if "mart_temperature_by_decade" in available_marts
        else "- temperature by decade",
        "",
        "PAGE 2 — Drivers",
        "- co2 trend" if "mart_co2_trend" in available_marts else "- emissions trend",
        "- temperature vs co2 relationship",
        "",
        "PAGE 3 — Impacts",
        "- sea level rise"
        if "mart_sea_level_trend" in available_marts
        else "- sea level trend",
        "- arctic ice decline"
        if "mart_ice_extent_trend" in available_marts
        else "- ice extent trend",
        "- climate correlation"
        if "mart_climate_correlation" in available_marts
        else "- metric correlation",
    ]
    return page_lines


def _build_data_coverage_note_lines(summary: dict[str, Any]) -> list[str]:
    """Explain when some integrated metrics begin later than the project timeline."""
    datasets = summary.get("datasets", [])
    min_years = [
        dataset.get("coverage", {}).get("min_year")
        for dataset in datasets
        if dataset.get("coverage", {}).get("min_year") is not None
    ]
    if not min_years:
        return []

    overall_min_year = min(min_years)
    note_lines = [
        f"- The integrated dataset keeps the union of available years across sources, so the project timeline begins at `{overall_min_year}`."
    ]
    later_start_lines = []
    for dataset in datasets:
        coverage = dataset.get("coverage", {})
        dataset_min_year = coverage.get("min_year")
        if dataset_min_year is None or dataset_min_year <= overall_min_year:
            continue
        later_start_lines.append(
            f"- `{dataset['name']}` has no source data before `{dataset_min_year}`, so `{dataset['name']}` marts begin at `{dataset_min_year}`."
        )

    if not later_start_lines:
        return []

    return note_lines + later_start_lines


def _infer_column_meaning(
    column_name: str,
    original_input: str,
    profile: dict[str, Any],
) -> str | None:
    """Infer a lightweight column meaning description when it is reasonably clear."""
    normalized_column = normalize_name(column_name)
    topics = _infer_dataset_topics(profile=profile, original_input=original_input)
    profile_column = _find_profile_column(profile, normalized_column)

    if "temperature" in topics:
        temperature_meanings = {
            "year": "Year and month of observation",
            "source": "Temperature dataset source",
            "mean": "Global temperature anomaly in °C",
        }
        if normalized_column in temperature_meanings:
            return temperature_meanings[normalized_column]

    generic_meanings = {
        "year": "Time period or year of observation",
        "date": "Date of observation or record",
        "source": "Source label or contributing dataset",
        "provider": "Provider or platform associated with the record",
        "catalog": "Category or catalog grouping used for analysis",
        "region": "Regional grouping used for analysis",
        "country": "Country grouping used for analysis",
        "mean": "Average measured value",
        "population": "Population count for the observed entity",
    }
    if normalized_column in generic_meanings:
        if (
            normalized_column == "year"
            and profile_column
            and _is_datetime_profile_column(profile_column)
        ):
            return "Year and month of observation"
        return generic_meanings[normalized_column]

    if normalized_column.endswith("_id"):
        return "Identifier used to group related records"
    if normalized_column.startswith("is_") or normalized_column.startswith("has_"):
        return "Boolean flag derived from the dataset"

    return None


def _infer_dataset_topics(
    profile: dict[str, Any],
    original_input: str,
) -> set[str]:
    """Infer lightweight dataset topics from the source reference and profile schema."""
    source_reference = original_input
    if original_input.lower().startswith(("http://", "https://")):
        source_reference = normalize_name(original_input)
    else:
        source_reference = normalize_name(Path(original_input).name)

    raw_fragments = [
        normalize_name(str(profile.get("source", ""))),
        source_reference,
        *[
            normalize_name(str(column.get("column_name", "")))
            for column in profile.get("columns", [])
        ],
    ]
    token_set: set[str] = set()
    for fragment in raw_fragments:
        token_set.update(token for token in fragment.split("_") if token)

    topics: set[str] = set()

    if {"temperature", "temp", "climate", "gistemp", "gcag"} & token_set:
        topics.add("temperature")
    if {"population", "people", "persons"} & token_set:
        topics.add("population")
    if {"distance", "miles", "km", "kilometer", "kilometre"} & token_set:
        topics.add("distance")
    if {"fare", "amount", "price", "revenue", "sales", "tip"} & token_set:
        topics.add("currency")

    return topics


def _find_profile_column(
    profile: dict[str, Any],
    normalized_column_name: str,
) -> dict[str, Any] | None:
    """Return a profile column entry by normalized name when present."""
    for column in profile.get("columns", []):
        if normalize_name(str(column.get("column_name", ""))) == normalized_column_name:
            return column
    return None


def _format_plain_column_list(columns: list[str]) -> str:
    """Render a plain-text list of column names for sentence-style README prose."""
    if not columns:
        return "none"
    return ", ".join(f"`{column}`" for column in columns)


def _build_profile_column_summary_table(profile: dict[str, Any]) -> str:
    """Render the profiling metadata as a markdown summary table."""
    header = "| Column | Type | Null % | Unique Values | Example Values |"
    divider = "| ------ | ---- | ------ | ------------- | -------------- |"
    rows = []

    for column in profile.get("columns", []):
        sample_values = ", ".join(
            str(value) for value in column.get("sample_values", [])
        )
        if not sample_values:
            sample_values = "-"
        rows.append(
            "| {column_name} | {dtype} | {null_percentage}% | {unique_count} | {sample_values} |".format(
                column_name=_escape_markdown_cell(column["column_name"]),
                dtype=_humanize_profile_dtype(column),
                null_percentage=_format_null_percentage(column["null_percentage"]),
                unique_count=column["unique_count"],
                sample_values=_escape_markdown_cell(sample_values),
            )
        )

    if not rows:
        rows.append("| - | - | - | - | - |")

    return "\n".join([header, divider, *rows])


def _select_primary_mart_name(
    mart_results: dict[str, dict[str, Any]],
    question: str | None = None,
    dataset_metrics: list[dict[str, Any]] | None = None,
) -> str | None:
    """Pick the primary mart name for summary reporting."""
    if question:
        primary_mart = _select_question_primary_mart_name(
            question=question,
            mart_results=mart_results,
            dataset_metrics=dataset_metrics,
        )
        if primary_mart is not None:
            return primary_mart

    for mart_name in (
        "mart_question_answer",
        "mart_top_entities",
        "mart_time_trend",
        "mart_release_impact",
        "mart_provider_performance",
        "mart_catalog_summary",
    ):
        if mart_name in mart_results:
            return mart_name

    return None


def _select_question_primary_mart_name(
    question: str,
    mart_results: dict[str, dict[str, Any]],
    dataset_metrics: list[dict[str, Any]] | None = None,
) -> str | None:
    """Choose a primary mart whose narrative best matches the project question."""
    if "mart_question_answer" in mart_results:
        return "mart_question_answer"

    question_tokens = _tokenize_question(question)
    primary_metric = _select_question_metric_name(question, dataset_metrics or [])
    relation_tokens = {
        "relate",
        "relationship",
        "relationships",
        "correlation",
        "correlations",
        "compare",
    }
    rank_tokens = {"top", "highest", "warmest", "lowest", "rank", "ranking"}
    trend_tokens = {
        "trend",
        "trends",
        "time",
        "change",
        "changes",
        "over",
        "years",
        "year",
    }

    if primary_metric and rank_tokens & question_tokens:
        if (
            primary_metric == "temperature_anomaly"
            and "mart_top_warmest_years" in mart_results
        ):
            return "mart_top_warmest_years"

    if primary_metric and trend_tokens & question_tokens:
        trend_mart_name = f"mart_{primary_metric}_trend"
        if trend_mart_name in mart_results:
            return trend_mart_name

    if relation_tokens & question_tokens:
        for mart_name in ("mart_climate_correlation", "mart_metric_correlation"):
            if mart_name in mart_results:
                return mart_name

    if primary_metric:
        for mart_name in (
            f"mart_{primary_metric}_trend",
            f"mart_{primary_metric}_summary_stats",
        ):
            if mart_name in mart_results:
                return mart_name

    return None


def _build_question_guidance(
    question: str,
    mart_results: dict[str, dict[str, Any]],
    dataset_metrics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build question-driven narrative guidance without removing generic outputs."""
    primary_mart_name = _select_primary_mart_name(
        mart_results=mart_results,
        question=question,
        dataset_metrics=dataset_metrics,
    )
    primary_metric = _select_primary_metric_from_mart(
        mart_results=mart_results,
        primary_mart_name=primary_mart_name,
    )
    return {
        "question": question,
        "primary_mart": primary_mart_name,
        "primary_kpi": primary_metric or "record_count",
        "focus_reason": _build_question_focus_line(
            question=question,
            primary_mart_name=primary_mart_name,
        )
        .lstrip("- ")
        .rstrip("."),
    }


def _select_primary_metric_from_mart(
    mart_results: dict[str, dict[str, Any]],
    primary_mart_name: str | None,
) -> str | None:
    """Extract the lead metric column from the selected primary mart."""
    if primary_mart_name is None:
        return None

    primary_mart = mart_results.get(primary_mart_name)
    if primary_mart is None:
        return None

    for column_name in primary_mart["dataframe"].columns:
        if column_name not in {
            "year",
            "month",
            "date",
            "decade",
            "rank",
            "record_count",
            "left_metric",
            "right_metric",
        }:
            return str(column_name)

    if "record_count" in primary_mart["dataframe"].columns:
        return "record_count"

    return None


def _select_question_metric_name(
    question: str,
    dataset_metrics: list[dict[str, Any]],
) -> str | None:
    """Pick the project metric most clearly emphasized by the question wording."""
    if not dataset_metrics:
        return None

    normalized_question = normalize_name(question).replace("_", " ")
    scored_metrics: list[tuple[tuple[int, int, int], str]] = []

    for index, metric in enumerate(dataset_metrics):
        metric_name = str(metric["dataset_name"])
        metric_phrase = metric_name.replace("_", " ")
        metric_tokens = set(metric_phrase.split())
        overlap_score = len(metric_tokens & _tokenize_question(question))
        phrase_present = 1 if metric_phrase in normalized_question else 0
        phrase_position = (
            -normalized_question.find(metric_phrase) if phrase_present else -10_000
        )
        scored_metrics.append(
            ((phrase_present, overlap_score, phrase_position), metric_name)
        )

    scored_metrics.sort(key=lambda item: item[0], reverse=True)
    best_score, best_metric = scored_metrics[0]
    if best_score[0] == 0 and best_score[1] == 0:
        return dataset_metrics[0]["dataset_name"]
    return best_metric


def _tokenize_question(question: str) -> set[str]:
    """Split question text into lightweight lowercase tokens."""
    normalized = normalize_name(question)
    return {token for token in normalized.split("_") if token}


def _compute_dataset_coverage(
    dataframe: pd.DataFrame, time_column: str
) -> dict[str, Any]:
    """Return min/max year coverage for a dataset based on its time column."""
    if time_column not in dataframe.columns:
        return {"min_year": None, "max_year": None}
    series = dataframe[time_column].dropna()
    if series.empty:
        return {"min_year": None, "max_year": None}

    if pd.api.types.is_datetime64_any_dtype(series):
        years = series.dt.year
        return {"min_year": int(years.min()), "max_year": int(years.max())}

    string_values = series.astype("string").str.strip()
    if not string_values.empty and string_values.str.fullmatch(r"\d{4}").all():
        years = pd.to_numeric(string_values, errors="coerce").dropna()
        if not years.empty:
            return {"min_year": int(years.min()), "max_year": int(years.max())}

    parsed_datetimes = pd.to_datetime(
        string_values,
        errors="coerce",
        utc=True,
    ).dropna()
    if parsed_datetimes.empty:
        return {"min_year": None, "max_year": None}

    years = parsed_datetimes.dt.year
    return {"min_year": int(years.min()), "max_year": int(years.max())}


def _build_question_focus_line(
    question: str,
    primary_mart_name: str | None,
) -> str:
    """Explain why the selected mart is the narrative lead for the README."""
    if primary_mart_name is None:
        return "- The question did not map to a single narrative mart, so the README summarizes the generated outputs."

    question_tokens = _tokenize_question(question)
    if "correlation" in primary_mart_name or "relate" in question_tokens:
        return f"- The question asks how metrics move together, so `{primary_mart_name}` is treated as the primary narrative mart."
    if "relationship" in question_tokens or "relationships" in question_tokens:
        return f"- The question asks how metrics move together, so `{primary_mart_name}` is treated as the primary narrative mart."
    if "top" in primary_mart_name or "warmest" in primary_mart_name:
        return f"- The question emphasizes ranked outcomes, so `{primary_mart_name}` is treated as the primary narrative mart."
    if "trend" in primary_mart_name:
        return f"- The question emphasizes change over time, so `{primary_mart_name}` is treated as the primary narrative mart."
    return f"- `{primary_mart_name}` is treated as the primary narrative mart for the stated question."


def _build_key_result_table(dataframe: pd.DataFrame | None, limit: int = 5) -> str:
    """Build a small markdown table from the top rows of the primary mart."""
    if dataframe is None or dataframe.empty:
        return "No mart rows are available."

    preview = dataframe.head(limit).copy()
    preview = preview.where(preview.notna(), "")
    columns = [str(column) for column in preview.columns]
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = [
        "| " + " | ".join(_escape_markdown_cell(value) for value in row) + " |"
        for row in preview.itertuples(index=False, name=None)
    ]
    return "\n".join([header, divider, *rows])


def _build_project_artifact_lines(
    ingestion_snapshot_path: str,
    summary: dict[str, Any],
) -> list[str]:
    """Build a deterministic markdown list of key project artifact paths."""
    metadata_dir = f"projects/{summary['source']}/metadata/"
    mart_paths = list(summary["mart_paths"].values())

    lines = [
        f"- Raw snapshot: `{ingestion_snapshot_path}`",
        f"- Clean dataset: `{summary['cleaned_data_path']}`",
        f"- Metadata: `{metadata_dir}`",
    ]
    if summary.get("lineage_html_path"):
        lines.append(f"- Lineage graph: `{summary['lineage_html_path']}`")
    if summary.get("anomaly_report_v2_path"):
        lines.append(f"- Anomaly framing v2: `{summary['anomaly_report_v2_path']}`")

    if summary["mart_paths"]:
        lines.append("- Mart tables:")
        lines.extend([f"  - `{path}`" for path in mart_paths])
    else:
        lines.append("- Mart tables: none")

    return lines


def _build_reproduce_command(
    source_path: str,
    source_name: str,
    question: str,
    metric_selection: dict[str, Any],
    dimension_selection: dict[str, Any],
) -> str:
    """Build a single CLI command that reproduces the analysis."""
    command_parts = [
        "python -m app.main",
        _shell_quote(source_path),
        "--source-name",
        _shell_quote(source_name),
        "--question",
        _shell_quote(question),
        "--project-root",
        _shell_quote("."),
    ]

    if metric_selection["selection_mode"] == "user_provided":
        command_parts.extend(
            ["--metric-column", _shell_quote(metric_selection["metric_column"])]
        )
    if dimension_selection["selection_mode"] == "user_provided":
        command_parts.extend(
            [
                "--dimension-column",
                _shell_quote(dimension_selection["dimension_column"]),
            ]
        )

    return " ".join(command_parts)


def _shell_quote(value: str) -> str:
    """Quote CLI values deterministically for markdown examples."""
    escaped = value.replace('"', '\\"')
    return f'"{escaped}"'


def _classify_profile_columns(
    profile: dict[str, Any],
    kind: str,
) -> list[str]:
    """Group profile columns into broad numeric, categorical, and datetime buckets."""
    columns: list[str] = []
    for column in profile.get("columns", []):
        dtype = str(column.get("dtype", "")).lower()
        is_datetime_column = _is_datetime_profile_column(column)
        if kind == "numeric" and _is_numeric_dtype_name(dtype):
            columns.append(column["column_name"])
        elif kind == "datetime" and is_datetime_column:
            columns.append(column["column_name"])
        elif kind == "categorical" and not (
            _is_numeric_dtype_name(dtype) or is_datetime_column
        ):
            columns.append(column["column_name"])
    return columns


def _is_numeric_dtype_name(dtype: str) -> bool:
    """Return True when a pandas dtype string is numeric-like."""
    return (
        any(token in dtype for token in ("int", "float", "double", "decimal", "number"))
        and "datetime" not in dtype
    )


def _is_datetime_dtype_name(dtype: str) -> bool:
    """Return True when a pandas dtype string is datetime-like."""
    return any(token in dtype for token in ("datetime", "timestamp", "date"))


def _is_datetime_profile_column(column: dict[str, Any]) -> bool:
    """Infer datetime-like columns from dtype, name hints, and sample values."""
    dtype = str(column.get("dtype", "")).lower()
    if _is_datetime_dtype_name(dtype):
        return True

    if _is_numeric_dtype_name(dtype):
        return False

    column_name = normalize_name(column.get("column_name", ""))
    if any(
        hint in column_name
        for hint in (
            "date",
            "datetime",
            "timestamp",
            "created_at",
            "updated_at",
            "date_added",
            "event_date",
        )
    ):
        return True

    sample_values = [
        value for value in column.get("sample_values", []) if value not in (None, "")
    ]
    if not sample_values:
        return False

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed = pd.to_datetime(sample_values, errors="coerce", utc=True)
    return float(parsed.notna().mean()) >= 0.8


def _format_column_list(columns: list[str]) -> str:
    """Render a list of column names for markdown bullets."""
    if not columns:
        return "none"
    return ", ".join(f"`{column}`" for column in columns)


def _humanize_dtype(dtype: str) -> str:
    """Map pandas dtype strings to simple human-readable labels."""
    normalized = dtype.lower()
    if _is_datetime_dtype_name(normalized):
        return "datetime"
    if _is_numeric_dtype_name(normalized):
        if "int" in normalized:
            return "integer"
        return "float"
    if "bool" in normalized:
        return "boolean"
    return "string"


def _humanize_profile_dtype(column: dict[str, Any]) -> str:
    """Map profile metadata to a human-readable type label."""
    if _is_datetime_profile_column(column):
        return "datetime"
    return _humanize_dtype(str(column.get("dtype", "")))


def _format_null_percentage(value: Any) -> str:
    """Format null percentages without trailing decimal noise."""
    numeric_value = float(value)
    if numeric_value.is_integer():
        return str(int(numeric_value))
    return str(numeric_value)


def _escape_markdown_cell(value: Any) -> str:
    """Escape markdown table cell values for deterministic README rendering."""
    return str(value).replace("|", "\\|").replace("\n", " ")


def _load_project_config(
    config_path: str | Path,
    repo_root: Path,
) -> dict[str, Any]:
    """Load and validate a config-driven project with one or more datasets."""
    resolved_path = Path(config_path)
    if not resolved_path.is_absolute():
        resolved_path = (repo_root / resolved_path).resolve()
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if (
        "project" not in payload
        or "datasets" not in payload
        or "question" not in payload
    ):
        raise ValueError(
            "project_config.json must include project, question, and datasets."
        )
    if not payload["datasets"]:
        raise ValueError("project_config.json must define at least one dataset.")
    payload["config_path"] = resolved_path
    return payload


def _prepare_config_project_outputs(project_dir: Path, project_mode: str) -> None:
    """Clear stale generated artifacts before a config-driven project rerun.

    The project config, current README target, and run manifest history are
    preserved. Generated raw/staging/integrated/mart outputs are treated as
    current-state artifacts and are rebuilt from scratch on every config run.
    """
    for directory_name in ("raw", "staging", "marts"):
        directory_path = project_dir / directory_name
        if directory_path.exists():
            shutil.rmtree(directory_path)
        directory_path.mkdir(parents=True, exist_ok=True)

    integrated_dir = project_dir / "integrated"
    if integrated_dir.exists():
        shutil.rmtree(integrated_dir)
    if project_mode == "multi_dataset":
        integrated_dir.mkdir(parents=True, exist_ok=True)

    metadata_dir = project_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    for artifact_path in metadata_dir.iterdir():
        if artifact_path.is_file() and artifact_path.name.startswith("run_manifest_"):
            continue
        if artifact_path.is_dir():
            shutil.rmtree(artifact_path)
        else:
            artifact_path.unlink()


def _resolve_config_dataset_source(dataset: dict[str, Any]) -> str:
    """Resolve the configured dataset source path or URL."""
    for key in ("source", "url", "path"):
        value = dataset.get(key)
        if value:
            return str(value)
    raise ValueError(f"Dataset config is missing a source reference: {dataset}")


def _build_generic_multi_dataset_marts(
    dataframe: pd.DataFrame,
    dataset_trend_aggregations: dict[str, str],
    project_root: Path,
) -> dict[str, dict[str, Any]]:
    """Build generic marts for any integrated multi-dataset project."""
    mart_results: dict[str, dict[str, Any]] = {}
    metric_columns = find_integrated_metric_columns(dataframe)

    for metric_column in metric_columns:
        trend_aggregation = dataset_trend_aggregations.get(metric_column, "average")
        mart_results[f"mart_{metric_column}_trend"] = build_integrated_trend_mart(
            dataframe=dataframe,
            dataset_column=metric_column,
            mart_name=f"mart_{metric_column}_trend",
            aggregation=trend_aggregation,
            project_root=project_root,
        )
        mart_results[f"mart_{metric_column}_summary_stats"] = build_summary_stats_mart(
            dataframe=dataframe,
            metric_column=metric_column,
            mart_name=f"mart_{metric_column}_summary_stats",
            project_root=project_root,
        )

    if len(metric_columns) >= 2:
        mart_results["mart_metric_correlation"] = build_integrated_correlation_mart(
            dataframe=dataframe,
            dataset_columns=metric_columns,
            project_root=project_root,
        )

    return mart_results


def _build_climate_specialization_marts(
    dataframe: pd.DataFrame,
    project_root: Path,
) -> dict[str, dict[str, Any]]:
    """Build additional climate-specific marts on top of the generic multi-dataset set."""
    temperature_column = (
        "temperature_anomaly"
        if "temperature_anomaly" in dataframe.columns
        else "temperature"
        if "temperature" in dataframe.columns
        else None
    )
    climate_columns = [
        column
        for column in (temperature_column, "co2", "sea_level", "ice_extent")
        if column is not None
        if column in dataframe.columns
    ]
    if len(climate_columns) < 2:
        return {}

    mart_results = {
        "mart_climate_correlation": build_climate_correlation_mart(
            dataframe=dataframe,
            dataset_columns=climate_columns,
            project_root=project_root,
        )
    }
    if temperature_column is not None:
        mart_results["mart_top_warmest_years"] = build_integrated_top_ranked_mart(
            dataframe=dataframe,
            dataset_column=temperature_column,
            mart_name="mart_top_warmest_years",
            aggregation="average",
            ordering="highest",
            limit=10,
            project_root=project_root,
        )
        mart_results["mart_temperature_by_decade"] = build_integrated_decade_mart(
            dataframe=dataframe,
            dataset_column=temperature_column,
            mart_name="mart_temperature_by_decade",
            aggregation="average",
            project_root=project_root,
        )

    return mart_results


def _write_config_project_final_summary_report(
    normalized_source_name: str,
    question: str,
    results: dict[str, Any],
    project_dir: Path,
    repo_root: Path,
) -> dict[str, Any]:
    """Write a summary report for a config-driven project run."""
    summary = {
        "project_mode": results["project_mode"],
        "source": normalized_source_name,
        "question": question,
        "config_path": results["config_path"],
        "primary_mart": results["question_guidance"]["primary_mart"],
        "primary_kpi": results["question_guidance"]["primary_kpi"],
        "question_focus_reason": results["question_guidance"]["focus_reason"],
        "datasets": [
            {
                "name": dataset["name"],
                "source": dataset["source"],
                "time_column": dataset["time_column"],
                "metric_column": dataset["metric_column"],
                "unit": dataset["unit"],
                "coverage": dataset.get(
                    "coverage", {"min_year": None, "max_year": None}
                ),
                "loader_warnings": dataset.get("loader_warnings", []),
                "raw_snapshot_path": dataset["raw_snapshot_path"],
                "profile_path": dataset["profile_path"],
                "cleaned_data_path": dataset["cleaned_data_path"],
                "cleaning_metadata_path": dataset["cleaning_metadata_path"],
            }
            for dataset in results["datasets"]
        ],
        "mart_paths": results["marts"],
        "metrics_definitions_path": results["metrics_definitions"]["output_path"],
        "anomaly_report_path": results["anomaly_detection_path"],
        "analytics_metadata_path": results["analytics_metadata"]["output_path"],
        "dashboard_suggestions_path": results["dashboard_suggestions"]["output_path"],
    }

    if results["project_mode"] == "multi_dataset":
        summary["integrated_dataset_path"] = results["integrated_dataset"][
            "output_path"
        ]
        summary["dataset_metrics"] = results["integrated_dataset"]["dataset_metrics"]
    else:
        summary.update(
            {
                "profile_path": results["profiling"]["output_path"],
                "cleaned_data_path": results["cleaning"]["output_path"],
                "cleaning_metadata_path": results["cleaning"]["metadata_path"],
                "skipped_marts": results["skipped_marts"],
                "metric_column": results["metric_selection"]["metric_column"],
                "metric_unit": infer_metric_unit(
                    results["metric_selection"]["metric_column"]
                ),
                "metric_selection_mode": results["metric_selection"]["selection_mode"],
                "metric_candidates": [
                    candidate["column"]
                    for candidate in results["metric_selection"]["candidates"][:5]
                ],
                "dimension_column": results["dimension_selection"]["dimension_column"],
                "dimension_selection_mode": results["dimension_selection"][
                    "selection_mode"
                ],
                "dimension_candidates": [
                    candidate["column"]
                    for candidate in results["dimension_selection"]["candidates"][:5]
                ],
                "aggregation_intent": {
                    "aggregation": results["aggregation_intent"]["aggregation"],
                    "ordering": results["aggregation_intent"]["ordering"],
                },
                "aggregation_selection_mode": results["aggregation_intent"][
                    "selection_mode"
                ],
                "primary_kpi": results["kpi_suggestions"]["primary_kpi"],
            }
        )

    summary_path = project_dir / "metadata" / "final_summary_report.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return {"path": summary_path.relative_to(repo_root).as_posix(), "summary": summary}


def _write_config_project_readme(
    normalized_source_name: str,
    question: str,
    results: dict[str, Any],
    mart_results: dict[str, dict[str, Any]],
    project_dir: Path,
    repo_root: Path,
) -> dict[str, str]:
    """Write a project README for a config-driven run."""
    project_title = format_analysis_title(normalized_source_name)
    summary = results["final_summary_report"]["summary"]
    primary_mart_name = _select_primary_mart_name(
        mart_results=mart_results,
        question=question,
        dataset_metrics=summary.get("dataset_metrics"),
    )
    primary_mart = mart_results.get(primary_mart_name) if primary_mart_name else None
    key_result_table = _build_key_result_table(
        primary_mart["dataframe"] if primary_mart is not None else None
    )
    reproduce_command = (
        f'python -m app.main --config "{summary["config_path"]}" --project-root "."'
    )

    if summary["project_mode"] == "multi_dataset":
        dataset_lines = [
            f"- `{dataset['name']}` from `{dataset['source']}` using `{dataset['metric_column']}` over `{dataset['time_column']}`"
            for dataset in summary["datasets"]
        ]
        loader_warning_lines = [
            f"- `{dataset['name']}` loader warning: {warning}"
            for dataset in summary["datasets"]
            for warning in dataset.get("loader_warnings", [])
        ]
        metric_lines = [
            _build_multi_dataset_metric_line(metric)
            for metric in summary["dataset_metrics"]
        ]
        integrated_columns = ["year"] + [
            metric["dataset_name"] for metric in summary["dataset_metrics"]
        ]
        artifact_lines = [
            f"- Integrated dataset: `{summary['integrated_dataset_path']}`",
            f"- Integrated columns: {_format_column_list(integrated_columns)}",
            f"- Metadata: `projects/{normalized_source_name}/metadata/`",
        ]
        if summary["mart_paths"]:
            artifact_lines.append("- Mart tables:")
            artifact_lines.extend(
                [f"  - `{path}`" for path in summary["mart_paths"].values()]
            )
        recommended_bi_lines = (
            _build_climate_bi_page_lines(summary)
            if _is_climate_multi_dataset(summary)
            else []
        )
        coverage_note_lines = _build_data_coverage_note_lines(summary)
        readme_content = (
            "\n".join(
                [
                    f"# {project_title}",
                    "",
                    "## Question",
                    "",
                    question,
                    "",
                    "## Dataset Overview",
                    "",
                    f"- Config-driven project with `{len(summary['datasets'])}` datasets",
                    *dataset_lines,
                    "",
                    "## Dataset Description",
                    "",
                    "- Each configured dataset is ingested separately, cleaned independently, and standardized into `year`, `source_metric_value`, and `dataset_name` inside the optional integration contract.",
                    "- This project uses `integrated/` because it combines multiple datasets on a shared time grain before generating use-case marts.",
                    "- The integrated dataset stores semantically named project metrics rather than generic placeholders, which keeps downstream marts readable in BI tools.",
                    *(
                        [
                            "",
                            "## Source Notes",
                            "",
                            *loader_warning_lines,
                        ]
                        if loader_warning_lines
                        else []
                    ),
                    "",
                    "## Metric Interpretation",
                    "",
                    *metric_lines,
                    "",
                    "## Integrated Dataset",
                    "",
                    f"- Integrated dataset path: `{summary['integrated_dataset_path']}`",
                    f"- Integrated columns: {_format_column_list(integrated_columns)}",
                    "- The integrated layer is aligned on `year` and preserves each project metric as its own column.",
                    "",
                    "## Data Coverage",
                    "",
                    "| Dataset | Min Year | Max Year |",
                    "| --- | --- | --- |",
                    *[
                        "| {} | {} | {} |".format(
                            d["name"],
                            d.get("coverage", {}).get("min_year", "—"),
                            d.get("coverage", {}).get("max_year", "—"),
                        )
                        for d in summary["datasets"]
                    ],
                    *(
                        [
                            "",
                            "Coverage notes:",
                            *coverage_note_lines,
                        ]
                        if coverage_note_lines
                        else []
                    ),
                    "",
                    "## Analysis Results",
                    "",
                    (
                        f"Primary mart: `{primary_mart_name}`"
                        if primary_mart_name is not None
                        else "No mart table was generated for this run."
                    ),
                    *(
                        [
                            _build_question_focus_line(
                                question=question,
                                primary_mart_name=primary_mart_name,
                            )
                        ]
                        if primary_mart_name is not None
                        else []
                    ),
                    "",
                    key_result_table,
                    "",
                    "## Generated Artifacts",
                    "",
                    *artifact_lines,
                    *(
                        [
                            "",
                            "## Recommended BI Pages",
                            "",
                            *recommended_bi_lines,
                        ]
                        if recommended_bi_lines
                        else []
                    ),
                    "",
                    "## Reproducibility",
                    "",
                    "```bash",
                    reproduce_command,
                    "```",
                ]
            ).strip()
            + "\n"
        )
    else:
        profile = _load_profile_metadata(repo_root, summary["profile_path"])
        original_input = summary["datasets"][0]["source"]
        dataset_overview_lines = _build_dataset_overview_lines(profile, original_input)
        dataset_statistics_lines = _build_dataset_statistics_lines(profile)
        dataset_description_lines = _build_dataset_description_lines(
            question=question,
            profile=profile,
            original_input=original_input,
        )
        metric_interpretation_lines = _build_metric_interpretation_lines(
            summary=summary,
            original_input=original_input,
            profile=profile,
        )
        column_summary_table = _build_profile_column_summary_table(profile)
        column_meaning_table = _build_column_meaning_table(
            profile=profile,
            original_input=original_input,
        )
        artifact_lines = _build_project_artifact_lines(
            ingestion_snapshot_path=summary["datasets"][0]["raw_snapshot_path"],
            summary=summary,
        )
        readme_content = (
            "\n".join(
                [
                    f"# {project_title}",
                    "",
                    "## Question",
                    "",
                    question,
                    "",
                    "## Dataset Overview",
                    "",
                    f"- Config-driven project with `{len(summary['datasets'])}` dataset",
                    *dataset_overview_lines,
                    "",
                    column_summary_table,
                    "",
                    "## Dataset Statistics",
                    "",
                    *dataset_statistics_lines,
                    "",
                    "## Dataset Description",
                    "",
                    *dataset_description_lines,
                    "",
                    "## Metric Interpretation",
                    "",
                    *metric_interpretation_lines,
                    "",
                    "## Column Meaning",
                    "",
                    column_meaning_table,
                    "",
                    "## Analysis Results",
                    "",
                    (
                        f"Primary mart: `{primary_mart_name}`"
                        if primary_mart_name is not None
                        else "No mart table was generated for this run."
                    ),
                    "",
                    key_result_table,
                    "",
                    "## Generated Artifacts",
                    "",
                    "This config-driven project follows the base `raw/ -> staging/ -> marts/` flow and does not create `integrated/` because only one dataset is configured.",
                    "",
                    *artifact_lines,
                    "",
                    "## Reproducibility",
                    "",
                    "```bash",
                    reproduce_command,
                    "```",
                    "",
                    "## Supporting Metadata",
                    "",
                    f"- Possible KPIs: {', '.join(results['analytics_metadata'].get('possible_kpis', [])[:5]) or 'none'}",
                    f"- Suggested dimensions: {', '.join(results['analytics_metadata'].get('suggested_dimensions', [])[:5]) or 'none'}",
                ]
            ).strip()
            + "\n"
        )

    readme_path = project_dir / "README.md"
    readme_path.write_text(readme_content, encoding="utf-8")
    return {"path": readme_path.relative_to(repo_root).as_posix()}


def _relative_to_repo(repo_root: Path, path_value: str | Path) -> str:
    """Return a repo-relative path when possible."""
    resolved = Path(path_value)
    try:
        return resolved.relative_to(repo_root).as_posix()
    except ValueError:
        return str(resolved)


def _prefixed_path(project_prefix: str, relative_path: str) -> str:
    """Attach the project prefix to a module-relative artifact path."""
    return f"{project_prefix}/{relative_path}"


def _with_project_prefix(
    payload: dict[str, Any],
    project_prefix: str,
    *path_keys: str,
) -> dict[str, Any]:
    """Return a shallow copy with selected path fields scoped to the project folder."""
    scoped_payload = dict(payload)
    for path_key in path_keys:
        scoped_payload[path_key] = _prefixed_path(
            project_prefix, scoped_payload[path_key]
        )
    return scoped_payload
