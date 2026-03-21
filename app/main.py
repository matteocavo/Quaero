"""CLI entrypoint for running single-dataset or config-driven analytics pipelines."""

from __future__ import annotations

import argparse
import json
import logging

from app.pipeline_runner import (
    run_pipeline as run_dataset_pipeline,
    run_project_from_config,
)


def _configure_logging() -> None:
    """Configure CLI logging once for interactive pipeline runs."""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )


def run_pipeline(
    dataset_path: str,
    source_name: str,
    question: str,
    project_root: str | None = None,
) -> dict:
    """Run the single-dataset pipeline as a reusable module-friendly helper."""
    _configure_logging()
    return run_dataset_pipeline(
        source_path=dataset_path,
        source_name=source_name,
        question=question,
        project_root=project_root,
    )


def run_project_pipeline(
    config_path: str,
    project_root: str | None = None,
) -> dict:
    """Run a config-driven project as a reusable module-friendly helper."""
    _configure_logging()
    return run_project_from_config(
        config_path=config_path,
        project_root=project_root,
    )


def main() -> None:
    """Parse CLI arguments and delegate execution to the pipeline runner."""
    _configure_logging()

    parser = argparse.ArgumentParser(
        description="Run Quaero on a CSV or Parquet input, or on a project config."
    )
    parser.add_argument(
        "source_path",
        nargs="?",
        help="Local path or direct URL to the source CSV or Parquet file.",
    )
    parser.add_argument(
        "--config",
        help="Optional project_config.json path for config-driven project execution.",
    )
    parser.add_argument(
        "--source-name",
        help="Logical source name used for output folders and metadata files.",
    )
    parser.add_argument(
        "--question",
        help="Business question used for KPI and dashboard suggestion stages.",
    )
    parser.add_argument(
        "--metric-column",
        help="Optional numeric column override used for mart generation.",
    )
    parser.add_argument(
        "--dimension-column",
        help="Optional grouping dimension override used for inference fallback.",
    )
    parser.add_argument(
        "--catalog-column",
        help="Optional categorical column used for catalog summary marts.",
    )
    parser.add_argument(
        "--provider-column",
        default="provider",
        help="Provider column name. Defaults to 'provider'.",
    )
    parser.add_argument(
        "--release-year-column",
        default="release_year",
        help="Release year column name. Defaults to 'release_year'.",
    )
    parser.add_argument(
        "--project-root",
        help="Optional project root for output paths. Defaults to the repository root.",
    )

    args = parser.parse_args()
    if bool(args.config) == bool(args.source_path):
        parser.error("Provide either a dataset source_path or --config, but not both.")

    if args.config:
        result = run_project_pipeline(
            config_path=args.config,
            project_root=args.project_root,
        )
    else:
        if not args.source_name or not args.question:
            parser.error(
                "--source-name and --question are required when running a single dataset."
            )
        if (
            any(
                value is not None
                for value in (
                    args.metric_column,
                    args.dimension_column,
                    args.catalog_column,
                )
            )
            or args.provider_column != "provider"
            or args.release_year_column != "release_year"
        ):
            result = run_dataset_pipeline(
                source_path=args.source_path,
                source_name=args.source_name,
                question=args.question,
                metric_column=args.metric_column,
                dimension_column=args.dimension_column,
                catalog_column=args.catalog_column,
                provider_column=args.provider_column,
                release_year_column=args.release_year_column,
                project_root=args.project_root,
            )
        else:
            result = run_pipeline(
                dataset_path=args.source_path,
                source_name=args.source_name,
                question=args.question,
                project_root=args.project_root,
            )
    print(json.dumps(result["final_summary_report"]["summary"], indent=2))


if __name__ == "__main__":
    main()
