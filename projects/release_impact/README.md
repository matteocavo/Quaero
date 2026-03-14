# Release Impact Analysis

## Question

Which release years generate the strongest average streams?

## Dataset Overview

- Dataset source: `sample_data/release_impact_sample.csv`
- Dataset name: `release_impact`
- Rows processed: `5`
- Number of columns: `4`
- Detected numeric columns: `release_year`, `streams`
- Detected categorical columns: `provider`, `catalog`
- Detected datetime columns: none

| Column | Type | Null % | Unique Values | Example Values |
| ------ | ---- | ------ | ------------- | -------------- |
| release_year | integer | 0% | 5 | 2023, 2021, 2022, 2024, 2025 |
| provider | string | 0% | 3 | Alpha, Alpha, Beta, Gamma, Alpha |
| catalog | string | 0% | 3 | Rock, Pop, Pop, Rock, Jazz |
| streams | integer | 0% | 5 | 210, 120, 180, 450, 190 |

## Dataset Statistics

- Total rows: `5`
- Total columns: `4`
- Numeric columns count: `2`
- Categorical columns count: `2`
- Datetime columns count: `0`

## Dataset Description

- This dataset includes `5` rows across `4` columns.
- The analysis combines numeric measures such as `release_year`, `streams` with grouping columns like `provider`, `catalog`.
- The current project answers the question: "Which release years generate the strongest average streams?".

## Metric Interpretation

- Metric column: `streams`
- Dimension column: `release_year`
- Aggregation: `average`
- Unit: `unknown`
- Explanation: This analysis uses the `streams` column and the `average` aggregation to rank the inferred dimension in the generated marts.

## Column Meaning

| Column | Meaning |
| ------ | ------- |
| provider | Provider or platform associated with the record |
| catalog | Category or catalog grouping used for analysis |

## Analysis Results

Primary mart: `mart_question_answer`
- `mart_question_answer` is treated as the primary narrative mart for the stated question.

| release_year | record_count | avg_streams |
| --- | --- | --- |
| 2024 | 1 | 450.0 |
| 2023 | 1 | 210.0 |
| 2025 | 1 | 190.0 |
| 2022 | 1 | 180.0 |
| 2021 | 1 | 120.0 |

## Generated Artifacts

Analytical marts live under `marts/` and can be loaded directly into BI tools for dashboarding.

- Raw snapshot: `projects/release_impact/raw/snapshot_2026_03_14_144138.parquet`
- Clean dataset: `projects/release_impact/staging/staging_2026_03_14_144138_9a3dae84.parquet`
- Metadata: `projects/release_impact/metadata/`
- Mart tables:
  - `projects/release_impact/marts/mart_question_answer.parquet`
  - `projects/release_impact/marts/mart_top_entities.parquet`
  - `projects/release_impact/marts/mart_release_impact.parquet`
  - `projects/release_impact/marts/mart_provider_performance.parquet`
  - `projects/release_impact/marts/mart_catalog_summary.parquet`
  - `projects/release_impact/marts/mart_distribution.parquet`
  - `projects/release_impact/marts/mart_summary_stats.parquet`
  - `projects/release_impact/marts/mart_time_trend.parquet`

## Reproducibility

```bash
python -m app.main "sample_data/release_impact_sample.csv" --source-name "release_impact" --question "Which release years generate the strongest average streams?" --project-root "."
```

## Supporting Metadata

- Possible KPIs: total_streams, avg_streams
- Suggested dimensions: provider, catalog
