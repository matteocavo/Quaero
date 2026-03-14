# Global Economic Indicators Analysis

## Question

How do global GDP, inflation, and unemployment evolve over time and how are they related?

## Dataset Overview

- Config-driven project with `3` datasets
- `gdp` from `https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv` using `gdp` over `year`
- `inflation` from `https://api.worldbank.org/v2/country/all/indicator/FP.CPI.TOTL.ZG?format=json&per_page=20000` using `inflation` over `year`
- `unemployment` from `https://api.worldbank.org/v2/country/all/indicator/SL.UEM.TOTL.ZS?format=json&per_page=20000` using `unemployment` over `year`

## Dataset Description

- Each configured dataset is ingested separately, cleaned independently, and standardized into `year`, `source_metric_value`, and `dataset_name` inside the optional integration contract.
- This project uses `integrated/` because it combines multiple datasets on a shared time grain before generating use-case marts.
- The integrated dataset stores semantically named project metrics rather than generic placeholders, which keeps downstream marts readable in BI tools.

## Metric Interpretation

- `gdp` comes from `gdp` with inferred unit `current_usd`.
- `inflation` comes from `inflation` with inferred unit `percent`.
- `unemployment` comes from `unemployment` with inferred unit `percent`.

## Integrated Dataset

- Integrated dataset path: `projects/global_economic_indicators/integrated/master_dataset.parquet`
- Integrated columns: `year`, `gdp`, `inflation`, `unemployment`
- The integrated layer is aligned on `year` and preserves each project metric as its own column.

## Data Coverage

| Dataset | Min Year | Max Year |
| --- | --- | --- |
| gdp | 1960 | 2023 |
| inflation | 1981 | 2024 |
| unemployment | 1991 | 2025 |

Coverage notes:
- The integrated dataset keeps the union of available years across sources, so the project timeline begins at `1960`.
- `inflation` has no source data before `1981`, so `inflation` marts begin at `1981`.
- `unemployment` has no source data before `1991`, so `unemployment` marts begin at `1991`.

## Analysis Results

Primary mart: `mart_gdp_trend`
- The question emphasizes change over time, so `mart_gdp_trend` is treated as the primary narrative mart.

| year | record_count | avg_gdp |
| --- | --- | --- |
| 1960 | 1 | 1364504252362.649 |
| 1961 | 1 | 1439319473881.0146 |
| 1962 | 1 | 1542844506884.743 |
| 1963 | 1 | 1664976959207.4958 |
| 1964 | 1 | 1827784855167.609 |

## Generated Artifacts

- Integrated dataset: `projects/global_economic_indicators/integrated/master_dataset.parquet`
- Integrated columns: `year`, `gdp`, `inflation`, `unemployment`
- Metadata: `projects/global_economic_indicators/metadata/`
- Mart tables:
  - `projects/global_economic_indicators/marts/mart_gdp_trend.parquet`
  - `projects/global_economic_indicators/marts/mart_gdp_summary_stats.parquet`
  - `projects/global_economic_indicators/marts/mart_inflation_trend.parquet`
  - `projects/global_economic_indicators/marts/mart_inflation_summary_stats.parquet`
  - `projects/global_economic_indicators/marts/mart_unemployment_trend.parquet`
  - `projects/global_economic_indicators/marts/mart_unemployment_summary_stats.parquet`
  - `projects/global_economic_indicators/marts/mart_metric_correlation.parquet`

## Reproducibility

```bash
python -m app.main --config "projects/global_economic_indicators/project_config.json" --project-root "."
```
