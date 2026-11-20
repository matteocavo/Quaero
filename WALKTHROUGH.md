# Walkthrough: your first Quaero run

A short, hands-on path for first-time users: run the Python pipeline on a public
CSV, check the answer with pandas, read the quality score and the lineage, then
repeat with your own file.

This page does not repeat setup, CLI options or the full output list. Those live
in [README.md](README.md) and [REPRODUCIBILITY.md](REPRODUCIBILITY.md).

## Two ways to run Quaero

| | Browser-only demo (GitHub Pages) | Local Python pipeline |
| --- | --- | --- |
| Runs on | Your browser | Your machine (Python) |
| Data | Bundled illustrative data | Any CSV you point it at |
| Writes files | No | Yes, under `projects/<source-name>/` |
| Use it to | See what the outputs look like | Analyse your own data |

Everything below uses the local pipeline.

## 1. Setup

Python 3.11+ is recommended (see README). From the repository root:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Run it on a public dataset

The example uses the
[datasets/airport-codes](https://github.com/datasets/airport-codes) CSV
(about 86,000 rows, 13 columns). Its repository lists the data as ODC-PDDL.
The underlying IATA/ICAO code lists may carry their own terms, so check them
before redistributing outputs. The file is downloaded at run time and is not
stored in this repository.

```bash
python -m app.main \
  "https://raw.githubusercontent.com/datasets/airport-codes/main/data/airport-codes.csv" \
  --source-name airports \
  --question "Which airport types have the highest average elevation_ft?" \
  --project-root .
```

(On Windows `cmd`, put the command on one line.) The run took about 11 seconds
in the test environment. Outputs go to `projects/airports/`.

## 3. Check the answer with pandas

Open `projects/airports/marts/mart_question_answer.parquet` and compare it with
a direct pandas calculation:

```python
import pandas as pd

url = "https://raw.githubusercontent.com/datasets/airport-codes/main/data/airport-codes.csv"
df = pd.read_csv(url)

mart = pd.read_parquet("projects/airports/marts/mart_question_answer.parquet")
check = df.groupby("type")["elevation_ft"].mean().sort_values(ascending=False)

print(mart.head(3))
print(check.head(3))
```

Values observed on 2026-11-20 (the upstream file can change, so later runs may differ):

| type | `record_count` in mart | mean `elevation_ft` |
| --- | --- | --- |
| balloonport | 62 | 2124.1 |
| small_airport | 42,788 | 1357.6 |
| closed | 13,657 | 1291.9 |

The means match pandas. `record_count` is the number of rows of that type,
including rows with no elevation. The mean uses only rows that have one. For
balloonport that is 47 of 62 rows, so read the average with the missing values in
mind. About 17% of `elevation_ft` values are empty.

## 4. Read the data quality score

`projects/airports/metadata/dq_score_airports.json` holds one score per column
and an overall score (80.7 for this run). Each column score is a weighted sum
(implemented in `pipelines/dq_scorer.py`):

```
dq_score = 0.40 * completeness + 0.20 * uniqueness + 0.40 * format_score
```

- **completeness**: share of non-null values.
- **uniqueness**: distinct values divided by non-null values.
- **format_score**: 100 for numeric columns; for text columns, reduced when
  values are not strings.

How to read it:

- Low completeness is the main signal. `icao_code` (12.4%) and `iata_code`
  (10.5%) are mostly empty, which is expected because most heliports and small
  fields have no such code.
- Low uniqueness is not a defect for categorical columns. `type` has 7 distinct
  values over 86,000 rows, so its uniqueness is 0.0 and its score is 80.0. Treat
  the score as a prompt to look, not a verdict.
- The score is a heuristic. It does not check whether values are correct.

One thing to watch: `continent` shows completeness 53.9%, but in the source the
code `NA` means North America. Default pandas CSV parsing reads `NA` as missing,
so the score understates this column (pandas reports 46.1% missing; with
`keep_default_na=False` it is 0%). Check how your own file spells missing values.

## 5. Read the lineage

Open `projects/airports/metadata/lineage.html` in a browser, or read
`lineage.json`. It lists columns as nodes (53 for this run) and the steps between
layers as edges (29), for example `raw::type` to `staging::type` as a
`passthrough`. Use it to trace which source column a mart column came from.
Links between staging and mart columns are inferred from names, so confirm them
against the mart SQL files in `projects/airports/marts/`.

## 6. Other generated files

- `marts/*.parquet` and matching `.sql`: the five analytical tables
  (`mart_question_answer`, `mart_top_entities`, `mart_catalog_summary`,
  `mart_distribution`, `mart_summary_stats`).
- `metadata/cleaning_summary_airports.json`: row and column counts, duplicates,
  rows with nulls.
- `metrics/metrics.yml`: auto-generated metric names such as `avg_elevation_ft`.
  Descriptions are placeholders to edit.
- `metadata/anomaly_report_airports.json`: anomaly checks.
- `README.md` inside the project folder: a summary of the run.

## 7. Use your own CSV

```bash
python -m app.main "path/to/your_file.csv" \
  --source-name my_data \
  --question "Which <dimension> has the highest average <numeric column>?" \
  --project-root .
```

- Name real column names in the question so Quaero can map them.
- If it picks the wrong column, set `--metric-column` and/or `--dimension-column`.
- Then repeat steps 3 to 5 on your own output: compare one mart with pandas,
  read the quality scores, and check the lineage.

Keep private data out of the repository. Outputs land in `projects/<source-name>/`.
The repository's `.gitignore` already excludes most generated folders (`raw/`,
`staging/`, `marts/`) and several metadata files, but not all of them, so check
`git status` before committing anything from a run on private data.
