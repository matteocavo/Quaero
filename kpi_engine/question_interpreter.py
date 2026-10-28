"""Transparent, deterministic interpretation of an analytical question.

The pipeline answers one kind of question: *one* measure, grouped by *one* column, with *one* of three
aggregations (average, sum, count), listed highest or lowest first. This module reads a question against the
columns of a dataset and says exactly how it will be answered, or why it cannot be:

* ``ready``        the question maps to a supported analysis; ``issues`` may carry warnings and notes.
* ``needs_input``  something is missing or ambiguous (no measure, no grouping, a vague question). Nothing is guessed.
* ``unsupported``  the question asks for something the pipeline does not do (median, per month, a filter, a
                   comparison, two measures, a "why"...). It is refused with the unsupported part and a rewording.

Rules are plain keyword lists in English and Italian, listed below; there is no language model and no scoring that
pretends to be confidence. Explicit choices (``metric_column``, ``dimension_column``, ``aggregation``,
``time_column``) always win over anything read from the wording.

Nothing here reads or writes files. ``interpret_question`` is a pure function of its arguments.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_numeric_dtype,
)

from kpi_engine.metric_inference import (
    NUMERIC_DIMENSION_HINTS,
    score_dimension_candidates,
    score_metric_candidates,
)

SUPPORTED_AGGREGATIONS = ("average", "sum", "count")

# --------------------------------------------------------------------------------------------- word lists
# All words are compared without accents and in lower case.
_SUM = {
    "total",
    "totals",
    "sum",
    "totale",
    "totali",
    "somma",
    "complessivo",
    "complessiva",
    "cumulative",
}
_AVERAGE = {"average", "avg", "mean", "media", "medio", "mediamente"}
_AVERAGE_PHRASES = ("on average", "in media")
_COUNT_PHRASES = (
    "how many",
    "number of",
    "count of",
    "numero di",
    "quanti",
    "quante",
    "conteggio",
    "conta",
)
_COUNT_WORDS = {"count", "quanti", "quante", "conteggio"}
_MOST = {"most", "largest", "biggest", "more", "maggior", "maggiore", "maggiori"}
_LOWEST = {
    "lowest",
    "bottom",
    "least",
    "fewest",
    "smallest",
    "worst",
    "basso",
    "bassa",
    "bassi",
    "basse",
    "minore",
    "minori",
    "peggiore",
    "peggiori",
}
_MINMAX = {"maximum", "max", "minimum", "min", "massimo", "massima", "minimo", "minima"}
_UNSUPPORTED_AGGREGATION = {
    "median": "median",
    "mediana": "median",
    "mode": "mode",
    "moda": "mode",
    "percentile": "percentile",
    "percentili": "percentile",
    "stddev": "standard deviation",
    "variance": "variance",
    "varianza": "variance",
    "deviazione": "standard deviation",
    "unique": "distinct count",
    "distinct": "distinct count",
    "univoci": "distinct count",
    "univoche": "distinct count",
    "distinti": "distinct count",
    "distinte": "distinct count",
}
_CAUSAL = {
    "why",
    "reason",
    "reasons",
    "cause",
    "causes",
    "caused",
    "because",
    "driver",
    "drivers",
    "explain",
    "explains",
    "drive",
    "drives",
    "driven",
    "influence",
    "influences",
    "influenza",
    "perche",
    "motivo",
    "motivi",
    "causa",
    "spiega",
}
_FORECAST = {
    "predict",
    "predicted",
    "prediction",
    "forecast",
    "projection",
    "prevedere",
    "previsione",
    "previsioni",
    "prevedi",
}
_RELATIONSHIP = {
    "correlation",
    "correlate",
    "correlated",
    "relationship",
    "regression",
    "correlazione",
    "relazione",
}
_COMPARE = {"compare", "comparison", "versus", "vs", "confronta", "confronto", "paragona"}

_GRAINS = {
    "year": {"year", "years", "yearly", "annual", "annually", "anno", "anni", "annuale", "annuali"},
    "quarter": {"quarter", "quarters", "quarterly", "trimestre", "trimestri", "trimestrale"},
    "month": {"month", "months", "monthly", "mese", "mesi", "mensile", "mensili"},
    "week": {"week", "weeks", "weekly", "settimana", "settimane", "settimanale"},
    "day": {"day", "days", "daily", "giorno", "giorni", "giornaliero", "giornaliera"},
}
# Words that, before a grain word, mean "grouped per that period".
_GROUP_LEAD = {"by", "per", "each", "every", "ogni", "al", "a"}
_GRAIN_STANDALONE = {
    "monthly",
    "yearly",
    "weekly",
    "daily",
    "quarterly",
    "annual",
    "annually",
    "mensile",
    "mensili",
    "annuale",
    "annuali",
    "settimanale",
    "giornaliero",
    "giornaliera",
    "trimestrale",
}

_RELATIVE_TIME = (
    "last year",
    "last month",
    "last quarter",
    "last week",
    "this year",
    "this month",
    "past year",
    "past month",
    "yesterday",
    "anno scorso",
    "mese scorso",
    "ultimo anno",
    "ultimo mese",
    "quest anno",
    "questo anno",
    "questo mese",
    "ieri",
)
_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")
_TOP_N = re.compile(r"\b(?:top|bottom|first|last|primi|primo|ultimi|ultimo)\s+\d+\b")
_ID_WORDS = {"id", "ids", "key", "code", "uuid", "guid", "index", "idx"}
# Numeric columns named like these hold a place, a period or a code: they group rows, they are not measured.
_DIM_WORDS = set(NUMERIC_DIMENSION_HINTS) | {
    "anno",
    "mese",
    "regione",
    "citta",
    "paese",
    "zona",
    "giorno",
    "trimestre",
    "settimana",
    "week",
    "quarter",
    "day",
}
_IT_MARKERS = {
    "quale",
    "quali",
    "qual",
    "quanti",
    "quante",
    "ha",
    "hanno",
    "le",
    "il",
    "lo",
    "la",
    "gli",
    "di",
    "della",
    "delle",
    "dei",
    "per",
    "che",
    "piu",
    "alto",
    "alte",
    "alti",
    "alta",
    "totale",
    "totali",
    "media",
    "numero",
    "nel",
    "nella",
    "nei",
    "tra",
    "fra",
}


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()


def _words(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _fold(text).replace("_", " "))


def _expanded(words: list[str]) -> set[str]:
    """The words plus simple singular forms, so "orders" also matches a column called order."""
    out = set(words)
    for w in words:
        if w.endswith("ies") and len(w) > 3:
            out.add(w[:-3] + "y")
        if w.endswith("s") and len(w) > 1:
            out.add(w[:-1])
    return out


def _has_phrase(joined: str, phrases: tuple[str, ...]) -> bool:
    return any(f" {_fold(p)} " in joined for p in phrases)


def _name_words(column: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", _fold(str(column)).replace("_", " ")))


def _is_identifier(name: str, series: pd.Series) -> bool:
    """A numeric column that is a key (customer_id, order_id, PULocationID), not a quantity."""
    if not is_numeric_dtype(series) or is_bool_dtype(series):
        return False
    words = _name_words(name)
    named_like_key = bool(words & _ID_WORDS) or any(w.endswith("id") and len(w) > 3 for w in words)
    if not named_like_key:
        return False
    if is_integer_dtype(series):
        return True
    non_null = series.dropna()
    return len(non_null) > 0 and bool((non_null == non_null.round()).all())


def _is_constant(series: pd.Series) -> bool:
    return int(series.dropna().nunique()) <= 1


def _issue(code: str, severity: str, message: str, suggestion: str | None = None) -> dict[str, Any]:
    return {"code": code, "severity": severity, "message": message, "suggestion": suggestion}


def _sel(column: str | None, origin: str | None) -> dict[str, Any]:
    return {"column": column, "origin": origin}


def _val(value: str | None, origin: str | None) -> dict[str, Any]:
    return {"value": value, "origin": origin}


# ------------------------------------------------------------------------------------------------ main
def interpret_question(
    question: str,
    dataframe: pd.DataFrame,
    *,
    metric_column: str | None = None,
    dimension_column: str | None = None,
    aggregation: str | None = None,
    time_column: str | None = None,
) -> dict[str, Any]:
    """Say how *question* will be answered on *dataframe* (cleaned, normalized column names), or why not."""
    text = (question or "").strip()
    words = _words(text)
    wordset = _expanded(words)
    joined = " " + " ".join(words) + " "
    language = "it" if len(set(words) & _IT_MARKERS) >= 2 else "en"
    columns = [str(c) for c in dataframe.columns]
    column_words = set().union(*(_name_words(c) for c in columns)) if columns else set()
    issues: list[dict[str, Any]] = []

    def say(code: str, severity: str, message: str, suggestion: str | None = None) -> None:
        issues.append(_issue(code, severity, message, suggestion))

    if not text:
        say(
            "empty_question",
            "needs_input",
            "The question is empty.",
            "Write what you want to know, for example "
            "'Which region has the highest total revenue?'.",
        )
        return _result(
            text,
            language,
            issues,
            _sel(None, None),
            _sel(None, None),
            _val(None, None),
            _val("highest", "assumed_default"),
            _sel(None, None),
            None,
        )

    # ---- explicit selections must exist (the pipeline reports the same errors; here they are readable)
    for label, value in (
        ("metric", metric_column),
        ("dimension", dimension_column),
        ("time", time_column),
    ):
        if value and value not in columns:
            say(
                f"unknown_{label}_column",
                "unsupported",
                f"The {label} column '{value}' is not in the dataset.",
                "Choose one of: " + ", ".join(columns[:12]) + ("..." if len(columns) > 12 else ""),
            )
    if (
        metric_column
        and metric_column in columns
        and not (
            is_numeric_dtype(dataframe[metric_column])
            and not is_bool_dtype(dataframe[metric_column])
        )
    ):
        say(
            "metric_not_numeric",
            "unsupported",
            f"The measure '{metric_column}' is not numeric, so it cannot be added up or averaged.",
            "Choose a numeric column, or choose Count to count rows.",
        )
    if aggregation and aggregation not in SUPPORTED_AGGREGATIONS:
        say(
            "aggregation_not_supported",
            "unsupported",
            f"The aggregation '{aggregation}' is not available.",
            "Choose average, sum or count.",
        )
    if any(i["severity"] == "unsupported" for i in issues):
        return _result(
            text,
            language,
            issues,
            _sel(metric_column, "user_provided" if metric_column else None),
            _sel(dimension_column, "user_provided" if dimension_column else None),
            _val(aggregation, "user_provided" if aggregation else None),
            {"value": "highest", "origin": "assumed_default"},
            _sel(time_column, None),
            None,
        )

    numeric = [
        c for c in columns if is_numeric_dtype(dataframe[c]) and not is_bool_dtype(dataframe[c])
    ]
    usable_numeric = [
        c for c in numeric if not _is_identifier(c, dataframe[c]) and not _is_constant(dataframe[c])
    ]

    # ---- 1. requests the pipeline cannot answer ------------------------------------------------------
    def present(vocabulary: set[str]) -> set[str]:
        return {w for w in set(words) & vocabulary if w not in column_words}

    for word in sorted(present(set(_UNSUPPORTED_AGGREGATION))):
        say(
            "aggregation_not_supported",
            "unsupported",
            f"'{word}' asks for a {_UNSUPPORTED_AGGREGATION[word]}, which Quaero does not calculate. "
            "Only average, sum and count are available.",
            "Ask for the total, the average or the number of records instead.",
        )
    if present(_CAUSAL):
        say(
            "causal_question",
            "unsupported",
            "A question about why something happens cannot be answered: Quaero only describes the data "
            "(totals, averages, counts).",
            "Ask what the data shows, for example 'Which department has the highest average salary?'.",
        )
    if present(_FORECAST):
        say(
            "forecast_request",
            "unsupported",
            "Forecasts and predictions are not supported.",
            "Ask about the data you have, for example a total or an average per group.",
        )
    if present(_RELATIONSHIP):
        say(
            "relationship_request",
            "unsupported",
            "Correlations and relationships between columns are not calculated.",
            "Ask for one measure by one grouping column.",
        )
    question_years = set(_YEAR.findall(_fold(text))) - column_words
    if present(_COMPARE) or ("between" in words and not question_years):
        say(
            "comparison_request",
            "unsupported",
            "Comparing specific values (for example 'North versus South') is not supported: the analysis always "
            "covers every group.",
            "Group by the column that holds those values and read the rows you need.",
        )

    if question_years or _has_phrase(joined, _RELATIVE_TIME):
        say(
            "period_filter",
            "unsupported",
            "The question limits the analysis to a period (a year or 'last month'), but Quaero cannot filter by "
            "period: the result would cover all the data.",
            "Upload only the period you need, or group by a year column to compare years.",
        )

    value_hit = _value_filter(dataframe, wordset, column_words)
    if value_hit is not None:
        column, value = value_hit
        say(
            "value_filter",
            "unsupported",
            f"The question restricts the analysis to {column} = '{value}', but Quaero cannot filter on values: the "
            "result would cover every value.",
            f"Group by {column} to see all its values, or upload only the rows you need.",
        )

    # ---- 2. aggregation --------------------------------------------------------------------------------
    named_numeric = [
        c
        for c in usable_numeric
        if _name_words(c) <= wordset and not (_name_words(c) & _DIM_WORDS) and c != dimension_column
    ]
    has_sum = bool(set(words) & _SUM)
    has_avg = bool(set(words) & _AVERAGE) or _has_phrase(joined, _AVERAGE_PHRASES)
    has_count = _has_phrase(joined, _COUNT_PHRASES) or bool(set(words) & _COUNT_WORDS)
    has_most = bool(wordset & _MOST) or "fewest" in words
    minmax = present(_MINMAX)
    explicit_ordering_word = bool(
        set(words) & ({"highest", "lowest", "top", "bottom", "best", "worst"} | _LOWEST)
    )
    aggregation_value: str | None = None
    aggregation_origin: str | None = None
    metric_noun_named = bool(named_numeric)

    if has_sum and has_avg:
        say(
            "multiple_aggregations",
            "unsupported",
            "The question asks for both a total and an average. Quaero answers one aggregation at a time.",
            "Run it twice: once for the total and once for the average.",
        )
    elif aggregation:
        aggregation_value, aggregation_origin = aggregation, "user_provided"
    elif has_sum:
        aggregation_value, aggregation_origin = "sum", "auto_inferred"
    elif has_avg:
        aggregation_value, aggregation_origin = "average", "auto_inferred"
    elif has_count or has_most:
        # "how many orders", "most titles": rows are counted. "most revenue", "number of streams": the named
        # numeric column is added up.
        if metric_noun_named:
            aggregation_value, aggregation_origin = "sum", "auto_inferred"
            say(
                "count_read_as_total",
                "note",
                f"'{named_numeric[0]}' is a numeric column, so '{'how many' if has_count else 'most'}' is read as "
                f"the total of {named_numeric[0]}, not a count of rows.",
                "Choose Count in the aggregation menu if you want the number of rows.",
            )
        else:
            aggregation_value, aggregation_origin = "count", "auto_inferred"

    if minmax and not (aggregation_value or explicit_ordering_word):
        say(
            "aggregation_not_supported",
            "unsupported",
            f"'{min(minmax)}' asks for the single largest or smallest value, which Quaero does not "
            "calculate. Only average, sum and count are available.",
            "Ask for the group with the highest or lowest total or average instead.",
        )

    ordering_value = (
        "lowest"
        if (set(words) & _LOWEST) or (minmax & {"minimum", "min", "minimo", "minima"})
        else "highest"
    )
    ordering_origin = "auto_inferred" if explicit_ordering_word or minmax else "assumed_default"

    # ---- 3. time grouping asked in words ----------------------------------------------------------------
    grain = _requested_grain(words)

    # ---- 4. dimension --------------------------------------------------------------------------------------
    metric_for_scoring = (
        metric_column if metric_column else (named_numeric[0] if len(named_numeric) == 1 else None)
    )
    dimension: dict[str, Any] = _sel(None, None)
    dim_signal = False
    if dimension_column:
        dimension = _sel(dimension_column, "user_provided")
        dim_signal = True
    else:
        grain_columns = [
            c
            for c in columns
            if grain and _name_words(c) & _GRAINS[grain] and c != metric_for_scoring
        ]
        if grain and grain_columns:
            dimension = _sel(grain_columns[0], "auto_inferred")
            dim_signal = True
        elif grain:
            say(
                "time_grouping",
                "unsupported",
                f"The question groups by {grain}, but the dataset has no {grain} column and Quaero groups by the exact "
                f"values of a date column, not by {grain}.",
                f"Add a {grain} column to your file, or group by another column.",
            )
        else:
            candidates = [
                c
                for c in score_dimension_candidates(
                    text, dataframe, metric_column=metric_for_scoring
                )
                if not _is_constant(dataframe[c["column"]])
            ]
            if candidates:
                top, second = candidates[0], candidates[1] if len(candidates) > 1 else None
                score = top["score"]
                signal = score[0] > 0 or score[1] > 0 or score[2] > 0
                unique = second is None or score > second["score"]
                if len(candidates) == 1 and not signal:
                    dimension, dim_signal = _sel(top["column"], "auto_inferred"), False
                elif signal and unique:
                    dimension, dim_signal = _sel(top["column"], "auto_inferred"), True
                elif signal:
                    say(
                        "dimension_ambiguous",
                        "needs_input",
                        "Several columns could be the grouping: "
                        + ", ".join(c["column"] for c in candidates[:5])
                        + ".",
                        "Name the column in the question ('... by region') or choose it in the grouping menu.",
                    )
                else:
                    say(
                        "dimension_unclear",
                        "needs_input",
                        "The question does not say what to group by. Candidate columns: "
                        + ", ".join(c["column"] for c in candidates[:5])
                        + ".",
                        "Say it in the question ('... by region') or choose the grouping column.",
                    )
            else:
                say(
                    "no_dimension",
                    "needs_input",
                    "There is no column to group by.",
                    "Add a text, date or code column to your file.",
                )

    # ---- 5. metric ----------------------------------------------------------------------------------------------
    metric: dict[str, Any] = _sel(None, None)
    if aggregation_value == "count":
        metric = _sel("record_count", aggregation_origin)
        if metric_column:
            say(
                "metric_not_used_for_count",
                "warning",
                f"Counting rows does not use {metric_column}; the result is the number of records per group.",
                None,
            )
    elif metric_column:
        metric = _sel(metric_column, "user_provided")
        if metric_column in columns and _is_constant(dataframe[metric_column]):
            say(
                "metric_constant",
                "warning",
                f"'{metric_column}' has the same value (or none) in every row.",
                "Pick a column that varies.",
            )
        elif metric_column in columns and _is_identifier(metric_column, dataframe[metric_column]):
            say(
                "metric_identifier",
                "warning",
                f"'{metric_column}' looks like an identifier, not a quantity.",
                "Pick a numeric column that is an amount or a measure.",
            )
    else:
        measure_named = [c for c in named_numeric if c != dimension["column"]]
        if len(measure_named) >= 2:
            say(
                "multiple_metrics",
                "unsupported",
                "The question names more than one measure ("
                + ", ".join(measure_named[:4])
                + "). Quaero answers one "
                "measure at a time.",
                f"Ask once per measure, for example for {measure_named[0]} and then for {measure_named[1]}.",
            )
        elif len(measure_named) == 1:
            metric = _sel(measure_named[0], "auto_inferred")
        else:
            scored = [
                c for c in score_metric_candidates(text, dataframe) if c["column"] in usable_numeric
            ]
            if not scored:
                say(
                    "no_metric",
                    "needs_input",
                    "There is no numeric column that can be measured (identifier, constant and empty columns are "
                    "not used).",
                    "Add a numeric column, or ask how many records there are per group.",
                )
            else:
                top, second = scored[0], scored[1] if len(scored) > 1 else None
                signal = top["score"][0] > 0 or top["score"][1] > 0
                unique = second is None or top["score"] > second["score"]
                if signal and unique or len(scored) == 1 and dim_signal:
                    metric = _sel(top["column"], "auto_inferred")
                else:
                    say(
                        "metric_unclear",
                        "needs_input",
                        "The question does not say what to measure. Numeric columns: "
                        + ", ".join(c["column"] for c in scored[:5])
                        + ".",
                        "Name the measure ('... average revenue ...') or choose it in the measure menu.",
                    )
    if metric_column and aggregation_value != "count":
        extra = [c for c in named_numeric if c != metric_column and c != dimension["column"]]
        if extra:
            say(
                "wording_not_applied",
                "warning",
                f"The question also mentions {', '.join(extra[:3])}, but {metric_column} is the chosen measure; only "
                f"{metric_column} is used.",
                None,
            )

    # ---- 6. aggregation fallback and wording conflicts with explicit choices ---------------------------------------------
    if aggregation_value is None and not any(i["code"] == "multiple_aggregations" for i in issues):
        aggregation_value, aggregation_origin = "average", "assumed_default"
        say(
            "aggregation_assumed",
            "warning",
            "The question does not say total, average or how many, so the average is assumed.",
            "Say 'total', 'average' or 'how many', or choose the aggregation yourself.",
        )
        if metric["column"] == "record_count":
            metric = _sel(metric["column"], "assumed_default")
    if grain and dimension_column and dimension_column in columns:
        if not (_name_words(dimension_column) & _GRAINS[grain]):
            if is_datetime64_any_dtype(dataframe[dimension_column]):
                grouped_by = f"the exact values of {dimension_column}"
            else:
                grouped_by = dimension_column
            say(
                "time_grouping_not_applied",
                "warning",
                f"The question groups by {grain}, but the analysis groups by {grouped_by} (the column you chose).",
                None,
            )
    if _TOP_N.search(_fold(text)):
        say(
            "top_n_not_applied",
            "note",
            "The result lists every group in order; the 'top N' limit is not applied.",
            None,
        )

    # ---- 7. time column of the trend mart -----------------------------------------------------------------------------
    time = _sel(None, None)
    time_note: str | None = None
    if time_column:
        time = _sel(time_column, "user_provided")
    else:
        from pipelines.mart_builder import find_time_column

        found = find_time_column(dataframe)
        if found:
            time = _sel(found, "auto_inferred")
    if (
        dimension["column"]
        and dimension["column"] in columns
        and is_datetime64_any_dtype(dataframe[dimension["column"]])
    ):
        time_note = "exact_values"
        say(
            "exact_dates",
            "note",
            f"Groups are the exact values of {dimension['column']}, not periods (no month or year grouping).",
            None,
        )

    return _result(
        text,
        language,
        issues,
        metric,
        dimension,
        _val(aggregation_value, aggregation_origin),
        {"value": ordering_value, "origin": ordering_origin},
        time,
        time_note,
    )


# ---------------------------------------------------------------------------------------------- helpers
def _requested_grain(words: list[str]) -> str | None:
    for i, word in enumerate(words):
        for grain, vocabulary in _GRAINS.items():
            if word in vocabulary:
                if word in _GRAIN_STANDALONE:
                    return grain
                if i > 0 and words[i - 1] in _GROUP_LEAD:
                    return grain
    return None


def _value_filter(
    dataframe: pd.DataFrame, wordset: set[str], column_words: set[str]
) -> tuple[str, str] | None:
    """A text value of a column (for example 'North' in region) named in the question means a filter on it."""
    for column in dataframe.columns:
        series = dataframe[column]
        # Text columns: object (pandas 2), str (pandas 3), string and category dtypes.
        if is_numeric_dtype(series) or is_bool_dtype(series) or is_datetime64_any_dtype(series):
            continue
        if series.nunique(dropna=True) > 200:
            continue
        for raw in series.dropna().unique():
            value = str(raw).strip()
            folded = _words(value)
            if not folded or len(value) < 3 or value.isdigit():
                continue
            if len(folded) == 1 and (
                folded[0] in column_words or folded[0] in {"the", "and", "all", "any"}
            ):
                continue  # the word is also a column name ("sales"), so it names the measure, not a filter
            if all(w in wordset for w in folded):
                return str(column), value
    return None


def _describe(
    metric: dict[str, Any],
    dimension: dict[str, Any],
    aggregation: dict[str, Any],
    ordering: dict[str, Any],
) -> str | None:
    if not (metric["column"] and dimension["column"] and aggregation["value"]):
        return None
    what = {
        "average": f"Average of {metric['column']}",
        "sum": f"Total of {metric['column']}",
        "count": "Number of records",
    }[aggregation["value"]]
    return f"{what} for each {dimension['column']}, {ordering['value']} first."


def _suggestions(
    metric: dict[str, Any], dimension: dict[str, Any], aggregation: dict[str, Any], language: str
) -> list[str]:
    m, d, a = metric["column"], dimension["column"], aggregation["value"]
    if not d:
        return []
    if language == "it":
        if a == "count" or not m or m == "record_count":
            return [f"Quanti record ci sono per {d}?"]
        return [f"Qual è il totale di {m} per {d}?", f"Qual è la media di {m} per {d}?"]
    if a == "count" or not m or m == "record_count":
        return [f"How many records are there per {d}?"]
    return [f"Which {d} has the highest total {m}?", f"Which {d} has the highest average {m}?"]


def _result(
    text: str,
    language: str,
    issues: list[dict[str, Any]],
    metric: dict[str, Any],
    dimension: dict[str, Any],
    aggregation: dict[str, Any],
    ordering: dict[str, Any],
    time: dict[str, Any],
    time_grouping: str | None,
) -> dict[str, Any]:
    if any(i["severity"] == "unsupported" for i in issues):
        status = "unsupported"
    elif any(i["severity"] == "needs_input" for i in issues):
        status = "needs_input"
    else:
        status = "ready"
    return {
        "question": text,
        "language": language,
        "status": status,
        "metric": metric,
        "dimension": dimension,
        "aggregation": aggregation,
        "ordering": ordering,
        "time": {**time, "grouping": time_grouping},
        "filters": {
            "requested": any(i["code"] in ("period_filter", "value_filter") for i in issues)
        },
        "issues": issues,
        "summary": _describe(metric, dimension, aggregation, ordering)
        if status == "ready"
        else None,
        "suggested_questions": _suggestions(metric, dimension, aggregation, language)
        if status != "ready"
        else [],
    }


class QuestionInterpretationError(ValueError):
    """The question cannot be answered as asked; the message says which part and what to do."""

    def __init__(self, interpretation: dict[str, Any]) -> None:
        self.interpretation = interpretation
        super().__init__(format_refusal(interpretation))


def format_refusal(interpretation: dict[str, Any]) -> str:
    """One readable message for a run that is refused (shown as the run error)."""
    blocking = [
        i for i in interpretation["issues"] if i["severity"] in ("unsupported", "needs_input")
    ]
    head = (
        "The question cannot be answered as asked."
        if interpretation["status"] == "unsupported"
        else "The question is not specific enough to answer without guessing."
    )
    lines = [head]
    for issue in blocking:
        lines.append(
            f"- {issue['message']}" + (f" {issue['suggestion']}" if issue.get("suggestion") else "")
        )
    if interpretation.get("suggested_questions"):
        lines.append("Try: " + " or ".join(f"'{q}'" for q in interpretation["suggested_questions"]))
    return "\n".join(lines)
