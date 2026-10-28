"""Question interpretation: the regression baseline from the post-merge audit (24 questions, 5 datasets).

Each case states the intended analysis BEFORE the code is consulted. A question is either answered as asked
(``ready``), held back for a missing choice (``needs_input``), or refused with the unsupported part
(``unsupported``). It is never answered with something else.
"""

from __future__ import annotations

import random

import pandas as pd
import pytest

from kpi_engine.question_interpreter import (
    QuestionInterpretationError,
    format_refusal,
    interpret_question,
)


def _sales(two_dates: bool = False) -> pd.DataFrame:
    rnd = random.Random(20261028)
    rows = []
    for i in range(240):
        day = pd.Timestamp("2022-01-01") + pd.Timedelta(days=rnd.randint(0, 1000))
        region = ["North", "South", "East", "West"][i % 4]
        row = {
            "order_date": day,
            "region": region,
            "product": f"P{(i * 7) % 6 + 1}",
            "channel": ["Online", "Retail", "Partner"][i % 3],
            "units": rnd.randint(1, 20),
            "revenue": round(100 + rnd.uniform(0, 200), 2),
            "discount": round(rnd.uniform(0, 0.3), 2) if rnd.random() > 0.2 else None,
            "customer_id": 1000 + i,
        }
        if two_dates:
            row["ship_date"] = day + pd.Timedelta(days=rnd.randint(2, 9))
        rows.append(row)
    return pd.DataFrame(rows)


def _staff() -> pd.DataFrame:
    rnd = random.Random(7)
    return pd.DataFrame(
        {
            "employee_id": range(5000, 5120),
            "department": [
                ["Engineering", "Sales", "Support", "Finance", "HR"][i % 5] for i in range(120)
            ],
            "salary": [rnd.randint(40000, 90000) for _ in range(120)],
            "rating": [round(rnd.uniform(2.5, 5), 1) for _ in range(120)],
            "hire_date": [
                pd.Timestamp("2012-01-01") + pd.Timedelta(days=rnd.randint(0, 3600))
                for _ in range(120)
            ],
            "city": [["Milan", "Rome", "Turin", "Naples"][i % 4] for i in range(120)],
            "years": [rnd.randint(0, 12) for _ in range(120)],
        }
    )


def _songs() -> pd.DataFrame:
    rnd = random.Random(3)
    n = 40
    return pd.DataFrame(
        {
            "song": [f"Song {i:02d}" for i in range(n)],
            "listeners": [0] * n,
            "streams": [rnd.randint(500, 9000) for _ in range(n - 1)] + [250000],
            "saves": [0] * n,
            "release_date": [
                pd.Timestamp(f"{2015 + (i % 10)}-0{(i % 9) + 1}-1{i % 9}") for i in range(n)
            ],
        }
    )


def _ita() -> pd.DataFrame:
    rnd = random.Random(5)
    base = {"Nord": 140, "Centro": 110, "Sud": 80, "Isole": 60, "Estero": 100}
    rows = []
    for i in range(120):
        reg = list(base)[i % 5]
        rows.append(
            {
                "regione": reg,
                "vendite": round(base[reg] + rnd.uniform(-25, 25), 2),
                "anno": 2020 + (i % 5),
                "prodotto": ["Pasta", "Vino", "Olio", "Caffe"][i % 4],
            }
        )
    return pd.DataFrame(rows)


DATA = {
    "sales": _sales(),
    "sales2d": _sales(two_dates=True),
    "staff": _staff(),
    "songs": _songs(),
    "ita": _ita(),
}


def codes(result: dict) -> set[str]:
    return {i["code"] for i in result["issues"]}


def plan(result: dict) -> tuple:
    return (
        result["metric"]["column"],
        result["dimension"]["column"],
        result["aggregation"]["value"],
        result["ordering"]["value"],
    )


def run(dataset: str, question: str, **explicit) -> dict:
    return interpret_question(question, DATA[dataset], **explicit)


# -------------------------------------------------------------------------- the 24 audit questions
READY = [
    # id, dataset, question, expected (metric, dimension, aggregation, ordering)
    (
        "C01",
        "sales",
        "Which region has the highest average revenue?",
        ("revenue", "region", "average", "highest"),
    ),
    (
        "C02",
        "sales",
        "What is the total revenue by product?",
        ("revenue", "product", "sum", "highest"),
    ),
    (
        "C03",
        "sales",
        "Which channel has the lowest average units?",
        ("units", "channel", "average", "lowest"),
    ),
    (
        "C04",
        "sales",
        "How many orders are there per region?",
        ("record_count", "region", "count", "highest"),
    ),
    (
        "C05",
        "sales",
        "Which product has the most revenue?",
        ("revenue", "product", "sum", "highest"),
    ),
    (
        "C11",
        "staff",
        "Which department pays the highest salary on average?",
        ("salary", "department", "average", "highest"),
    ),
    ("C13", "songs", "Which song has the most streams?", ("streams", "song", "sum", "highest")),
    (
        "C15",
        "ita",
        "Quale regione ha le vendite totali più alte?",
        ("vendite", "regione", "sum", "highest"),
    ),
    (
        "C24",
        "sales",
        "Which customer_id has the highest revenue?",
        ("revenue", "customer_id", "average", "highest"),
    ),
]


@pytest.mark.parametrize(
    ("case", "dataset", "question", "expected"), READY, ids=[r[0] for r in READY]
)
def test_supported_questions_are_read_as_asked(case, dataset, question, expected):
    result = run(dataset, question)
    assert result["status"] == "ready", (case, result["issues"])
    assert plan(result) == expected
    assert result["summary"]


UNSUPPORTED = [
    ("C06", "sales", "How has revenue changed by month?", {"time_grouping"}),
    ("C07", "sales", "What is the median revenue per region?", {"aggregation_not_supported"}),
    ("C08", "sales", "Which region had the highest revenue in 2023?", {"period_filter"}),
    (
        "C09",
        "sales",
        "Compare revenue between North and South",
        {"comparison_request", "value_filter"},
    ),
    ("C10", "sales", "Which region has the highest total revenue and units?", {"multiple_metrics"}),
    ("C14", "songs", "What is the total number of streams per year?", {"time_grouping"}),
    ("C23", "staff", "Why is salary higher in some departments?", {"causal_question"}),
]


@pytest.mark.parametrize(
    ("case", "dataset", "question", "expected"), UNSUPPORTED, ids=[r[0] for r in UNSUPPORTED]
)
def test_unsupported_questions_are_refused_with_the_reason(case, dataset, question, expected):
    result = run(dataset, question)
    assert result["status"] == "unsupported", (case, plan(result), result["issues"])
    assert expected <= codes(result)
    blocking = [i for i in result["issues"] if i["severity"] == "unsupported"]
    assert all(i["message"] and i["suggestion"] for i in blocking)
    assert result["summary"] is None  # nothing is presented as the answer


def test_c12_a_vague_performance_question_asks_for_the_measure():
    result = run("staff", "Which department has the best performance?")
    assert result["status"] == "needs_input"
    assert "metric_unclear" in codes(result)
    assert result["metric"]["column"] is None


def test_c16_a_question_with_no_content_is_not_given_an_arbitrary_analysis():
    result = run("sales", "What patterns are there in the data?")
    assert result["status"] == "needs_input"
    assert {"dimension_unclear", "metric_unclear"} <= codes(result)
    assert result["metric"]["column"] is None and result["dimension"]["column"] is None


# --------------------------------------------------------------------------- explicit selections
def test_c17_explicit_columns_answer_a_vague_question_and_the_assumed_aggregation_is_flagged():
    result = run(
        "sales",
        "What patterns are there in the data?",
        metric_column="revenue",
        dimension_column="region",
    )
    assert result["status"] == "ready"
    assert plan(result) == ("revenue", "region", "average", "highest")
    assert (
        result["metric"]["origin"] == "user_provided"
        and result["dimension"]["origin"] == "user_provided"
    )
    assert result["aggregation"]["origin"] == "assumed_default"
    assert "aggregation_assumed" in codes(result)


def test_c18_explicit_metric_wins_over_the_metric_named_in_the_question_and_says_so():
    result = run("sales", "Which region has the highest average revenue?", metric_column="units")
    assert plan(result)[:3] == ("units", "region", "average")
    assert result["metric"]["origin"] == "user_provided"
    assert "wording_not_applied" in codes(result)


def test_c19_explicit_dimension_wins_over_the_one_in_the_question():
    result = run("sales", "What is the total revenue by product?", dimension_column="channel")
    assert plan(result)[:3] == ("revenue", "channel", "sum")
    assert result["dimension"]["origin"] == "user_provided"


def test_c20_a_non_numeric_measure_is_refused_clearly():
    result = run("sales", "Which region has the highest average revenue?", metric_column="region")
    assert result["status"] == "unsupported" and "metric_not_numeric" in codes(result)


def test_c21_an_unknown_column_is_refused_clearly():
    result = run(
        "sales", "Which region has the highest average revenue?", dimension_column="no_such_column"
    )
    assert result["status"] == "unsupported" and "unknown_dimension_column" in codes(result)
    assert "region" in result["issues"][0]["suggestion"]


def test_c22_explicit_time_column_is_kept():
    result = run(
        "sales2d",
        "Show revenue over time",
        metric_column="revenue",
        dimension_column="region",
        time_column="ship_date",
    )
    assert result["time"]["column"] == "ship_date" and result["time"]["origin"] == "user_provided"
    assert result["status"] == "ready"


@pytest.mark.parametrize(
    ("dataset", "question", "explicit", "expected"),
    [
        (
            "sales",
            "Which product has the most revenue?",
            {"metric_column": "revenue", "dimension_column": "product"},
            ("revenue", "product", "sum"),
        ),
        (
            "songs",
            "Which song has the most streams?",
            {"metric_column": "streams", "dimension_column": "song"},
            ("streams", "song", "sum"),
        ),
        (
            "sales",
            "What is the total revenue?",
            {"metric_column": "revenue", "dimension_column": "region"},
            ("revenue", "region", "sum"),
        ),
    ],
)
def test_e1_to_e4_explicit_columns_with_wording_that_used_to_force_a_count(
    dataset, question, explicit, expected
):
    result = run(dataset, question, **explicit)
    assert result["status"] == "ready"
    assert plan(result)[:3] == expected


def test_e3_how_many_with_an_explicit_measure_counts_rows_and_says_the_measure_is_unused():
    result = run(
        "sales", "How many orders per region?", metric_column="revenue", dimension_column="region"
    )
    assert plan(result)[:3] == ("record_count", "region", "count")
    assert "metric_not_used_for_count" in codes(result)


def test_an_explicit_aggregation_wins_over_the_wording():
    result = run("sales", "Which product has the most revenue?", aggregation="average")
    assert result["aggregation"] == {"value": "average", "origin": "user_provided"}
    result = run("sales", "What is the total revenue by product?", aggregation="count")
    assert plan(result)[:3] == ("record_count", "product", "count")


def test_an_aggregation_that_is_not_available_is_refused():
    result = run("sales", "Revenue by region", aggregation="median")
    assert result["status"] == "unsupported" and "aggregation_not_supported" in codes(result)


# ---------------------------------------------------------------------------- aggregation wording
@pytest.mark.parametrize(
    ("question", "aggregation", "ordering"),
    [
        ("Which region has the highest average revenue?", "average", "highest"),
        ("What is the total revenue by region?", "sum", "highest"),
        ("Which region has the lowest total revenue?", "sum", "lowest"),
        ("Which region has the most orders?", "count", "highest"),
        ("Which region has the fewest orders?", "count", "lowest"),
        ("How many orders per region?", "count", "highest"),
        ("Count of orders per region", "count", "highest"),
        ("Which product has the largest total revenue?", "sum", "highest"),
        ("What is the mean revenue per product?", "average", "highest"),
        ("Which region has the maximum average revenue?", "average", "highest"),
        ("Which region has the minimum average revenue?", "average", "lowest"),
        ("Quanti ordini per regione?", "count", "highest"),
    ],
)
def test_aggregation_and_ordering_from_the_wording(question, aggregation, ordering):
    dataset = "sales"
    if "regione" in question:
        DATA["sales_it"] = DATA["sales"].rename(columns={"region": "regione"})
        dataset = "sales_it"
    result = run(dataset, question)
    assert result["status"] == "ready", result["issues"]
    assert result["aggregation"]["value"] == aggregation
    assert result["ordering"]["value"] == ordering


def test_most_means_total_for_a_measure_and_a_count_for_rows_and_the_note_says_which():
    measure = run("sales", "Which product has the most revenue?")
    assert measure["aggregation"]["value"] == "sum" and "count_read_as_total" in codes(measure)
    rows = run("sales", "Which product has the most orders?")
    assert rows["aggregation"]["value"] == "count"


def test_number_of_a_measure_is_its_total():
    result = run("songs", "What is the number of streams per song?")
    assert plan(result)[:3] == ("streams", "song", "sum")


def test_both_a_total_and_an_average_are_refused():
    result = run("sales", "Show the total and average revenue by region")
    assert result["status"] == "unsupported" and "multiple_aggregations" in codes(result)


def test_max_and_min_alone_are_refused_but_a_column_called_min_is_left_alone():
    assert run("sales", "What is the maximum revenue per region?")["status"] == "unsupported"
    frame = pd.DataFrame({"genre": list("ABCABC"), "duration_min": [10, 20, 30, 12, 22, 33.0]})
    result = interpret_question("Which genre generates the highest average duration min?", frame)
    assert result["status"] == "ready" and plan(result)[:3] == ("duration_min", "genre", "average")


def test_top_n_is_noted_not_applied():
    result = run("sales", "Top 5 products by revenue")
    assert result["status"] == "ready" and "top_n_not_applied" in codes(result)


# -------------------------------------------------------------------------- identifiers and constants
def test_identifier_columns_are_never_inferred_as_measures():
    frame = pd.DataFrame(
        {
            "order_id": range(1, 21),
            "region": list("AB") * 10,
            "amount": [float(i) for i in range(20)],
        }
    )
    result = interpret_question("Which region has the highest average amount?", frame)
    assert result["metric"]["column"] == "amount"
    only_ids = pd.DataFrame({"order_id": range(1, 21), "region": list("AB") * 10})
    result = interpret_question("Which region has the highest average order_id?", only_ids)
    assert result["status"] == "needs_input" and "no_metric" in codes(result)


def test_constant_and_empty_columns_are_not_picked_without_an_explicit_choice():
    result = run("songs", "Which song has the highest average listeners?")
    assert result["metric"]["column"] != "listeners"
    forced = run(
        "songs",
        "Which song has the highest average listeners?",
        metric_column="listeners",
        dimension_column="song",
    )
    assert forced["metric"]["column"] == "listeners" and "metric_constant" in codes(forced)


def test_a_year_column_is_not_taken_for_a_second_measure():
    frame = pd.DataFrame({"anno": [2020, 2021, 2022] * 4, "vendite": [float(i) for i in range(12)]})
    result = interpret_question("How many vendite per anno?", frame)
    assert result["status"] == "ready"


# ---------------------------------------------------------------------------- time and filters
def test_a_period_column_that_exists_is_used_for_per_year_and_a_missing_one_is_refused():
    # 'anno' holds the year, so 'per year' (or 'per anno') groups by it.
    assert run("ita", "What is the total vendite per year?")["dimension"]["column"] == "anno"
    result = run("ita", "Qual è il totale delle vendite per anno?")
    assert result["status"] == "ready" and result["dimension"]["column"] == "anno"
    frame = pd.DataFrame({"year": [2020, 2021] * 6, "revenue": [float(i) for i in range(12)]})
    assert interpret_question("Total revenue per year", frame)["dimension"]["column"] == "year"
    # no month column anywhere: refused, not answered by exact dates
    assert "time_grouping" in codes(run("ita", "Qual è il totale delle vendite per mese?"))


def test_exact_dates_are_said_to_be_exact_when_there_is_no_grain_in_the_question():
    result = run("sales", "Show the average revenue by order_date")
    assert result["status"] == "ready" and "exact_dates" in codes(result)
    assert result["time"]["grouping"] == "exact_values"


@pytest.mark.parametrize(
    "question",
    [
        "Which region had the highest revenue in 2023?",
        "What was the revenue last year by region?",
        "Total revenue by region since 2022",
        "Qual è il totale delle vendite nel 2023 per regione?",
    ],
)
def test_requests_limited_to_a_period_are_refused(question):
    dataset = "ita" if "vendite" in question else "sales"
    assert "period_filter" in codes(run(dataset, question))


def test_a_value_named_in_the_question_is_a_filter_and_is_refused_but_a_word_that_is_also_a_column_is_not():
    assert "value_filter" in codes(run("sales", "Total revenue for North by product"))
    frame = pd.DataFrame(
        {"department": ["Sales", "HR", "Ops"] * 4, "sales": [float(i) for i in range(12)]}
    )
    result = interpret_question("Total sales by department", frame)
    assert result["status"] == "ready"


@pytest.mark.parametrize(
    "question",
    [
        "Why is revenue higher in the East?",
        "Perché le vendite sono più alte al Nord?",
        "What drives revenue by region?",
    ],
)
def test_causal_questions_are_refused(question):
    dataset = "ita" if "vendite" in question else "sales"
    assert run(dataset, question)["status"] == "unsupported"


@pytest.mark.parametrize(
    "question",
    [
        "Predict next year's revenue by region",
        "Is revenue correlated with units?",
        "Forecast revenue per product",
    ],
)
def test_forecasts_and_correlations_are_refused(question):
    assert run("sales", question)["status"] == "unsupported"


# --------------------------------------------------------------------------------- refusals are usable
def test_a_refusal_offers_a_question_that_works_and_that_question_is_accepted():
    refused = run(
        "sales",
        "Which region has the median revenue?",
        metric_column="revenue",
        dimension_column="region",
    )
    assert refused["status"] == "unsupported" and refused["suggested_questions"]
    for suggestion in refused["suggested_questions"]:
        again = interpret_question(suggestion, DATA["sales"])
        assert again["status"] == "ready", (suggestion, again["issues"])


def test_italian_refusals_offer_italian_questions_that_are_accepted():
    refused = interpret_question("Qual è la mediana delle vendite per regione?", DATA["ita"])
    assert refused["status"] == "unsupported" and refused["language"] == "it"
    assert refused["suggested_questions"]
    assert all(q.startswith(("Qual è", "Quanti")) for q in refused["suggested_questions"])
    for suggestion in refused["suggested_questions"]:
        assert interpret_question(suggestion, DATA["ita"])["status"] == "ready", suggestion
    assert (
        interpret_question("Perché le vendite per regione sono diverse?", DATA["ita"])["status"]
        == "unsupported"
    )


def test_the_refusal_text_names_the_part_and_the_way_out():
    refused = run(
        "sales",
        "What is the median revenue per region?",
        metric_column="revenue",
        dimension_column="region",
    )
    text = format_refusal(refused)
    assert text.startswith("The question cannot be answered as asked.")
    assert "median" in text and "Try:" in text
    with pytest.raises(QuestionInterpretationError) as error:
        raise QuestionInterpretationError(refused)
    assert str(error.value) == text and error.value.interpretation is refused


def test_an_empty_question_asks_for_one():
    assert run("sales", "   ")["status"] == "needs_input"


def test_the_result_is_plain_data():
    import json

    result = run("sales", "Which region has the highest average revenue?")
    assert json.loads(json.dumps(result)) == result
    assert set(result) == {
        "question",
        "language",
        "status",
        "metric",
        "dimension",
        "aggregation",
        "ordering",
        "time",
        "filters",
        "issues",
        "summary",
        "suggested_questions",
    }
    assert not any("confidence" in key or "score" in key for key in result)


def test_the_same_question_always_gets_the_same_interpretation():
    a = run("sales", "Which region has the highest average revenue?")
    b = run("sales", "Which region has the highest average revenue?")
    assert a == b
