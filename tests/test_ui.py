from __future__ import annotations

import pytest

try:
    from streamlit.testing.v1 import AppTest
except Exception as exc:  # pragma: no cover - environment-dependent import guard
    pytest.skip(
        f"streamlit.testing.v1.AppTest unavailable in this environment: {exc}",
        allow_module_level=True,
    )


def test_ui_accepts_business_question_and_exposes_run_pipeline_button() -> None:
    app = AppTest.from_file("app/ui.py")

    app.run()

    app.text_input[0].set_value(
        "Which release years generate the strongest average streams?"
    )
    app.text_input[1].set_value("release_impact")

    app.run()

    assert len(app.exception) == 0
    assert any(button.label == "Run Pipeline" for button in app.button)


def test_ui_renders_single_dataset_url_mode_without_exceptions() -> None:
    app = AppTest.from_file("app/ui.py")

    app.run()

    app.radio[1].set_value("Fetch from URL")
    app.run()

    assert len(app.exception) == 0
    assert any(text_input.label == "Dataset URL" for text_input in app.text_input)


def test_ui_renders_multi_dataset_mode_without_exceptions() -> None:
    app = AppTest.from_file("app/ui.py")

    app.run()

    app.radio[0].set_value("Multi-dataset project")
    app.run()

    assert len(app.exception) == 0
    assert any(text_input.label == "Project Name" for text_input in app.text_input)
    assert any(
        number_input.label == "Number of datasets" for number_input in app.number_input
    )
    assert any(button.label == "Run Pipeline" for button in app.button)
