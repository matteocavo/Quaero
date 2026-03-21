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

    app.text_input["Business Question"].set_value(
        "Which release years generate the strongest average streams?"
    )
    app.text_input["Source Name (Project Name)"].set_value("release_impact")

    app.run()

    assert len(app.exception) == 0
    assert any(button.label == "Run Pipeline" for button in app.button)
