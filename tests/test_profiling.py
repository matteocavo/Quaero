from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipelines.profiling import profile_dataframe


def test_profile_dataframe_writes_metadata_and_returns_profile(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "sales": [10.5, 12.0, None, 9.75],
            "region": ["North", "South", None, "North"],
        }
    )

    profile = profile_dataframe(dataframe=dataframe, project_root=tmp_path)

    profile_path = tmp_path / "metadata" / "source_profile.json"

    assert profile_path.exists()
    assert profile["source"] == "source"
    assert profile["output_path"] == "metadata/source_profile.json"
    assert profile["row_count"] == 4
    assert profile["column_count"] == 3

    saved_profile = json.loads(profile_path.read_text(encoding="utf-8"))
    assert saved_profile == {
        key: value for key, value in profile.items() if key != "output_path"
    }

    columns_by_name = {
        column_profile["column_name"]: column_profile
        for column_profile in profile["columns"]
    }

    assert columns_by_name["order_id"]["dtype"] == "int64"
    assert columns_by_name["order_id"]["null_percentage"] == 0.0
    assert columns_by_name["order_id"]["unique_count"] == 4
    assert columns_by_name["order_id"]["sample_values"] == [3, 4, 2, 1]
    assert columns_by_name["order_id"]["min"] == 1
    assert columns_by_name["order_id"]["max"] == 4

    assert columns_by_name["sales"]["dtype"] == "float64"
    assert columns_by_name["sales"]["null_percentage"] == 25.0
    assert columns_by_name["sales"]["unique_count"] == 3
    assert columns_by_name["sales"]["sample_values"] == [9.75, 12.0, 10.5]
    assert columns_by_name["sales"]["min"] == 9.75
    assert columns_by_name["sales"]["max"] == 12.0

    assert columns_by_name["region"]["dtype"] == "object"
    assert columns_by_name["region"]["null_percentage"] == 25.0
    assert columns_by_name["region"]["unique_count"] == 2
    assert columns_by_name["region"]["sample_values"] == ["North", "South", "North"]
    assert "min" not in columns_by_name["region"]
    assert "max" not in columns_by_name["region"]
