from pathlib import Path

import pytest

from mdf_agent.core.agent import MDFAgent


def test_build_submission_shape(tmp_path: Path):
    agent = MDFAgent.init(
        path=str(tmp_path),
        title="Test Dataset",
        authors=["Doe, Jane"],
    )
    agent.manifest.data_sources = ["./data"]
    agent.save_manifest()

    payload = agent.build_submission(test=True, update=False)

    assert payload["title"] == "Test Dataset"
    assert payload["data_sources"]
    assert payload["test"] is True
    assert payload["update"] is False
    assert "update_metadata_only" in payload


def test_validate_missing_fields(tmp_path: Path):
    agent = MDFAgent.init(
        path=str(tmp_path),
        title="Test Dataset",
        authors=["Doe, Jane"],
    )
    agent.manifest.data_sources = []
    agent.save_manifest()

    validation = agent.validate()
    assert any("data_sources" in err for err in validation["errors"])
