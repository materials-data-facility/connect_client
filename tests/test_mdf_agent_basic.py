from pathlib import Path

import pytest

import mdf_agent.core.agent as agent_module
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


class _DummyUploadResponse:
    status_code = 201
    text = ""


class _RecordingUploadClient:
    created_verify_values = []

    def __init__(self, *args, **kwargs):
        self.verify = kwargs.get("verify")
        self.__class__.created_verify_values.append(self.verify)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def put(self, url, content=None, headers=None):
        if content is not None:
            b"".join(content)
        return _DummyUploadResponse()


def test_https_put_verifies_tls_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    source = tmp_path / "data.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")

    _RecordingUploadClient.created_verify_values = []
    monkeypatch.delenv("MDF_SSL_VERIFY", raising=False)
    monkeypatch.setattr("httpx.Client", _RecordingUploadClient)
    monkeypatch.setattr(agent_module, "_INSECURE_SSL_WARNING_EMITTED", False)

    uri = agent_module._https_put_file(source, "/mdf_open/test/data.csv", "data-token")

    assert uri == "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/mdf_open/test/data.csv"
    assert _RecordingUploadClient.created_verify_values == [True]


def test_https_put_warns_when_tls_verification_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    source = tmp_path / "data.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")

    _RecordingUploadClient.created_verify_values = []
    monkeypatch.setenv("MDF_SSL_VERIFY", "false")
    monkeypatch.setattr("httpx.Client", _RecordingUploadClient)
    monkeypatch.setattr(agent_module, "_INSECURE_SSL_WARNING_EMITTED", False)

    agent_module._https_put_file(source, "/mdf_open/test/data.csv", "data-token")

    captured = capsys.readouterr()
    assert "MDF_SSL_VERIFY=false disables TLS certificate verification" in captured.err
    assert _RecordingUploadClient.created_verify_values == [False]
