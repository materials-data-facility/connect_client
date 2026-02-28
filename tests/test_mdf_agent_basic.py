import zipfile
from pathlib import Path

import pytest

import mdf_agent.core.agent as agent_module
from mdf_agent.core.agent import MDFAgent, UploadResult, _create_zip_archive


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


# ---------------------------------------------------------------------------
# Zip archive tests
# ---------------------------------------------------------------------------


def test_zip_archive_created_for_directory(tmp_path: Path):
    """Zip is created from a temp dir with subdirs; verify arcnames and compression."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "a.csv").write_text("a,b\n1,2\n")
    sub = data_dir / "subdir"
    sub.mkdir()
    (sub / "b.csv").write_text("x,y\n3,4\n")

    zip_path = _create_zip_archive([(data_dir, "")])
    assert zip_path is not None

    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = sorted(zf.namelist())
            assert names == ["a.csv", "subdir/b.csv"]
            # Verify compression method
            for info in zf.infolist():
                assert info.compress_type == zipfile.ZIP_DEFLATED
    finally:
        zip_path.unlink(missing_ok=True)


def test_zip_archive_preserves_structure(tmp_path: Path):
    """Multiple directories get correct prefixed paths in zip."""
    dir_a = tmp_path / "alpha"
    dir_a.mkdir()
    (dir_a / "a.csv").write_text("1")

    dir_b = tmp_path / "beta"
    dir_b.mkdir()
    sub = dir_b / "nested"
    sub.mkdir()
    (sub / "b.csv").write_text("2")

    zip_path = _create_zip_archive([(dir_a, "alpha"), (dir_b, "beta")])
    assert zip_path is not None

    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = sorted(zf.namelist())
            assert names == ["alpha/a.csv", "beta/nested/b.csv"]
    finally:
        zip_path.unlink(missing_ok=True)


def test_zip_archive_skipped_over_size_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
    """When total size exceeds limit, returns None and prints warning."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "big.bin").write_bytes(b"x" * 100)

    monkeypatch.setattr(agent_module, "_ZIP_MAX_TOTAL_BYTES", 50)

    result = _create_zip_archive([(data_dir, "")])
    assert result is None

    captured = capsys.readouterr()
    assert "Skipping zip archive" in captured.err


def test_zip_archive_skipped_for_empty_dir(tmp_path: Path):
    """Empty directory produces no zip."""
    empty = tmp_path / "empty"
    empty.mkdir()

    result = _create_zip_archive([(empty, "")])
    assert result is None


def test_zip_skips_external_symlinks(tmp_path: Path):
    """Symlinks pointing outside the source dir are excluded from zip."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "real.txt").write_text("hello")

    outside = tmp_path / "secret.txt"
    outside.write_text("password")
    (data_dir / "link.txt").symlink_to(outside)

    zip_path = _create_zip_archive([(data_dir, "")])
    assert zip_path is not None

    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            assert "real.txt" in names
            assert "link.txt" not in names
    finally:
        zip_path.unlink(missing_ok=True)


def test_upload_result_includes_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """_upload_local_files returns UploadResult with archive_url for directories."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "file.csv").write_text("a,b\n1,2\n")

    _RecordingUploadClient.created_verify_values = []
    monkeypatch.delenv("MDF_SSL_VERIFY", raising=False)
    monkeypatch.setattr("httpx.Client", _RecordingUploadClient)
    monkeypatch.setattr(agent_module, "_INSECURE_SSL_WARNING_EMITTED", False)

    result = agent_module._upload_local_files(
        data_sources=[str(data_dir)],
        data_token="fake-token",
        source_id="test_dataset",
    )

    assert isinstance(result, UploadResult)
    assert result.archive_url is not None
    assert result.archive_url.endswith("/.mdf/data.zip")
    assert result.archive_size is not None
    assert result.archive_size > 0
    assert len(result.data_sources) == 1  # one file uploaded
