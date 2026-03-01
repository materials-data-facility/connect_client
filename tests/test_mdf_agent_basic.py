import zipfile
from pathlib import Path

import pytest

import mdf_agent.core.agent as agent_module
from mdf_agent.core.agent import (
    MDFAgent,
    UploadResult,
    _create_zip_archive,
    _detect_gcp_endpoint,
    _download_https_file,
    _globus_uri_to_https,
    _resolve_globus_files,
)


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


# ---------------------------------------------------------------------------
# Clone / download tests
# ---------------------------------------------------------------------------

NCSA_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"


def test_globus_uri_to_https():
    """Converts NCSA globus:// URI to HTTPS URL."""
    uri = f"globus://{NCSA_UUID}/mdf_open/test_ds/data.csv"
    result = _globus_uri_to_https(uri)
    assert result == "https://data.materialsdatafacility.org/mdf_open/test_ds/data.csv"


def test_globus_uri_to_https_non_ncsa():
    """Non-NCSA endpoint URI returns None."""
    uri = "globus://aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/some/path.csv"
    result = _globus_uri_to_https(uri)
    assert result is None


def test_globus_uri_to_https_non_globus():
    """Non-globus URI returns None."""
    assert _globus_uri_to_https("https://example.com/file.csv") is None
    assert _globus_uri_to_https("stream://my-stream") is None


def test_resolve_globus_files_per_file_uris():
    """Per-file NCSA URIs produce correct (url, relative_path) pairs."""
    sources = [
        f"globus://{NCSA_UUID}/mdf_open/my_dataset/subdir/file1.csv",
        f"globus://{NCSA_UUID}/mdf_open/my_dataset/file2.csv",
        "https://example.com/other.csv",  # should be skipped
        "stream://my-stream",  # should be skipped
    ]
    result = _resolve_globus_files(sources, "fake-transfer-token")
    assert len(result) == 2
    assert result[0] == (
        "https://data.materialsdatafacility.org/mdf_open/my_dataset/subdir/file1.csv",
        "subdir/file1.csv",
    )
    assert result[1] == (
        "https://data.materialsdatafacility.org/mdf_open/my_dataset/file2.csv",
        "file2.csv",
    )


def test_resolve_globus_files_directory_uri(monkeypatch: pytest.MonkeyPatch):
    """Directory URI triggers operation_ls mock, expands to file list."""
    sources = [f"globus://{NCSA_UUID}/mdf_open/my_dataset/"]

    class FakeEntry(dict):
        pass

    class FakeListing(list):
        pass

    def fake_operation_ls(endpoint_id, path=None):
        if path == "/mdf_open/my_dataset/":
            return FakeListing([
                FakeEntry({"type": "file", "name": "a.csv"}),
                FakeEntry({"type": "dir", "name": "sub"}),
            ])
        elif path == "/mdf_open/my_dataset/sub/":
            return FakeListing([
                FakeEntry({"type": "file", "name": "b.csv"}),
            ])
        return FakeListing([])

    class FakeTC:
        def __init__(self, authorizer=None):
            pass
        operation_ls = staticmethod(fake_operation_ls)

    import globus_sdk
    monkeypatch.setattr(globus_sdk, "TransferClient", FakeTC)

    result = _resolve_globus_files(sources, "fake-transfer-token")
    assert len(result) == 2
    urls = [r[0] for r in result]
    rels = [r[1] for r in result]
    assert "https://data.materialsdatafacility.org/mdf_open/my_dataset/a.csv" in urls
    assert "https://data.materialsdatafacility.org/mdf_open/my_dataset/sub/b.csv" in urls
    assert "a.csv" in rels
    assert "sub/b.csv" in rels


class _FakeStreamResponse:
    """Mock httpx streaming response for download tests."""
    def __init__(self, content: bytes):
        self._content = content
        self.status_code = 200
        self.headers = {"content-length": str(len(content))}

    def raise_for_status(self):
        pass

    def iter_bytes(self, chunk_size=None):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def close(self):
        pass


class _FakeDownloadClient:
    """Mock httpx.Client that returns a streaming download response."""
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def stream(self, method, url, **kwargs):
        return _FakeStreamResponse(b"hello,world\n1,2\n")


def test_download_https_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Mock httpx GET → file written with correct content and parent dirs created."""
    monkeypatch.delenv("MDF_SSL_VERIFY", raising=False)
    monkeypatch.setattr("httpx.Client", _FakeDownloadClient)

    dest = tmp_path / "sub" / "dir" / "file.csv"
    result = _download_https_file(
        "https://data.materialsdatafacility.org/mdf_open/test/file.csv",
        dest,
        "fake-data-token",
    )
    assert result == dest
    assert dest.exists()
    assert dest.read_text() == "hello,world\n1,2\n"


def test_clone_with_zip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Mock card with download_url → downloads zip, extracts, verifies output files."""
    # Create a test zip
    zip_path = tmp_path / "source.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.csv", "a,b\n1,2\n")
        zf.writestr("sub/nested.txt", "hello")
    zip_bytes = zip_path.read_bytes()

    class FakeZipStreamResponse:
        def __init__(self, *a, **kw):
            self.status_code = 200
            self.headers = {"content-length": str(len(zip_bytes))}

        def raise_for_status(self):
            pass

        def iter_bytes(self, chunk_size=None):
            yield zip_bytes

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def close(self):
            pass

    class FakeZipClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def stream(self, method, url, **kw):
            return FakeZipStreamResponse()

    fake_card = {
        "source_id": "test_ds_v1.1",
        "title": "Test Dataset",
        "download_url": f"globus://{NCSA_UUID}/mdf_open/test_ds/.mdf/data.zip",
        "archive_size": len(zip_bytes),
        "data_sources": [f"globus://{NCSA_UUID}/mdf_open/test_ds/data.csv"],
    }

    class FakeBackendClient:
        _globus_data_token = "fake-data"
        _globus_transfer_token = "fake-transfer"

        @classmethod
        def authenticated(cls, **kw):
            return cls()

        def get_card(self, source_id, version=None):
            return fake_card

        def close(self):
            pass

    monkeypatch.delenv("MDF_SSL_VERIFY", raising=False)
    monkeypatch.setattr("httpx.Client", FakeZipClient)
    monkeypatch.setattr(agent_module, "BackendClient", FakeBackendClient)

    out = tmp_path / "output"
    agent = MDFAgent()
    result = agent.clone(source_id="test_ds_v1.1", output_dir=str(out))

    assert result["success"] is True
    assert result["method"] == "zip"
    assert result["files_count"] == 2
    assert (out / "data.csv").exists()
    assert (out / "sub" / "nested.txt").exists()


def test_clone_without_zip_https(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Mock card with per-file data_sources → downloads each file individually."""
    fake_card = {
        "source_id": "test_ds_v1.1",
        "title": "Test Dataset",
        "data_sources": [
            f"globus://{NCSA_UUID}/mdf_open/test_ds/file1.csv",
            f"globus://{NCSA_UUID}/mdf_open/test_ds/sub/file2.csv",
        ],
    }

    class FakeBackendClient:
        _globus_data_token = "fake-data"
        _globus_transfer_token = "fake-transfer"

        @classmethod
        def authenticated(cls, **kw):
            return cls()

        def get_card(self, source_id, version=None):
            return fake_card

        def close(self):
            pass

    downloaded_urls = []

    def fake_download(url, dest, token, progress_callback=None):
        downloaded_urls.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(f"content of {dest.name}")
        return dest

    monkeypatch.setattr(agent_module, "BackendClient", FakeBackendClient)
    monkeypatch.setattr(agent_module, "_download_https_file", fake_download)

    out = tmp_path / "output"
    agent = MDFAgent()
    result = agent.clone(source_id="test_ds_v1.1", output_dir=str(out))

    assert result["success"] is True
    assert result["method"] == "https"
    assert result["files_count"] == 2
    assert len(downloaded_urls) == 2
    assert (out / "file1.csv").exists()
    assert (out / "sub" / "file2.csv").exists()


def test_clone_transfer_no_gcp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """No GCP detected → raises error with helpful message."""
    fake_card = {
        "source_id": "test_ds_v1.1",
        "title": "Test Dataset",
        "data_sources": [f"globus://{NCSA_UUID}/mdf_open/test_ds/data.csv"],
    }

    class FakeBackendClient:
        _globus_data_token = "fake-data"
        _globus_transfer_token = "fake-transfer"

        @classmethod
        def authenticated(cls, **kw):
            return cls()

        def get_card(self, source_id, version=None):
            return fake_card

        def close(self):
            pass

    monkeypatch.setattr(agent_module, "BackendClient", FakeBackendClient)
    monkeypatch.setattr(agent_module, "_detect_gcp_endpoint", lambda: None)

    agent = MDFAgent()
    with pytest.raises(RuntimeError, match="Globus Transfer requires a local endpoint"):
        agent.clone(
            source_id="test_ds_v1.1",
            output_dir=str(tmp_path / "out"),
            method="transfer",
        )


def test_detect_gcp_from_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Config globus.local_endpoint_id used as fallback when SDK detection fails."""
    from mdf_agent.core import config as config_module
    from mdf_agent.core.config import GlobalConfig

    config_path = tmp_path / "config.json"
    cfg = GlobalConfig(path=config_path)
    cfg.set("globus.local_endpoint_id", "11111111-2222-3333-4444-555555555555")

    # Mock SDK detection to fail
    class FakeGCP:
        endpoint_id = None

    import globus_sdk
    monkeypatch.setattr(globus_sdk, "LocalGlobusConnectPersonal", FakeGCP)

    # Patch GlobalConfig constructor in the config module so _detect_gcp_endpoint
    # picks up our test config
    original_init = GlobalConfig.__init__

    def patched_init(self, path=None):
        original_init(self, path=config_path)

    monkeypatch.setattr(config_module, "GlobalConfig", type(
        "GlobalConfig", (GlobalConfig,), {"__init__": patched_init}
    ))

    result = _detect_gcp_endpoint()
    assert result == "11111111-2222-3333-4444-555555555555"
