from __future__ import annotations

from typing import Any, Dict, Optional

from mdf_agent.core.backend_client import BackendClient


class _DummyResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200):
        self._payload = payload
        self.text = ""
        self.status_code = status_code
        self.headers: Dict[str, str] = {}

    def json(self) -> Dict[str, Any]:
        return self._payload


class _DummyDownloadResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code
        self.text = content.decode("utf-8", errors="ignore")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _RecordingHTTPClient:
    def __init__(self):
        self.calls: list[Dict[str, Any]] = []
        self.download_calls: list[Dict[str, Any]] = []

    def request(
        self,
        method: str,
        url: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> _DummyResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "json": json,
                "params": params,
                "headers": headers or {},
            }
        )

        if method == "GET" and url.endswith("/stream/stream-1/files"):
            return _DummyResponse(
                {
                    "success": True,
                    "files": [
                        {
                            "filename": "data.csv",
                            "path": "streams/stream-1/data.csv",
                            "storage_backend": "globus",
                        },
                        {
                            "filename": "notes.txt",
                            "path": "streams/stream-1/notes.txt",
                            "storage_backend": "globus",
                        },
                    ],
                }
            )

        if method == "POST" and url.endswith("/stream/stream-1/download-url"):
            path = (json or {}).get("path")
            return _DummyResponse(
                {"success": True, "download_url": f"https://download.example/{path}"}
            )

        return _DummyResponse({"success": True})

    def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> _DummyDownloadResponse:
        self.download_calls.append({"url": url, "headers": headers})
        return _DummyDownloadResponse(content=f"download:{url}".encode("utf-8"))


def _make_client(
    token: Optional[str] = None,
    globus_data_token: Optional[str] = None,
) -> tuple[BackendClient, _RecordingHTTPClient]:
    client = BackendClient(
        base_url="https://api.example",
        token=token,
        globus_data_token=globus_data_token,
    )
    recorder = _RecordingHTTPClient()
    client._client = recorder  # type: ignore[assignment]
    return client, recorder


def test_health_calls_endpoint():
    client, recorder = _make_client()
    client.health()
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/health"


def test_curation_pending_calls_endpoint():
    client, recorder = _make_client()
    client.curation_pending(limit=10, offset=5, organization="MDF")
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/curation/pending"
    assert recorder.calls[-1]["params"] == {"limit": 10, "offset": 5, "organization": "MDF"}


def test_curation_detail_calls_endpoint():
    client, recorder = _make_client()
    client.curation_detail("src-1", version="2.0")
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/curation/src-1"
    assert recorder.calls[-1]["params"] == {"version": "2.0"}


def test_curation_approve_calls_endpoint():
    client, recorder = _make_client()
    client.curation_approve(
        source_id="src-1",
        mint_doi=False,
        notes="looks good",
        metadata_updates={"title": "Updated"},
        version="1.0",
    )
    assert recorder.calls[-1]["method"] == "POST"
    assert recorder.calls[-1]["url"] == "https://api.example/curation/src-1/approve"
    assert recorder.calls[-1]["json"] == {
        "mint_doi": False,
        "notes": "looks good",
        "metadata_updates": {"title": "Updated"},
        "version": "1.0",
    }


def test_curation_reject_calls_endpoint():
    client, recorder = _make_client()
    client.curation_reject(
        source_id="src-2",
        reason="missing metadata",
        suggestions="add units",
        version="1.0",
    )
    assert recorder.calls[-1]["method"] == "POST"
    assert recorder.calls[-1]["url"] == "https://api.example/curation/src-2/reject"
    assert recorder.calls[-1]["json"] == {
        "reason": "missing metadata",
        "suggestions": "add units",
        "version": "1.0",
    }


def test_dataset_preview_calls_endpoint():
    client, recorder = _make_client()
    client.dataset_preview("src-1")
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/preview/src-1"


def test_dataset_files_calls_endpoint():
    client, recorder = _make_client()
    client.dataset_files("src-1")
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/preview/src-1/files"


def test_dataset_file_detail_encodes_path():
    client, recorder = _make_client()
    client.dataset_file_detail("src-1", "folder/a file.csv")
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/preview/src-1/files/folder/a%20file.csv"


def test_dataset_sample_calls_endpoint():
    client, recorder = _make_client()
    client.dataset_sample("src-1")
    assert recorder.calls[-1]["method"] == "GET"
    assert recorder.calls[-1]["url"] == "https://api.example/preview/src-1/sample"


def test_stream_close_accepts_optional_payload():
    client, recorder = _make_client()
    client.stream_close(
        stream_id="stream-1",
        mint_doi=True,
        title="Run 42",
        description="Published stream",
        authors=[{"name": "Jane Doe"}],
        keywords=["alloy", "xrd"],
        license="CC-BY-4.0",
    )
    assert recorder.calls[-1]["method"] == "POST"
    assert recorder.calls[-1]["url"] == "https://api.example/stream/stream-1/close"
    assert recorder.calls[-1]["json"] == {
        "stream_id": "stream-1",
        "mint_doi": True,
        "title": "Run 42",
        "description": "Published stream",
        "authors": [{"name": "Jane Doe"}],
        "keywords": ["alloy", "xrd"],
        "license": "CC-BY-4.0",
    }


def test_stream_clone_downloads_files(tmp_path):
    client, recorder = _make_client(token="openid-token", globus_data_token="data-token")
    result = client.stream_clone("stream-1", dest_dir=str(tmp_path), file_filter="*.csv")

    assert result["success"] is True
    assert result["downloaded"] == 1
    output_file = tmp_path / "data.csv"
    assert output_file.exists()
    assert output_file.read_bytes() == b"download:https://download.example/streams/stream-1/data.csv"

    assert recorder.download_calls == [
        {
            "url": "https://download.example/streams/stream-1/data.csv",
            "headers": {"Authorization": "Bearer data-token"},
        }
    ]
