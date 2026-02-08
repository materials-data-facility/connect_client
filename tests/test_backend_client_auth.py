from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any, Dict, Optional

from mdf_agent.core.backend_client import BackendClient, _api_url_for_service
from mdf_agent.auth.globus import NCSA_MDF_COLLECTION_UUID, get_scopes_for_service


class _DummyResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload
        self.text = ""

    def json(self) -> Dict[str, Any]:
        return self._payload


class _RecordingHTTPClient:
    def __init__(self):
        self.last_headers: Optional[Dict[str, str]] = None

    def request(self, method: str, url: str, json: Any = None, params: Any = None, headers: Any = None):
        self.last_headers = headers or {}
        return _DummyResponse({"success": True})


def test_from_env_reads_auth(monkeypatch):
    monkeypatch.setenv("MDF_API_URL", "https://example.org")
    monkeypatch.setenv("MDF_CONNECT_TOKEN", "token-123")
    monkeypatch.setenv("MDF_DEV_USER_ID", "user-123")

    client = BackendClient.from_env()
    assert client.base_url == "https://example.org"
    assert client._token == "token-123"
    assert client._user_id == "user-123"


def test_request_prefers_token_header():
    client = BackendClient(base_url="https://example.org", token="abc", user_id="dev-user")
    recorder = _RecordingHTTPClient()
    client._client = recorder  # type: ignore[assignment]

    client.search("query")
    assert recorder.last_headers == {"Authorization": "Bearer abc"}


def test_request_uses_dev_user_header():
    client = BackendClient(base_url="https://example.org", user_id="dev-user")
    recorder = _RecordingHTTPClient()
    client._client = recorder  # type: ignore[assignment]

    client.search("query")
    assert recorder.last_headers == {"X-User-Id": "dev-user"}


def test_authenticated_uses_env_token(monkeypatch):
    monkeypatch.setenv("MDF_CONNECT_TOKEN", "env-token")
    monkeypatch.delenv("MDF_DEV_USER_ID", raising=False)

    client = BackendClient.authenticated(base_url="https://example.org", service_instance="dev")
    assert client._token == "env-token"
    assert client._user_id is None


def test_authenticated_local_without_credentials_no_oauth(monkeypatch):
    monkeypatch.delenv("MDF_CONNECT_TOKEN", raising=False)
    monkeypatch.delenv("MDF_DEV_USER_ID", raising=False)
    monkeypatch.delenv("LOCAL_USER_ID", raising=False)

    client = BackendClient.authenticated(base_url="http://127.0.0.1:8080", service_instance="local")
    assert client._token is None
    assert client._user_id is None


def test_authenticated_uses_confidential_credentials(monkeypatch):
    monkeypatch.delenv("MDF_CONNECT_TOKEN", raising=False)
    monkeypatch.delenv("MDF_DEV_USER_ID", raising=False)
    monkeypatch.setenv("MDF_CLIENT_ID", "client-id")
    monkeypatch.setenv("MDF_CLIENT_SECRET", "client-secret")

    _scope, resource_server = get_scopes_for_service("prod")
    captured: Dict[str, Any] = {}

    class _FakeConfidentialClient:
        def __init__(self, client_id: str, client_secret: str):
            captured["client_id"] = client_id
            captured["client_secret"] = client_secret

        def oauth2_client_credentials_tokens(self, requested_scopes: Optional[str] = None):
            captured["requested_scopes"] = requested_scopes
            return SimpleNamespace(
                by_resource_server={
                    resource_server: {"access_token": "service-token"},
                    NCSA_MDF_COLLECTION_UUID: {"access_token": "data-token"},
                }
            )

    fake_globus_sdk = SimpleNamespace(ConfidentialAppAuthClient=_FakeConfidentialClient)
    monkeypatch.setitem(sys.modules, "globus_sdk", fake_globus_sdk)

    client = BackendClient.authenticated(base_url="https://example.org", service_instance="prod")
    assert captured["client_id"] == "client-id"
    assert captured["client_secret"] == "client-secret"
    assert "requested_scopes" in captured
    assert client._token == "service-token"
    assert client._globus_data_token == "data-token"
    assert client._user_id is None


def test_api_url_for_service_respects_env_override(monkeypatch):
    monkeypatch.setenv("MDF_API_URL", "https://override.example")
    assert _api_url_for_service("prod") == "https://override.example"
