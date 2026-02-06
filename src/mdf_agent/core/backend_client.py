from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import httpx

_V2_API_URLS = {
    "prod": "https://api.materialsdatafacility.org",
    "dev": "https://api-dev.materialsdatafacility.org",
    "local": "http://127.0.0.1:8080",
}


def _api_url_for_service(service_instance: str) -> str:
    override_url = os.environ.get("MDF_API_URL")
    if override_url:
        return override_url
    normalized = (service_instance or "prod").strip().lower()
    if normalized == "production":
        normalized = "prod"
    elif normalized == "development":
        normalized = "dev"
    return _V2_API_URLS.get(normalized, _V2_API_URLS["prod"])


class BackendClient:
    def __init__(self, base_url: str, token: Optional[str] = None, user_id: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=30.0)
        self._token = token
        self._user_id = user_id

    @classmethod
    def from_env(cls) -> "BackendClient":
        base_url = os.environ.get("MDF_API_URL", "http://127.0.0.1:8080")
        token = os.environ.get("MDF_CONNECT_TOKEN")
        user_id = os.environ.get("MDF_DEV_USER_ID")
        return cls(base_url=base_url, token=token, user_id=user_id)

    @classmethod
    def authenticated(
        cls,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> "BackendClient":
        """Create an authenticated backend client.

        Resolution order:
        1. Explicit token argument
        2. MDF_CONNECT_TOKEN env var
        3. dev_user_id / MDF_DEV_USER_ID env var (X-User-Id)
        4. Interactive Globus OAuth2 login
        """
        url = base_url or _api_url_for_service(service_instance)
        normalized_service = (service_instance or "prod").strip().lower()

        resolved_token = token or os.environ.get("MDF_CONNECT_TOKEN")
        if resolved_token:
            return cls(base_url=url, token=resolved_token)

        resolved_user_id = dev_user_id or os.environ.get("MDF_DEV_USER_ID")
        if not resolved_user_id and normalized_service == "local":
            resolved_user_id = os.environ.get("LOCAL_USER_ID")
        if resolved_user_id:
            return cls(base_url=url, user_id=resolved_user_id)

        if normalized_service == "local":
            # Local dev backends commonly run in AUTH_MODE=dev and can derive a default user.
            return cls(base_url=url)

        # Lazy import so token/dev-user workflows do not require globus_sdk.
        from mdf_agent.auth.globus import get_authorizer

        authorizer = get_authorizer(service_instance=service_instance)
        auth_header = authorizer.get_authorization_header()
        bearer_prefix = "Bearer "
        bearer_token = auth_header[len(bearer_prefix):] if auth_header.startswith(bearer_prefix) else auth_header
        return cls(base_url=url, token=bearer_token)

    def close(self) -> None:
        self._client.close()

    def submit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/submit", json_data=payload)

    def status(self, source_id: str, version: Optional[str] = None) -> Dict[str, Any]:
        params = {"version": version} if version else None
        return self._request("GET", f"/status/{source_id}", params=params)

    def submissions(self, organization: Optional[str] = None) -> Dict[str, Any]:
        params = {"organization": organization} if organization else None
        return self._request("GET", "/submissions", params=params)

    def update_status(self, source_id: str, version: str, status: str) -> Dict[str, Any]:
        payload = {"source_id": source_id, "version": version, "status": status}
        return self._request("POST", "/status/update", json_data=payload)

    def stream_create(self, title: str, lab_id: Optional[str] = None, organization: Optional[str] = None) -> Dict[str, Any]:
        payload = {"title": title}
        if lab_id:
            payload["lab_id"] = lab_id
        if organization:
            payload["organization"] = organization
        return self._request("POST", "/stream/create", json_data=payload)

    def stream_append(
        self,
        stream_id: str,
        files: Optional[Any] = None,
        file_count: Optional[int] = None,
        total_bytes: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"stream_id": stream_id}
        if files is not None:
            payload["files"] = files
        if file_count is not None:
            payload["file_count"] = file_count
        if total_bytes is not None:
            payload["total_bytes"] = total_bytes
        return self._request("POST", f"/stream/{stream_id}/append", json_data=payload)

    def stream_status(self, stream_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/stream/{stream_id}")

    def stream_close(self, stream_id: str) -> Dict[str, Any]:
        payload = {"stream_id": stream_id}
        return self._request("POST", f"/stream/{stream_id}/close", json_data=payload)

    def stream_snapshot(
        self,
        stream_id: str,
        title: Optional[str] = None,
        update: bool = False,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"stream_id": stream_id, "update": update}
        if title:
            payload["title"] = title
        return self._request("POST", f"/stream/{stream_id}/snapshot", json_data=payload)

    def stream_upload(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Upload a file to a stream.

        Args:
            stream_id: The stream ID
            filename: Name of the file
            content: File contents as bytes
            metadata: Optional metadata dict
        """
        import base64
        payload: Dict[str, Any] = {
            "filename": filename,
            "content_base64": base64.b64encode(content).decode("ascii"),
        }
        if metadata:
            payload["metadata"] = metadata
        return self._request("POST", f"/stream/{stream_id}/upload", json_data=payload)

    def stream_upload_batch(
        self,
        stream_id: str,
        files: list,
    ) -> Dict[str, Any]:
        """Upload multiple files to a stream.

        Args:
            stream_id: The stream ID
            files: List of dicts with filename, content (bytes), and optional metadata
        """
        import base64
        payload_files = []
        for f in files:
            pf: Dict[str, Any] = {
                "filename": f["filename"],
                "content_base64": base64.b64encode(f["content"]).decode("ascii"),
            }
            if f.get("metadata"):
                pf["metadata"] = f["metadata"]
            payload_files.append(pf)
        return self._request("POST", f"/stream/{stream_id}/upload", json_data={"files": payload_files})

    def stream_list_files(self, stream_id: str) -> Dict[str, Any]:
        """List all files in a stream."""
        return self._request("GET", f"/stream/{stream_id}/files")

    def stream_get_upload_url(
        self,
        stream_id: str,
        filename: str,
        content_type: str = "application/octet-stream",
        expires_in: int = 3600,
    ) -> Dict[str, Any]:
        """Get a pre-signed URL for direct file upload.

        For large files (> 6MB), use this to upload directly to storage
        instead of going through the API.

        Args:
            stream_id: The stream ID
            filename: Name of the file
            content_type: MIME type
            expires_in: URL expiration time in seconds

        Returns:
            Dict with url, method, headers, path, expires_in
        """
        payload = {
            "filename": filename,
            "content_type": content_type,
            "expires_in": expires_in,
        }
        return self._request("POST", f"/stream/{stream_id}/upload-url", json_data=payload)

    def stream_confirm_upload(
        self,
        stream_id: str,
        path: str,
        size_bytes: int,
        checksum_md5: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Confirm a direct upload completed.

        Call this after uploading directly to storage via the pre-signed URL.

        Args:
            stream_id: The stream ID
            path: The path returned from get_upload_url
            size_bytes: File size in bytes
            checksum_md5: MD5 checksum of the file
            metadata: Optional custom metadata
        """
        payload: Dict[str, Any] = {
            "path": path,
            "size_bytes": size_bytes,
            "checksum_md5": checksum_md5,
        }
        if metadata:
            payload["metadata"] = metadata
        return self._request("POST", f"/stream/{stream_id}/upload-confirm", json_data=payload)

    def stream_get_download_url(
        self,
        stream_id: str,
        path: str,
    ) -> Dict[str, Any]:
        """Get a download URL for a file.

        Args:
            stream_id: The stream ID
            path: Full path to the file

        Returns:
            Dict with download_url
        """
        return self._request("POST", f"/stream/{stream_id}/download-url", json_data={"path": path})

    def get_card(self, source_id: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Get a dataset preview card."""
        params = {"version": version} if version else None
        return self._request("GET", f"/card/{source_id}", params=params)

    def get_citation(
        self,
        source_id: str,
        format: str = "all",
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get citation for a dataset.

        Args:
            source_id: Dataset identifier
            format: Citation format (bibtex, ris, apa, datacite, all)
            version: Optional version
        """
        params = {"format": format}
        if version:
            params["version"] = version
        return self._request("GET", f"/citation/{source_id}", params=params)

    def search(
        self,
        query: str,
        search_type: str = "all",
        limit: int = 20,
    ) -> Dict[str, Any]:
        """Search datasets and streams.

        Args:
            query: Search query string
            search_type: "all", "datasets", or "streams"
            limit: Max results to return
        """
        params = {"q": query, "type": search_type, "limit": str(limit)}
        return self._request("GET", "/search", params=params)

    def stream_preview(
        self,
        stream_id: str,
        filename: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get preview of files in a stream.

        Args:
            stream_id: The stream ID
            filename: Optional specific file to preview

        Returns:
            Dict with preview data (columns, rows, structure, etc.)
        """
        if filename:
            return self._request("GET", f"/stream/{stream_id}/files/{filename}/preview")
        return self._request("GET", f"/stream/{stream_id}/preview")

    def stream_clone(
        self,
        stream_id: str,
        dest_dir: str = ".",
        file_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Clone a stream's files to local directory.

        This downloads all files from the stream (from Globus or other storage)
        to a local directory.

        Args:
            stream_id: The stream ID to clone
            dest_dir: Destination directory
            file_filter: Optional glob pattern to filter files (e.g., "*.csv")

        Returns:
            Dict with clone results
        """
        # This is a client-side operation, not an API call
        # Import clone module here to avoid circular imports
        import sys
        from pathlib import Path

        # Try to import from the aws backend
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "cs" / "aws"))
            from v2.clone import clone_stream
            return clone_stream(
                stream_id=stream_id,
                dest_dir=dest_dir,
                file_filter=file_filter,
                verbose=True,
            )
        except ImportError:
            return {"success": False, "error": "Clone module not available"}

    def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers: Dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        elif self._user_id:
            headers["X-User-Id"] = self._user_id
        response = self._client.request(method, url, json=json_data, params=params, headers=headers)
        try:
            payload = response.json()
        except Exception:
            return {"success": False, "error": f"Invalid response: {response.text}"}

        if isinstance(payload, dict) and "body" in payload and isinstance(payload["body"], str):
            try:
                return json.loads(payload["body"])
            except Exception:
                return payload
        return payload
