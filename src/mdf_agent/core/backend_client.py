from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote, unquote, urlparse

import httpx

_V2_API_URLS = {
    "prod": "https://api.materialsdatafacility.org",
    "dev": "https://api-dev.materialsdatafacility.org",
    "staging": "https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging",
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
    url = _V2_API_URLS.get(normalized, "")
    if not url:
        raise ValueError(
            f"No API URL configured for service '{normalized}'. "
            f"Use --api-url or set MDF_API_URL environment variable."
        )
    return url


class BackendClient:
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        user_id: Optional[str] = None,
        globus_data_token: Optional[str] = None,
        globus_transfer_token: Optional[str] = None,
        mdf_connect_token: Optional[str] = None,
        groups_token: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=30.0)
        self._token = token
        self._user_id = user_id
        self._globus_data_token = globus_data_token
        self._mdf_connect_token = mdf_connect_token
        self._globus_transfer_token = globus_transfer_token
        self._groups_token = groups_token

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
        3. MDF_CLIENT_ID + MDF_CLIENT_SECRET env vars (confidential client flow)
        4. dev_user_id / MDF_DEV_USER_ID env var (X-User-Id)
        5. Interactive Globus OAuth2 login
        """
        url = base_url or _api_url_for_service(service_instance)
        normalized_service = (service_instance or "prod").strip().lower()

        resolved_token = token or os.environ.get("MDF_CONNECT_TOKEN")
        if resolved_token:
            return cls(base_url=url, token=resolved_token)

        confidential_client_id = os.environ.get("MDF_CLIENT_ID")
        confidential_client_secret = os.environ.get("MDF_CLIENT_SECRET")
        if confidential_client_id and confidential_client_secret:
            # Lazy import so token/dev-user workflows do not require globus_sdk.
            import globus_sdk

            from mdf_agent.auth.globus import (
                DATA_MDF_SCOPE,
                NCSA_MDF_COLLECTION_UUID,
                TRANSFER_SCOPE,
                get_scopes_for_service,
            )

            scope, resource_server = get_scopes_for_service(service_instance)
            confidential_client = globus_sdk.ConfidentialAppAuthClient(
                confidential_client_id,
                confidential_client_secret,
            )
            token_response = confidential_client.oauth2_client_credentials_tokens(
                requested_scopes=f"{scope} {DATA_MDF_SCOPE} {TRANSFER_SCOPE}",
            )
            by_resource_server = getattr(token_response, "by_resource_server", {}) or {}

            def _extract_access_token(token_entry: Any) -> str:
                if not token_entry:
                    return ""
                if isinstance(token_entry, dict):
                    return token_entry.get("access_token", "")
                return getattr(token_entry, "access_token", "")

            service_token = _extract_access_token(by_resource_server.get(resource_server))
            data_token = _extract_access_token(by_resource_server.get(NCSA_MDF_COLLECTION_UUID))
            transfer_token = _extract_access_token(by_resource_server.get("transfer.api.globus.org"))
            if service_token:
                return cls(base_url=url, token=service_token, globus_data_token=data_token or None, globus_transfer_token=transfer_token or None)

        resolved_user_id = dev_user_id or os.environ.get("MDF_DEV_USER_ID")
        if not resolved_user_id and normalized_service == "local":
            resolved_user_id = os.environ.get("LOCAL_USER_ID")
        if resolved_user_id:
            return cls(base_url=url, user_id=resolved_user_id)

        if normalized_service == "local":
            # Local dev backends commonly run in AUTH_MODE=dev and can derive a default user.
            return cls(base_url=url)

        # Lazy import so token/dev-user workflows do not require globus_sdk.
        from mdf_agent.auth.globus import get_authorizer_for_scopes, get_scopes_for_service, DATA_MDF_SCOPE, TRANSFER_SCOPE, GROUPS_SCOPE, NCSA_MDF_COLLECTION_UUID

        scope, _resource_server = get_scopes_for_service(service_instance)
        authorizers = get_authorizer_for_scopes(
            [scope, DATA_MDF_SCOPE, TRANSFER_SCOPE, GROUPS_SCOPE],
        )

        bearer_prefix = "Bearer "

        def _extract(authorizer):
            if not authorizer:
                return ""
            h = authorizer.get_authorization_header()
            return h[len(bearer_prefix):] if h.startswith(bearer_prefix) else h

        # openid token for userinfo() identity check
        openid_token = _extract(authorizers.get("auth.globus.org"))
        # MDF Connect token for dependent token exchange (groups, transfer)
        mdf_connect_token = _extract(authorizers.get(_resource_server))

        # Data token for Globus HTTPS file operations (X-Globus-Token header)
        # The resource server key is the collection UUID, not the hostname.
        data_token = _extract(authorizers.get(NCSA_MDF_COLLECTION_UUID))

        # Transfer token for mkdir operations before HTTPS uploads
        transfer_token = _extract(authorizers.get("transfer.api.globus.org"))

        # Groups token for direct group membership checks
        groups_token = _extract(authorizers.get("groups.api.globus.org"))

        return cls(base_url=url, token=openid_token, globus_data_token=data_token or None, globus_transfer_token=transfer_token or None, mdf_connect_token=mdf_connect_token or None, groups_token=groups_token or None)

    def close(self) -> None:
        self._client.close()

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/health")

    def auth_check(self) -> Dict[str, Any]:
        return self._request("GET", "/auth/check")

    def submit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/submit", json_data=payload)

    def status(self, source_id: str, version: Optional[str] = None) -> Dict[str, Any]:
        params = {"version": version} if version else None
        return self._request("GET", f"/status/{source_id}", params=params)

    def submissions(self, organization: Optional[str] = None) -> Dict[str, Any]:
        params = {"organization": organization} if organization else None
        return self._request("GET", "/submissions", params=params)

    def curation_pending(
        self,
        limit: int = 50,
        offset: int = 0,
        organization: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if organization:
            params["organization"] = organization
        return self._request("GET", "/curation/pending", params=params)

    def curation_detail(self, source_id: str, version: Optional[str] = None) -> Dict[str, Any]:
        params = {"version": version} if version else None
        return self._request("GET", f"/curation/{source_id}", params=params)

    def curation_approve(
        self,
        source_id: str,
        mint_doi: bool = True,
        notes: Optional[str] = None,
        metadata_updates: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"mint_doi": mint_doi}
        if notes is not None:
            payload["notes"] = notes
        if metadata_updates is not None:
            payload["metadata_updates"] = metadata_updates
        if version:
            payload["version"] = version
        return self._request("POST", f"/curation/{source_id}/approve", json_data=payload)

    def curation_reject(
        self,
        source_id: str,
        reason: str,
        suggestions: Optional[str] = None,
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"reason": reason}
        if suggestions is not None:
            payload["suggestions"] = suggestions
        if version:
            payload["version"] = version
        return self._request("POST", f"/curation/{source_id}/reject", json_data=payload)

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

    def stream_close(
        self,
        stream_id: str,
        mint_doi: Optional[bool] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        authors: Optional[list] = None,
        keywords: Optional[list] = None,
        license: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"stream_id": stream_id}
        if mint_doi is not None:
            payload["mint_doi"] = mint_doi
        if title:
            payload["title"] = title
        if description:
            payload["description"] = description
        if authors is not None:
            payload["authors"] = authors
        if keywords is not None:
            payload["keywords"] = keywords
        if license:
            payload["license"] = license
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

    def versions(self, source_id: str, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """Get version history for a dataset."""
        params: Dict[str, Any] = {}
        if limit != 50:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        return self._request("GET", f"/versions/{source_id}", params=params or None)

    def edit_metadata(
        self,
        source_id: str,
        version: Optional[str] = None,
        **fields,
    ) -> Dict[str, Any]:
        """Edit metadata on a submission."""
        payload: Dict[str, Any] = {k: v for k, v in fields.items() if v is not None}
        if version:
            payload["version"] = version
        return self._request("POST", f"/submissions/{source_id}/metadata", json_data=payload)

    def withdraw(
        self,
        source_id: str,
        reason: str = "",
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Withdraw a pending_curation submission."""
        payload: Dict[str, Any] = {"reason": reason}
        if version:
            payload["version"] = version
        return self._request("POST", f"/submissions/{source_id}/withdraw", json_data=payload)

    def resubmit(
        self,
        source_id: str,
        notes: str = "",
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resubmit a rejected submission."""
        payload: Dict[str, Any] = {"notes": notes}
        if version:
            payload["version"] = version
        return self._request("POST", f"/submissions/{source_id}/resubmit", json_data=payload)

    def version_diff(
        self,
        source_id: str,
        from_version: str,
        to_version: str,
    ) -> Dict[str, Any]:
        """Get a structured diff between two versions."""
        params = {"from": from_version, "to": to_version}
        return self._request("GET", f"/versions/{source_id}/diff", params=params)

    def delete_submission(
        self,
        source_id: str,
        reason: str,
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Soft-delete a submission (curator-only)."""
        payload: Dict[str, Any] = {"reason": reason}
        if version:
            payload["version"] = version
        return self._request("POST", f"/submissions/{source_id}/delete", json_data=payload)

    def admin_stats(self) -> Dict[str, Any]:
        """Get admin statistics (curator-only)."""
        return self._request("GET", "/admin/stats")

    def dataset_stats(self, source_id: str) -> Dict[str, Any]:
        """Get access/download stats for a published dataset."""
        return self._request("GET", f"/stats/{source_id}")

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

    def dataset_preview(self, source_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/preview/{source_id}")

    def dataset_files(self, source_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/preview/{source_id}/files")

    def dataset_file_detail(self, source_id: str, path: str) -> Dict[str, Any]:
        encoded_path = quote(path, safe="/")
        return self._request("GET", f"/preview/{source_id}/files/{encoded_path}")

    def dataset_sample(self, source_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/preview/{source_id}/sample")

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
        import fnmatch

        files_result = self.stream_list_files(stream_id)
        if not files_result.get("success"):
            return {
                "success": False,
                "stream_id": stream_id,
                "error": files_result.get("error", "Failed to list stream files"),
            }

        destination = Path(dest_dir).expanduser().resolve()
        destination.mkdir(parents=True, exist_ok=True)

        downloaded = []
        errors = []

        for file_info in files_result.get("files", []):
            path = str(file_info.get("path") or "")
            filename = str(file_info.get("filename") or Path(path).name)
            if not path:
                errors.append({"file": filename or "<unknown>", "error": "Missing file path"})
                continue

            if file_filter and not (
                fnmatch.fnmatch(filename, file_filter) or fnmatch.fnmatch(path, file_filter)
            ):
                continue

            download_url_result = self.stream_get_download_url(stream_id, path)
            if not download_url_result.get("success"):
                errors.append({
                    "file": filename,
                    "error": download_url_result.get("error", "Failed to get download URL"),
                })
                continue

            download_url = str(download_url_result.get("download_url") or "")
            if not download_url:
                errors.append({"file": filename, "error": "No download URL returned"})
                continue

            try:
                content = self._download_stream_file(download_url, file_info)
            except Exception as exc:
                errors.append({"file": filename, "error": str(exc)})
                continue

            target_path = self._safe_output_path(destination, filename)
            if target_path is None:
                errors.append({"file": filename, "error": "Unsafe destination filename"})
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(content)
            downloaded.append({
                "filename": filename,
                "path": str(target_path),
                "size_bytes": len(content),
            })

        return {
            "success": len(errors) == 0,
            "stream_id": stream_id,
            "destination": str(destination),
            "downloaded": len(downloaded),
            "files": downloaded,
            "errors": errors if errors else None,
        }

    def _safe_output_path(self, destination: Path, filename: str) -> Optional[Path]:
        normalized = (filename or "").replace("\\", "/").lstrip("/")
        if not normalized:
            return None
        candidate = (destination / normalized).resolve()
        try:
            candidate.relative_to(destination)
        except ValueError:
            return None
        return candidate

    def _download_stream_file(self, download_url: str, file_info: Dict[str, Any]) -> bytes:
        parsed = urlparse(download_url)
        if parsed.scheme == "file":
            return Path(unquote(parsed.path)).read_bytes()

        headers: Optional[Dict[str, str]] = None
        storage_backend = str(file_info.get("storage_backend") or "").lower()
        host = (parsed.hostname or "").lower()
        if storage_backend == "globus" or host.endswith("materialsdatafacility.org"):
            token = self._globus_data_token or self._token
            if token:
                headers = {"Authorization": f"Bearer {token}"}

        response = self._client.get(download_url, headers=headers)
        response.raise_for_status()
        return response.content

    _RETRY_STATUSES = {429, 502, 503, 504}
    _MAX_RETRIES = 3

    def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        import time

        url = f"{self.base_url}{path}"
        headers: Dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        elif self._user_id:
            headers["X-User-Id"] = self._user_id
        if self._globus_data_token:
            headers["X-Globus-Token"] = self._globus_data_token
        if self._mdf_connect_token:
            headers["X-MDF-Token"] = self._mdf_connect_token
        if self._groups_token:
            headers["X-Groups-Token"] = self._groups_token

        last_exc: Optional[Exception] = None
        for attempt in range(self._MAX_RETRIES + 1):
            try:
                response = self._client.request(method, url, json=json_data, params=params, headers=headers)
            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                last_exc = exc
                if attempt < self._MAX_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
                return {"success": False, "error": f"Connection failed after {self._MAX_RETRIES + 1} attempts: {exc}"}

            if response.status_code in self._RETRY_STATUSES and attempt < self._MAX_RETRIES:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = 2 ** attempt
                else:
                    delay = 2 ** attempt
                time.sleep(delay)
                continue

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

        return {"success": False, "error": f"Request failed after {self._MAX_RETRIES + 1} attempts"}
