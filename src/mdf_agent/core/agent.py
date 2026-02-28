"""MDF Agent - Primary Python API.

This module provides the main MDFAgent class, which is the primary interface
for interacting with MDF datasets programmatically. It supports two modes:

1. Repository mode: Work with a git-style .mdf/ repository for tracking changes
2. Direct mode: Build and submit datasets without local state

Examples:
    Repository mode::

        agent = MDFAgent.init("./my_dataset", title="My Dataset", authors=["Jane Doe"])
        agent.add("data/*.csv", discover=True)
        agent.commit("Add experimental data")
        result = agent.publish(test=True)

    Direct mode::

        agent = MDFAgent()
        agent.set_title("My Dataset")
        agent.add_author("Jane Doe", affiliations=["MIT"])
        agent.add_data_source("globus://endpoint/path")
        result = agent.publish()
"""

from __future__ import annotations

import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from mdf_agent.core.manifest import load_manifest, save_manifest
from mdf_agent.core.repository import Repository
from mdf_agent.core.submission import build_submission
from mdf_agent.core.validation import validate_manifest
from mdf_agent.core.backend_client import BackendClient
from mdf_agent.extractors.registry import discover_metadata
from mdf_agent.models.config import Author, ManifestConfig


_MDF_HTTPS_BASE = "https://data.materialsdatafacility.org"
_NCSA_MDF_COLLECTION_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"


_UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
_ZIP_MAX_TOTAL_BYTES = 12 * 1024 * 1024 * 1024  # 12 GB
_INSECURE_SSL_WARNING_EMITTED = False


class UploadResult(NamedTuple):
    data_sources: List[str]
    archive_url: Optional[str]
    archive_size: Optional[int]


def _resolve_upload_tls_verify() -> bool | str:
    raw_value = os.environ.get("MDF_SSL_VERIFY")
    if raw_value is None or not raw_value.strip():
        return True

    normalized = raw_value.strip().lower()
    if normalized in ("true", "1", "yes"):
        return True
    if normalized in ("false", "0", "no"):
        global _INSECURE_SSL_WARNING_EMITTED
        if not _INSECURE_SSL_WARNING_EMITTED:
            print(
                "Warning: MDF_SSL_VERIFY=false disables TLS certificate verification for MDF uploads.",
                file=sys.stderr,
            )
            _INSECURE_SSL_WARNING_EMITTED = True
        return False
    return raw_value


def _mkdir_on_collection(
    path: str,
    transfer_token: str,
) -> None:
    """Create a directory on the MDF Globus collection via the Transfer API."""
    import httpx

    url = f"https://transfer.api.globus.org/v0.10/operation/endpoint/{_NCSA_MDF_COLLECTION_UUID}/mkdir"
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            url,
            json={"DATA_TYPE": "mkdir", "path": path},
            headers={"Authorization": f"Bearer {transfer_token}"},
        )
        # 502 "already exists" is fine
        if resp.status_code in (200, 201, 202) or "already exists" in resp.text.lower():
            return
        resp.raise_for_status()


def _create_zip_archive(
    local_dirs: List[tuple[Path, str]],
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> Optional[Path]:
    """Create a zip archive from one or more local directories.

    Args:
        local_dirs: List of (resolved_dir_path, prefix_in_zip) pairs.
        progress_callback: Optional progress callback.

    Returns:
        Path to temp zip file, or None if skipped (too large / empty).
    """
    # Collect all files and compute total size
    file_entries: List[tuple[Path, str]] = []  # (absolute_path, arcname)
    total_size = 0

    for dir_path, prefix in local_dirs:
        for file_path in sorted(dir_path.rglob("*")):
            if not file_path.is_file():
                continue
            # Symlink safety: skip if target resolves outside the source dir
            if file_path.is_symlink():
                try:
                    resolved = file_path.resolve()
                    if not str(resolved).startswith(str(dir_path.resolve())):
                        continue
                except (OSError, ValueError):
                    continue
            relative = file_path.relative_to(dir_path)
            arcname = f"{prefix}/{relative}" if prefix else str(relative)
            total_size += file_path.stat().st_size
            file_entries.append((file_path, arcname))

    if not file_entries:
        return None

    if total_size > _ZIP_MAX_TOTAL_BYTES:
        print(
            f"Warning: Skipping zip archive — total size ({total_size / (1024**3):.1f} GB) "
            f"exceeds {_ZIP_MAX_TOTAL_BYTES / (1024**3):.0f} GB limit.",
            file=sys.stderr,
        )
        return None

    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()
    tmp_path = Path(tmp.name)

    bytes_written = 0
    with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path, arcname in file_entries:
            zf.write(file_path, arcname)
            bytes_written += file_path.stat().st_size
            if progress_callback:
                progress_callback("Creating archive", bytes_written, total_size)

    return tmp_path


def _upload_local_files(
    data_sources: List[str],
    data_token: str,
    source_id: Optional[str] = None,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
    transfer_token: Optional[str] = None,
) -> UploadResult:
    """Upload local file paths to MDF HTTPS storage, returning updated source list.

    Local paths are uploaded via HTTPS PUT to the MDF Globus collection and
    replaced with ``globus://`` URIs. Non-local sources pass through unchanged.
    When directories are uploaded, a zip archive is also created and uploaded
    to ``{upload_prefix}/.mdf/data.zip``.

    Args:
        data_sources: List of data source paths/URIs.
        data_token: Globus HTTPS bearer token.
        source_id: If available, upload to ``/mdf_open/{source_id}/`` for
            deterministic paths. Falls back to ``/tmp/_uploads/{uuid}/``.
        progress_callback: Optional ``(filename, bytes_sent, total_bytes)`` callback.
        transfer_token: Globus Transfer API token, used to mkdir before upload.
    """
    import uuid

    updated: List[str] = []
    local_dirs: List[tuple[Path, str]] = []

    if source_id:
        upload_prefix = f"/mdf_open/{source_id}"
    else:
        upload_id = uuid.uuid4().hex[:8]
        upload_prefix = f"/tmp/_uploads/{upload_id}"

    # Create the upload directory on the collection via Transfer API
    if transfer_token:
        _mkdir_on_collection(upload_prefix + "/", transfer_token)

    # Count directories to determine prefix logic
    dir_count = sum(1 for s in data_sources if not s.startswith(("globus://", "https://", "http://", "stream://")) and Path(s).is_dir())

    for source in data_sources:
        # Skip anything that's already a URL
        if source.startswith(("globus://", "https://", "http://", "stream://")):
            updated.append(source)
            continue

        local_path = Path(source)
        if not local_path.exists():
            # Keep as-is — server will validate
            updated.append(source)
            continue

        if local_path.is_dir():
            # Track for zip archive creation
            prefix = local_path.name if dir_count > 1 else ""
            local_dirs.append((local_path.resolve(), prefix))
            # Upload directory contents recursively
            for file_path in sorted(local_path.rglob("*")):
                if file_path.is_file():
                    relative = file_path.relative_to(local_path)
                    dest_path = f"{upload_prefix}/{relative}"
                    uri = _https_put_file(file_path, dest_path, data_token, progress_callback=progress_callback)
                    if uri:
                        updated.append(uri)
        else:
            dest_path = f"{upload_prefix}/{local_path.name}"
            uri = _https_put_file(local_path, dest_path, data_token, progress_callback=progress_callback)
            if uri:
                updated.append(uri)

    # Create and upload zip archive for local directories
    archive_url: Optional[str] = None
    archive_size: Optional[int] = None
    if local_dirs:
        zip_path = _create_zip_archive(local_dirs, progress_callback=progress_callback)
        if zip_path is not None:
            try:
                archive_size = zip_path.stat().st_size
                mdf_dir = f"{upload_prefix}/.mdf"
                if transfer_token:
                    _mkdir_on_collection(mdf_dir + "/", transfer_token)
                dest = f"{mdf_dir}/data.zip"
                uri = _https_put_file(zip_path, dest, data_token, progress_callback=progress_callback)
                if uri:
                    archive_url = uri
            finally:
                zip_path.unlink(missing_ok=True)

    sources = updated if updated else data_sources
    return UploadResult(data_sources=sources, archive_url=archive_url, archive_size=archive_size)


_UPLOAD_MAX_RETRIES = 3
_UPLOAD_RETRY_STATUSES = {502, 503, 504}


def _https_put_file(
    local_path: Path,
    dest_path: str,
    data_token: str,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> Optional[str]:
    """Upload a single file via streaming HTTPS PUT and return its globus:// URI.

    Reads the file in 8 MB chunks to avoid loading entire files into memory.
    Retries on 502/503/504 and connection errors up to 3 times.
    """
    import time
    import httpx

    url = f"{_MDF_HTTPS_BASE}{dest_path}"
    file_size = local_path.stat().st_size
    ssl_verify = _resolve_upload_tls_verify()

    def file_stream():
        bytes_sent = 0
        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(_UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                bytes_sent += len(chunk)
                if progress_callback:
                    progress_callback(local_path.name, bytes_sent, file_size)
                yield chunk

    timeout = httpx.Timeout(connect=30, read=300, write=300, pool=30)

    for attempt in range(_UPLOAD_MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=timeout, verify=ssl_verify) as client:
                resp = client.put(
                    url,
                    content=file_stream(),
                    headers={
                        "Authorization": f"Bearer {data_token}",
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(file_size),
                    },
                )
                if resp.status_code in (200, 201, 204):
                    return f"globus://{_NCSA_MDF_COLLECTION_UUID}{dest_path}"
                if resp.status_code in _UPLOAD_RETRY_STATUSES and attempt < _UPLOAD_MAX_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(
                    f"Failed to upload {local_path.name}: HTTP {resp.status_code} {resp.text}"
                )
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            if attempt < _UPLOAD_MAX_RETRIES:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(
                f"Failed to upload {local_path.name} after {_UPLOAD_MAX_RETRIES + 1} attempts: {exc}"
            ) from exc
    return None


class MDFAgent:
    """Primary Python API for MDF Agent.

    This class provides a high-level interface for creating, managing, and
    publishing datasets to the Materials Data Facility (MDF).

    Attributes:
        root: Path to the dataset directory (repository mode) or None (direct mode).
        repo: Repository instance for git-style operations, or None.
        manifest: The ManifestConfig containing dataset metadata.
    """

    def __init__(self, root: Optional[Path] = None, manifest: Optional[ManifestConfig] = None):
        self.root = root
        self.repo: Optional[Repository] = None
        if root is not None:
            self.repo = Repository.load(root)
            self.manifest = self.repo.load_manifest()
        else:
            self.manifest = manifest or ManifestConfig()

    @classmethod
    def init(
        cls,
        path: str,
        title: str,
        authors: List[str],
        description: Optional[str] = None,
        publisher: Optional[str] = None,
        publication_year: Optional[int] = None,
    ) -> "MDFAgent":
        root = Path(path).resolve()
        repo = Repository.init_repo(
            root,
            title=title,
            authors=authors,
            description=description,
            publisher=publisher,
            publication_year=publication_year,
        )
        agent = cls(root=root)
        agent.repo = repo
        agent.manifest = repo.load_manifest()
        return agent

    @classmethod
    def from_repo(cls, path: str) -> "MDFAgent":
        root = Path(path).resolve()
        return cls(root=root)

    def save_manifest(self) -> None:
        if self.repo is None:
            raise ValueError("No repository attached")
        self.repo.save_manifest(self.manifest)

    def add(self, *paths: str, discover: Optional[bool] = None) -> List[str]:
        if self.repo is None:
            raise ValueError("Repository mode required for add")
        staged = self.repo.stage(paths)

        if discover or (discover is None and self.manifest.auto_discover):
            files = [str((self.root / path).resolve()) for path in staged]
            extracted = discover_metadata(files)
            if extracted:
                current = self.manifest.auto_metadata or {}
                current.update(extracted)
                self.manifest.auto_metadata = current
                self.save_manifest()
        return staged

    def commit(self, message: str) -> Dict[str, Any]:
        if self.repo is None:
            raise ValueError("Repository mode required for commit")
        commit = self.repo.commit(message)
        return commit.model_dump()

    def status(self) -> Dict[str, Any]:
        if self.repo is None:
            raise ValueError("Repository mode required for status")
        return self.repo.state.model_dump()

    def validate(self) -> Dict[str, List[str]]:
        has_committed = bool(
            self.repo and any(c.staged_files for c in self.repo.state.commits)
        )
        errors, warnings = validate_manifest(self.manifest, has_committed_files=has_committed)
        return {"errors": errors, "warnings": warnings}

    def build_submission(self, test: bool = False, update: bool = False) -> Dict[str, Any]:
        root = self.root or Path.cwd()
        return build_submission(self.manifest, root, test=test, update=update)

    def publish(
        self,
        test: bool = False,
        update: bool = False,
        dry_run: bool = True,
        token: Optional[str] = None,
        service_instance: str = "prod",
        api_url: Optional[str] = None,
        dev_user_id: Optional[str] = None,
        authorizer: Optional[Any] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> Dict[str, Any]:
        payload = self.build_submission(test=test, update=update)
        if dry_run:
            return {"success": True, "payload": payload}

        # Backward compatibility: extract a bearer token if a legacy authorizer is passed.
        if authorizer is not None and token is None:
            header = authorizer.get_authorization_header()
            if isinstance(header, str):
                bearer_prefix = "Bearer "
                token = header[len(bearer_prefix):] if header.startswith(bearer_prefix) else header

        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        try:
            # Upload local files to MDF storage via HTTPS and replace
            # paths with globus:// URIs before submitting.
            data_sources = payload.get("data_sources", [])
            if data_sources and client._globus_data_token:
                # Use source_id for deterministic upload paths when available
                ext = payload.get("extensions", {})
                upload_source_id = ext.get("mdf_source_id") or ext.get("mdf_source_name")
                upload_result = _upload_local_files(
                    data_sources,
                    client._globus_data_token,
                    source_id=upload_source_id,
                    progress_callback=progress_callback,
                    transfer_token=client._globus_transfer_token,
                )
                payload["data_sources"] = upload_result.data_sources
                if upload_result.archive_url:
                    payload["download_url"] = upload_result.archive_url
                if upload_result.archive_size:
                    payload["archive_size"] = upload_result.archive_size
            return client.submit(payload)
        finally:
            client.close()

    def set_title(self, title: str) -> None:
        self.manifest.title = title

    def add_author(self, name: str, affiliations: Optional[List[str]] = None, orcid: Optional[str] = None) -> None:
        if self.manifest.authors is None:
            self.manifest.authors = []
        author = Author(name=name, affiliations=affiliations or [], orcid=orcid)
        self.manifest.authors.append(author)

    def add_data_source(self, source: str) -> None:
        if self.manifest.data_sources is None:
            self.manifest.data_sources = []
        self.manifest.data_sources.append(source)

    # Search, curation, and discovery helpers

    def search(
        self,
        query: str,
        search_type: str = "all",
        limit: int = 20,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Search datasets and streams."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.search(query, search_type=search_type, limit=limit)
        client.close()
        return result

    def pending(
        self,
        limit: int = 50,
        organization: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List datasets pending curation."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.curation_pending(limit=limit, organization=organization)
        client.close()
        return result

    def approve(
        self,
        source_id: str,
        mint_doi: bool = True,
        notes: Optional[str] = None,
        version: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Approve a dataset for publication."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.curation_approve(source_id=source_id, mint_doi=mint_doi, notes=notes, version=version)
        client.close()
        return result

    def reject(
        self,
        source_id: str,
        reason: str,
        suggestions: Optional[str] = None,
        version: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reject a dataset and return to submitter."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.curation_reject(source_id=source_id, reason=reason, suggestions=suggestions, version=version)
        client.close()
        return result

    def versions(
        self,
        source_id: str,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get version history for a dataset."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.versions(source_id)
        client.close()
        return result

    def show(
        self,
        source_id: str,
        version: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get dataset preview card."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.get_card(source_id, version=version)
        client.close()
        return result

    def cite(
        self,
        source_id: str,
        format: str = "all",
        version: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get citation for a dataset."""
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.get_citation(source_id, format=format, version=version)
        client.close()
        return result

    # Streaming helpers
    def stream_create(
        self,
        title: str,
        lab_id: Optional[str] = None,
        organization: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ):
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.stream_create(title, lab_id=lab_id, organization=organization)
        client.close()
        return result

    def stream_append(
        self,
        stream_id: str,
        files: Optional[Any] = None,
        file_count: Optional[int] = None,
        total_bytes: Optional[int] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ):
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.stream_append(stream_id, files=files, file_count=file_count, total_bytes=total_bytes)
        client.close()
        return result

    def stream_status(
        self,
        stream_id: str,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ):
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.stream_status(stream_id)
        client.close()
        return result

    def stream_close(
        self,
        stream_id: str,
        mint_doi: Optional[bool] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        authors: Optional[list] = None,
        keywords: Optional[list] = None,
        license: Optional[str] = None,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ):
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.stream_close(
            stream_id=stream_id,
            mint_doi=mint_doi,
            title=title,
            description=description,
            authors=authors,
            keywords=keywords,
            license=license,
        )
        client.close()
        return result

    def stream_snapshot(
        self,
        stream_id: str,
        title: Optional[str] = None,
        update: bool = False,
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        service_instance: str = "prod",
        dev_user_id: Optional[str] = None,
    ):
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service_instance,
            dev_user_id=dev_user_id,
        )
        result = client.stream_snapshot(stream_id, title=title, update=update)
        client.close()
        return result
