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

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from mdf_agent.core.manifest import load_manifest, save_manifest
from mdf_agent.core.repository import Repository
from mdf_agent.core.submission import build_submission
from mdf_agent.core.validation import validate_manifest
from mdf_agent.core.backend_client import BackendClient
from mdf_agent.extractors.registry import discover_metadata
from mdf_agent.models.config import Author, ManifestConfig


_MDF_HTTPS_BASE = "https://g-456d30.dd271.03c0.data.globus.org"
_NCSA_MDF_COLLECTION_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"


_UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def _upload_local_files(
    data_sources: List[str],
    data_token: str,
    source_id: Optional[str] = None,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> List[str]:
    """Upload local file paths to MDF HTTPS storage, returning updated source list.

    Local paths are uploaded via HTTPS PUT to the MDF Globus collection and
    replaced with ``globus://`` URIs. Non-local sources pass through unchanged.

    Args:
        data_sources: List of data source paths/URIs.
        data_token: Globus HTTPS bearer token.
        source_id: If available, upload to ``/mdf_open/{source_id}/`` for
            deterministic paths. Falls back to ``/mdf_open/_uploads/{uuid}/``.
        progress_callback: Optional ``(filename, bytes_sent, total_bytes)`` callback.
    """
    import uuid

    updated: List[str] = []

    if source_id:
        upload_prefix = f"/mdf_open/{source_id}"
    else:
        upload_id = uuid.uuid4().hex[:8]
        upload_prefix = f"/mdf_open/_uploads/{upload_id}"

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

    return updated if updated else data_sources


def _https_put_file(
    local_path: Path,
    dest_path: str,
    data_token: str,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> Optional[str]:
    """Upload a single file via streaming HTTPS PUT and return its globus:// URI.

    Reads the file in 8 MB chunks to avoid loading entire files into memory.
    """
    import httpx

    url = f"{_MDF_HTTPS_BASE}{dest_path}"
    file_size = local_path.stat().st_size

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
    with httpx.Client(timeout=timeout) as client:
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
        raise RuntimeError(
            f"Failed to upload {local_path.name}: HTTP {resp.status_code} {resp.text}"
        )


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
                payload["data_sources"] = _upload_local_files(
                    data_sources,
                    client._globus_data_token,
                    source_id=upload_source_id,
                    progress_callback=progress_callback,
                )
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
