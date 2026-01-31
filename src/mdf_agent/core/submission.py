"""Submission building and publishing for MDF Connect.

This module handles:
1. Building submission payloads from manifest configuration
2. Resolving data sources (local paths, globs, remote URLs)
3. Submitting payloads to MDF Connect API

The submission payload format must match the MDF Connect API expectations
exactly. This is the critical compatibility layer.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import httpx

from mdf_agent.core.utils import deep_merge
from mdf_agent.models.config import DataSource, ManifestConfig
from mdf_agent.models.datacite import DataCite
from mdf_agent.models.submission import Submission

# MDF Connect API endpoints
CONNECT_SERVICE_LOC = "https://publish-prod.materialsdatafacility.org"
CONNECT_DEV_LOC = "https://publish-dev.materialsdatafacility.org"
CONNECT_EXTRACT_ROUTE = "/submit"


def _expand_data_source(
    source: DataSource,
    root: Path,
) -> List[str]:
    base = (root / source.path).resolve()
    if not source.include:
        return [str(base)]

    matches: List[Path] = []
    for pattern in source.include:
        matches.extend(base.glob(pattern))

    if source.exclude:
        excluded: List[Path] = []
        for pattern in source.exclude:
            excluded.extend(base.glob(pattern))
        matches = [match for match in matches if match not in excluded]

    return [str(match.resolve()) for match in matches]


def resolve_data_sources(
    data_sources: Iterable[str | DataSource],
    root: Path,
) -> List[str]:
    """Resolve data sources to absolute paths or URLs.

    Handles:
    - String URLs (globus://, https://) - passed through unchanged
    - String paths - resolved relative to root
    - DataSource objects - expanded with include/exclude patterns

    Args:
        data_sources: List of data source strings or DataSource objects.
        root: Root directory for resolving relative paths.

    Returns:
        List of resolved absolute paths or URLs.
    """
    resolved: List[str] = []
    for source in data_sources:
        if isinstance(source, DataSource):
            resolved.extend(_expand_data_source(source, root))
            continue
        if source.startswith("globus://") or source.startswith("https://"):
            resolved.append(source)
        else:
            resolved.append(str((root / source).resolve()))
    return resolved


def build_submission(
    manifest: ManifestConfig,
    root: Path,
    test: bool = False,
    update: bool = False,
) -> Dict[str, Any]:
    """Build a submission payload from manifest configuration.

    Constructs the complete JSON payload required by the MDF Connect API,
    including DataCite metadata, data sources, and all optional fields.

    Auto-discovered metadata (from extractors) is merged with manifest metadata,
    with explicit manifest values taking precedence.

    Args:
        manifest: The ManifestConfig containing dataset metadata.
        root: Root directory for resolving data source paths.
        test: If True, submit to test/sandbox environment.
        update: If True, this is an update to an existing dataset.

    Returns:
        Dict containing the complete submission payload.

    Raises:
        ValueError: If manifest is missing required fields (title, authors).
        json.JSONDecodeError: If payload contains invalid JSON (NaN, Infinity).
    """
    datacite = DataCite.from_manifest(manifest)
    dc_dict = datacite.to_dict()

    auto_metadata = manifest.auto_metadata or {}
    if auto_metadata.get("dc"):
        dc_dict = deep_merge(auto_metadata["dc"], dc_dict)

    mdf_block: Dict[str, Any] = {}
    if manifest.organization:
        mdf_block["organization"] = manifest.organization
    if manifest.acl:
        mdf_block["acl"] = manifest.acl

    if auto_metadata.get("mdf"):
        mdf_block = deep_merge(auto_metadata["mdf"], mdf_block)
    if manifest.mdf:
        mdf_block = deep_merge(mdf_block, manifest.mdf)

    custom_block = None
    if auto_metadata.get("custom"):
        custom_block = auto_metadata["custom"]
    if manifest.custom:
        custom_block = deep_merge(custom_block or {}, manifest.custom)

    data_sources = resolve_data_sources(manifest.data_sources, root)

    submission = Submission(
        dc=dc_dict,
        data_sources=data_sources,
        test=test,
        update=update,
        mdf=mdf_block,
        mrr=manifest.mrr,
        custom=custom_block,
        projects=manifest.projects,
        data_destinations=manifest.data_destinations,
        external_uri=manifest.external_uri,
        index=manifest.index,
        extraction_config=manifest.extraction_config,
        services=manifest.services,
        tags=manifest.tags,
        links=manifest.links,
        no_extract=manifest.no_extract,
        dataset_acl=manifest.dataset_acl,
        update_metadata_only=bool(manifest.update_metadata_only),
    )

    payload = submission.to_payload()
    json.dumps(payload, allow_nan=False)
    return payload


def submit_submission(
    payload: Dict[str, Any],
    authorizer: Optional[Any] = None,
    service_instance: str = "prod",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """Submit a payload to MDF Connect.

    Posts the submission payload to the MDF Connect API and handles
    authentication, retries on auth failure, and response parsing.

    Args:
        payload: The submission payload (from build_submission).
        authorizer: Globus authorizer for authentication. If None, submits
            without authentication (will likely fail).
        service_instance: "prod" for production, "dev" for development.
        timeout: HTTP request timeout in seconds.

    Returns:
        Dict with keys:
        - success: bool indicating if submission succeeded
        - source_id: str with dataset ID (if successful)
        - error: str with error message (if failed)
        - response: dict with full API response (if successful)
    """
    if service_instance in ("prod", "production", None):
        service_loc = CONNECT_SERVICE_LOC
    elif service_instance in ("dev", "development"):
        service_loc = CONNECT_DEV_LOC
    else:
        raise ValueError("service_instance must be 'prod' or 'dev'")

    headers: Dict[str, str] = {}
    if authorizer is not None:
        headers["Authorization"] = authorizer.get_authorization_header()

    url = f"{service_loc}{CONNECT_EXTRACT_ROUTE}"
    with httpx.Client(timeout=timeout) as client:
        res = client.post(url, json=payload, headers=headers)
        if res.status_code in (401, 403) and authorizer is not None:
            authorizer.handle_missing_authorization()
            headers["Authorization"] = authorizer.get_authorization_header()
            res = client.post(url, json=payload, headers=headers)

    try:
        data = res.json()
    except Exception:
        return {
            "success": False,
            "source_id": None,
            "error": f"Error decoding {res.status_code} response: {res.text}",
        }

    if res.status_code < 300:
        return {"success": True, "source_id": data.get("source_id"), "response": data}

    return {
        "success": False,
        "source_id": None,
        "error": f"Error {res.status_code} submitting dataset: {data}",
    }
