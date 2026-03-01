"""Submission building and publishing for MDF Connect.

This module handles:
1. Building submission payloads from manifest configuration
2. Resolving data sources (local paths, globs, remote URLs)
3. Submitting payloads to MDF Connect API

The submission payload uses the flat v2 metadata format.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse, parse_qs, unquote

from mdf_agent.auth.globus import NCSA_MDF_COLLECTION_UUID
from mdf_agent.models.config import DataSource, ManifestConfig
from mdf_agent.models.submission import Submission


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


def normalize_data_source(url: str) -> str:
    """Normalize a data source URL to canonical format.

    Converts Globus File Manager URLs and MDF data URLs to ``globus://`` URIs.
    Other URLs (``globus://``, ``stream://``, external ``https://``) pass through.

    Args:
        url: Raw data source URL string.

    Returns:
        Normalized URL string.
    """
    parsed = urlparse(url)

    # Globus File Manager URL → globus://collection_uuid/path
    if parsed.hostname == "app.globus.org" and "/file-manager" in parsed.path:
        qs = parse_qs(parsed.query)
        origin_id = qs.get("origin_id", [None])[0]
        origin_path = qs.get("origin_path", ["/"])[0]
        if origin_id:
            return f"globus://{origin_id}{unquote(origin_path)}"

    # MDF data domain → globus://NCSA_MDF_COLLECTION_UUID/path
    if parsed.hostname == "data.materialsdatafacility.org":
        return f"globus://{NCSA_MDF_COLLECTION_UUID}{parsed.path}"

    return url


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
        if source.startswith(("globus://", "https://", "stream://")):
            resolved.append(normalize_data_source(source))
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

    Constructs the complete flat v2 JSON payload for the MDF Connect API.

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
    if not manifest.title or not manifest.authors:
        raise ValueError("Manifest requires 'title' and 'authors'")

    # Build the flat metadata payload from the manifest
    metadata = manifest.to_metadata_payload()

    data_sources = resolve_data_sources(manifest.data_sources, root)

    # Auto-populate data_sources from git-tracked files if empty
    if not data_sources and root:
        from mdf_agent.core.repository import Repository
        try:
            repo = Repository.load(root)
            tracked = repo.get_tracked_files()
            if tracked:
                data_sources = [
                    str((root / f).resolve()) for f in tracked
                ]
        except Exception:
            pass

    submission = Submission(
        title=metadata.get("title"),
        authors=metadata.get("authors"),
        description=metadata.get("description"),
        keywords=metadata.get("keywords", []),
        publisher=metadata.get("publisher", "Materials Data Facility"),
        publication_year=metadata.get("publication_year"),
        resource_type=metadata.get("resource_type", "Dataset"),
        data_sources=data_sources,
        test=test,
        update=update,
        organization=metadata.get("organization"),
        tags=metadata.get("tags"),
        domains=metadata.get("domains"),
        acl=metadata.get("acl"),
        related_works=metadata.get("related_works"),
        external_doi=metadata.get("external_doi"),
        external_url=metadata.get("external_url"),
        external_source=metadata.get("external_source"),
        extensions=metadata.get("extensions"),
        ml=metadata.get("ml"),
        # Legacy fields that still need to be passed through
        mrr=manifest.mrr,
        data_destinations=manifest.data_destinations,
        external_uri=manifest.external_uri,
        index=manifest.index,
        extraction_config=manifest.extraction_config,
        services=manifest.services,
        links=manifest.links,
        no_extract=manifest.no_extract,
        dataset_acl=manifest.dataset_acl,
        update_metadata_only=bool(manifest.update_metadata_only),
    )

    payload = submission.to_payload()
    json.dumps(payload, allow_nan=False)
    return payload


