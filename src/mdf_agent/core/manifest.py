"""Manifest file handling for mdf.yaml.

This module provides functions for reading, writing, and initializing
mdf.yaml manifest files. The manifest is a YAML file that declares
dataset metadata in a human-readable format.

Example mdf.yaml::

    title: "My Dataset"
    authors:
      - name: "Jane Doe"
        affiliations: ["MIT"]
      - "John Smith"
    description: "A test dataset"
    data_sources:
      - "./data"
      - "globus://endpoint/path"
    acl: ["public"]
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Union

import yaml

from mdf_agent.models.config import Author, DataSource, ManifestConfig


def _normalize_authors(authors: List[Union[str, Dict[str, Any]]]) -> List[Union[str, Author]]:
    normalized: List[Union[str, Author]] = []
    for author in authors:
        if isinstance(author, Author) or isinstance(author, str):
            normalized.append(author)
        elif isinstance(author, dict):
            normalized.append(Author(**author))
        else:
            normalized.append(str(author))
    return normalized


def _normalize_data_sources(
    data_sources: List[Union[str, Dict[str, Any]]]
) -> List[Union[str, DataSource]]:
    normalized: List[Union[str, DataSource]] = []
    for source in data_sources:
        if isinstance(source, DataSource) or isinstance(source, str):
            normalized.append(source)
        elif isinstance(source, dict):
            normalized.append(DataSource(**source))
        else:
            normalized.append(str(source))
    return normalized


def load_manifest(path: Path) -> ManifestConfig:
    """Load a manifest from a YAML file.

    Parses the YAML file and normalizes authors and data_sources
    into their proper Pydantic model types.

    Args:
        path: Path to the mdf.yaml file.

    Returns:
        ManifestConfig populated from the YAML file.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        yaml.YAMLError: If the file is not valid YAML.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "authors" in data and data["authors"] is not None:
        authors = data["authors"]
        if not isinstance(authors, list):
            authors = [authors]
        data["authors"] = _normalize_authors(authors)
    if "data_sources" in data and data["data_sources"] is not None:
        data_sources = data["data_sources"]
        if not isinstance(data_sources, list):
            data_sources = [data_sources]
        data["data_sources"] = _normalize_data_sources(data_sources)
    return ManifestConfig(**data)


def save_manifest(config: ManifestConfig, path: Path) -> None:
    """Save a manifest to a YAML file.

    Serializes the ManifestConfig to YAML format, excluding None values
    for cleaner output.

    Args:
        config: The ManifestConfig to save.
        path: Path where the YAML file will be written.
    """
    data = config.model_dump(exclude_none=True)
    path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def init_manifest(
    path: Path,
    title: str,
    authors: List[str],
    description: str | None = None,
    publisher: str | None = None,
    publication_year: int | str | None = None,
) -> ManifestConfig:
    """Create and save a new manifest file.

    Creates a ManifestConfig with the provided metadata and saves it
    to the specified path.

    Args:
        path: Path where the mdf.yaml file will be created.
        title: Dataset title.
        authors: List of author names.
        description: Optional dataset description.
        publisher: Optional publisher name (defaults to MDF).
        publication_year: Optional publication year.

    Returns:
        The created ManifestConfig.
    """
    config = ManifestConfig(
        title=title,
        authors=authors,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )
    save_manifest(config, path)
    return config
