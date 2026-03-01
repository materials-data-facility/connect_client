"""Manifest validation for MDF Agent.

This module provides validation functions to check manifest configurations
before submission. Validation catches common errors early, before the
submission is sent to MDF Connect.

Validation returns two lists:
- errors: Issues that will prevent submission
- warnings: Issues that may cause problems but won't prevent submission
"""

from __future__ import annotations

from typing import List, Tuple

from mdf_agent.models.config import ManifestConfig


def validate_manifest(
    manifest: ManifestConfig,
    has_data_files: bool = False,
) -> Tuple[List[str], List[str]]:
    """Validate a manifest configuration.

    Checks for required fields and common issues. Returns errors (blocking)
    and warnings (non-blocking).

    Args:
        manifest: The ManifestConfig to validate.
        has_data_files: If True, data_sources will be auto-populated
            from directory scanning at build time, so skip that check.

    Returns:
        Tuple of (errors, warnings) where each is a list of message strings.

    Examples:
        >>> manifest = ManifestConfig(title="Test")
        >>> errors, warnings = validate_manifest(manifest)
        >>> if errors:
        ...     print("Cannot submit:", errors)
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not manifest.title:
        errors.append("Missing required field: title")
    if not manifest.authors:
        errors.append("Missing required field: authors")

    if not manifest.update_metadata_only and not manifest.data_sources and not has_data_files:
        errors.append("Missing data_sources (or set update_metadata_only)")

    if manifest.publication_year is not None:
        try:
            int(manifest.publication_year)
        except (TypeError, ValueError):
            warnings.append("publication_year should be an integer year")

    if manifest.dataset_doi and not str(manifest.dataset_doi).startswith("10."):
        warnings.append("dataset_doi does not look like a DOI")

    return errors, warnings
