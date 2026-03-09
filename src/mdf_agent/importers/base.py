"""Base class for external repository adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExternalRepoAdapter(ABC):
    """Base class for adapters that fetch metadata from external repositories."""

    @abstractmethod
    def name(self) -> str:
        """Short name of the repository (e.g. 'Zenodo')."""
        ...

    @abstractmethod
    def match(self, identifier: str) -> bool:
        """Return True if this adapter can handle the given identifier."""
        ...

    @abstractmethod
    def parse_identifier(self, identifier: str) -> str:
        """Extract the canonical record ID from an identifier string."""
        ...

    @abstractmethod
    def fetch_metadata(self, identifier: str) -> Dict[str, Any]:
        """Fetch metadata from the external repository.

        Returns a dict with keys:
            title: str
            authors: list of {"name": str, "orcid": optional str, "affiliations": optional list}
            description: str
            keywords: list of str
            license: optional {"name": str, "url": optional str, "identifier": optional str}
            doi: optional str
            url: str (landing page)
            file_urls: list of {"url": str, "filename": str, "size": optional int}
            external_source: str (repository name)
            external_identifier: str (repo-specific ID)
        """
        ...
