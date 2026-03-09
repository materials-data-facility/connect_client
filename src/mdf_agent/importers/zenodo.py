"""Zenodo repository adapter."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from mdf_agent.importers.base import ExternalRepoAdapter

# Matches zenodo:12345 or https://zenodo.org/records/12345 or https://zenodo.org/record/12345
_ZENODO_RE = re.compile(
    r"^(?:zenodo:(\d+)|https?://zenodo\.org/records?/(\d+)(?:/.*)?)",
    re.IGNORECASE,
)


def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text).strip()


class ZenodoAdapter(ExternalRepoAdapter):
    """Adapter for fetching metadata from Zenodo (public records)."""

    def name(self) -> str:
        return "Zenodo"

    def match(self, identifier: str) -> bool:
        return _ZENODO_RE.match(identifier.strip()) is not None

    def parse_identifier(self, identifier: str) -> str:
        m = _ZENODO_RE.match(identifier.strip())
        if not m:
            raise ValueError(f"Cannot parse Zenodo identifier: {identifier}")
        return m.group(1) or m.group(2)

    def fetch_metadata(self, identifier: str) -> Dict[str, Any]:
        import httpx

        record_id = self.parse_identifier(identifier)
        url = f"https://zenodo.org/api/records/{record_id}"

        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        metadata = data.get("metadata", {})

        # Authors
        authors = []
        for creator in metadata.get("creators", []):
            author: Dict[str, Any] = {"name": creator.get("name", "")}
            if creator.get("orcid"):
                author["orcid"] = creator["orcid"]
            if creator.get("affiliation"):
                author["affiliations"] = [creator["affiliation"]]
            authors.append(author)

        # Description (strip HTML)
        description = _strip_html(metadata.get("description", ""))

        # Keywords
        keywords = metadata.get("keywords", [])

        # License
        license_info = None
        lic = metadata.get("license")
        if isinstance(lic, dict):
            license_info = {
                "name": lic.get("id", ""),
                "url": lic.get("url"),
            }
        elif isinstance(lic, str):
            license_info = {"name": lic}

        # DOI
        doi = data.get("doi") or metadata.get("doi")

        # Landing page URL
        landing_url = f"https://zenodo.org/records/{record_id}"

        # Files
        file_urls = []
        for f in data.get("files", []):
            entry: Dict[str, Any] = {
                "url": f.get("links", {}).get("self") or f.get("links", {}).get("download", ""),
                "filename": f.get("key", f.get("filename", "")),
            }
            if f.get("size"):
                entry["size"] = f["size"]
            if entry["url"]:
                file_urls.append(entry)

        return {
            "title": metadata.get("title", "Untitled"),
            "authors": authors or [{"name": "Unknown"}],
            "description": description,
            "keywords": keywords,
            "license": license_info,
            "doi": doi,
            "url": landing_url,
            "file_urls": file_urls,
            "external_source": "Zenodo",
            "external_identifier": record_id,
        }
