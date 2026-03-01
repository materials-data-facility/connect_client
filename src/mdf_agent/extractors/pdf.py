from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

from mdf_agent.extractors.base import BaseExtractor

DOI_PATTERN = re.compile(r"10\.\d{4,9}/[^\s\"<>]+")


class PDFExtractor(BaseExtractor):
    extensions = [".pdf"]

    @classmethod
    def extract(cls, path: Path) -> Dict:
        try:
            from pypdf import PdfReader
        except Exception:
            return {}

        try:
            reader = PdfReader(str(path))
        except Exception:
            return {}

        metadata = reader.metadata or {}
        title = getattr(metadata, "title", None) or metadata.get("/Title")
        author = getattr(metadata, "author", None) or metadata.get("/Author")

        doi = None
        try:
            first_page = reader.pages[0].extract_text() or ""
            match = DOI_PATTERN.search(first_page)
            if match:
                doi = match.group(0)
        except Exception:
            doi = None

        dc: Dict = {}
        if title:
            dc["titles"] = [{"title": title}]
        if author:
            creators = []
            for name in re.split(r";|,\s*and\s*", author):
                clean = name.strip()
                if clean:
                    creators.append({"creatorName": clean})
            if creators:
                dc["creators"] = creators
        if doi:
            dc["relatedIdentifiers"] = [
                {
                    "relatedIdentifier": doi,
                    "relatedIdentifierType": "DOI",
                    "relationType": "IsPartOf",
                }
            ]

        if not dc:
            return {}
        return {"dc": dc}
