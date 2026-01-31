from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Submission(BaseModel):
    dc: Dict[str, Any] = Field(default_factory=dict)
    data_sources: List[str] = Field(default_factory=list)
    test: bool = False
    update: bool = False

    mdf: Dict[str, Any] = Field(default_factory=dict)
    mrr: Optional[Dict[str, Any]] = None
    custom: Optional[Dict[str, Any]] = None
    projects: Optional[Dict[str, Any]] = None
    data_destinations: Optional[List[str]] = None
    external_uri: Optional[str] = None
    index: Optional[Dict[str, Any]] = None
    extraction_config: Optional[Dict[str, Any]] = None
    services: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    links: Optional[List[Dict[str, Any]]] = None
    curation: Optional[bool] = None
    no_extract: Optional[bool] = None
    dataset_acl: Optional[List[str]] = None
    update_metadata_only: bool = False

    model_config = ConfigDict(extra="allow")

    def to_payload(self) -> Dict[str, Any]:
        submission = {
            "dc": self.dc,
            "data_sources": self.data_sources,
            "test": self.test,
            "update": self.update,
            "mdf": self.mdf if self.mdf else {},
        }
        optional_fields = {
            "mrr": self.mrr,
            "custom": self.custom,
            "projects": self.projects,
            "data_destinations": self.data_destinations,
            "external_uri": self.external_uri,
            "index": self.index,
            "extraction_config": self.extraction_config,
            "services": self.services,
            "tags": self.tags,
            "links": self.links,
            "curation": self.curation,
            "no_extract": self.no_extract,
            "dataset_acl": self.dataset_acl,
        }
        for key, value in optional_fields.items():
            if value not in (None, {}, [], ""):
                submission[key] = value
        submission["update_metadata_only"] = self.update_metadata_only
        return submission
