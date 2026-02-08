from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Submission(BaseModel):
    # v2 flat metadata fields
    title: Optional[str] = None
    authors: Optional[List[Dict[str, Any]]] = None
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    publisher: str = "Materials Data Facility"
    publication_year: Optional[int] = None
    resource_type: str = "Dataset"

    data_sources: List[str] = Field(default_factory=list)
    test: bool = False
    update: bool = False

    organization: Optional[str] = None
    tags: Optional[List[str]] = None
    acl: Optional[List[str]] = None

    license: Optional[Dict[str, Any]] = None
    funding: Optional[List[Dict[str, Any]]] = None
    related_works: Optional[List[Dict[str, Any]]] = None

    methods: Optional[List[str]] = None
    facility: Optional[str] = None
    fields_of_science: Optional[List[str]] = None
    domains: Optional[List[str]] = None

    external_doi: Optional[str] = None
    external_url: Optional[str] = None
    external_source: Optional[str] = None

    ml: Optional[Dict[str, Any]] = None

    extensions: Optional[Dict[str, Any]] = None

    # Legacy fields (still supported for backward compat with old clients)
    dc: Optional[Dict[str, Any]] = None
    mdf: Optional[Dict[str, Any]] = None
    mrr: Optional[Dict[str, Any]] = None
    custom: Optional[Dict[str, Any]] = None
    projects: Optional[Dict[str, Any]] = None
    data_destinations: Optional[List[str]] = None
    external_uri: Optional[str] = None
    index: Optional[Dict[str, Any]] = None
    extraction_config: Optional[Dict[str, Any]] = None
    services: Optional[Dict[str, Any]] = None
    links: Optional[List[Dict[str, Any]]] = None
    curation: Optional[bool] = None
    no_extract: Optional[bool] = None
    dataset_acl: Optional[List[str]] = None
    update_metadata_only: bool = False

    model_config = ConfigDict(extra="allow")

    def to_payload(self) -> Dict[str, Any]:
        """Build the flat v2 payload for submission."""
        payload: Dict[str, Any] = {}

        # Required fields
        if self.title:
            payload["title"] = self.title
        if self.authors:
            payload["authors"] = self.authors

        # Recommended
        if self.description:
            payload["description"] = self.description
        if self.keywords:
            payload["keywords"] = self.keywords
        payload["publisher"] = self.publisher
        if self.publication_year:
            payload["publication_year"] = self.publication_year
        payload["resource_type"] = self.resource_type

        # Data
        payload["data_sources"] = self.data_sources
        payload["test"] = self.test
        payload["update"] = self.update

        # Platform
        if self.organization:
            payload["organization"] = self.organization
        if self.tags:
            payload["tags"] = self.tags
        if self.acl:
            payload["acl"] = self.acl

        # Attribution
        if self.license:
            payload["license"] = self.license
        if self.funding:
            payload["funding"] = self.funding
        if self.related_works:
            payload["related_works"] = self.related_works

        # Scientific context
        if self.methods:
            payload["methods"] = self.methods
        if self.facility:
            payload["facility"] = self.facility
        if self.fields_of_science:
            payload["fields_of_science"] = self.fields_of_science
        if self.domains:
            payload["domains"] = self.domains

        # External import provenance
        if self.external_doi:
            payload["external_doi"] = self.external_doi
        if self.external_url:
            payload["external_url"] = self.external_url
        if self.external_source:
            payload["external_source"] = self.external_source

        # ML metadata
        if self.ml:
            payload["ml"] = self.ml

        # Extensions
        if self.extensions:
            payload["extensions"] = self.extensions

        # If legacy dc/mdf fields are set (old-style client), pass them through
        # so the server can auto-migrate
        if self.dc:
            payload["dc"] = self.dc
        if self.mdf:
            payload["mdf"] = self.mdf

        # Legacy optional fields
        optional_legacy = {
            "mrr": self.mrr,
            "custom": self.custom,
            "projects": self.projects,
            "data_destinations": self.data_destinations,
            "external_uri": self.external_uri,
            "index": self.index,
            "extraction_config": self.extraction_config,
            "services": self.services,
            "links": self.links,
            "curation": self.curation,
            "no_extract": self.no_extract,
            "dataset_acl": self.dataset_acl,
        }
        for key, value in optional_legacy.items():
            if value not in (None, {}, [], ""):
                payload[key] = value

        payload["update_metadata_only"] = self.update_metadata_only

        return payload
