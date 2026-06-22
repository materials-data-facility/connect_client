from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class Author(BaseModel):
    name: str
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    affiliations: List[str] = Field(default_factory=list)
    orcid: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class DataSource(BaseModel):
    path: str
    include: List[str] = Field(default_factory=list)
    exclude: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


class DerivedFrom(BaseModel):
    source_id: str
    relationship: Optional[str] = None
    description: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class ManifestConfig(BaseModel):
    title: Optional[Union[str, List[str]]] = None
    authors: Optional[List[Union[str, Author]]] = None
    description: Optional[str] = None
    publisher: Optional[str] = None
    publication_year: Optional[Union[int, str]] = None
    resource_type: Optional[str] = None

    dataset_doi: Optional[str] = None
    related_dois: Optional[List[str]] = None
    subjects: Optional[List[str]] = None

    data_sources: List[Union[str, DataSource]] = Field(default_factory=list)

    organization: Optional[str] = None
    acl: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    domains: Optional[List[str]] = None

    external_doi: Optional[str] = None
    external_url: Optional[str] = None
    external_source: Optional[str] = None

    services: Optional[Dict[str, Any]] = None
    projects: Optional[Dict[str, Any]] = None
    links: Optional[List[Dict[str, Any]]] = None
    index: Optional[Dict[str, Any]] = None

    mdf: Optional[Dict[str, Any]] = None
    mrr: Optional[Dict[str, Any]] = None
    custom: Optional[Dict[str, Any]] = None

    dataset_acl: Optional[List[str]] = None
    data_destinations: Optional[List[str]] = None
    external_uri: Optional[str] = None
    no_extract: Optional[bool] = None
    extraction_config: Optional[Dict[str, Any]] = None
    update_metadata_only: Optional[bool] = None

    auto_discover: Optional[bool] = None
    derived_from: Optional[List[DerivedFrom]] = None

    dc: Optional[Dict[str, Any]] = None
    auto_metadata: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="allow")

    def to_metadata_payload(self) -> Dict[str, Any]:
        """Convert ManifestConfig to the flat v2 metadata API format.

        Maps researcher-friendly manifest fields to the DatasetMetadata schema.
        """
        payload: Dict[str, Any] = {}

        # Title
        if self.title:
            payload["title"] = self.title[0] if isinstance(self.title, list) else self.title

        # Authors
        if self.authors:
            authors = []
            for a in self.authors:
                if isinstance(a, Author):
                    entry: Dict[str, Any] = {"name": a.name}
                    if a.given_name:
                        entry["given_name"] = a.given_name
                    if a.family_name:
                        entry["family_name"] = a.family_name
                    if a.affiliations:
                        entry["affiliations"] = a.affiliations
                    if a.orcid:
                        entry["orcid"] = a.orcid
                    authors.append(entry)
                else:
                    authors.append({"name": a})
            payload["authors"] = authors

        if self.description:
            payload["description"] = self.description
        if self.publisher:
            payload["publisher"] = self.publisher
        if self.publication_year:
            try:
                payload["publication_year"] = int(self.publication_year)
            except (ValueError, TypeError):
                pass
        if self.resource_type:
            payload["resource_type"] = self.resource_type

        # Keywords (from subjects)
        if self.subjects:
            payload["keywords"] = self.subjects

        # Organization
        if self.organization:
            payload["organization"] = self.organization
        if self.acl:
            payload["acl"] = self.acl
        if self.tags:
            payload["tags"] = self.tags
        if self.domains:
            payload["domains"] = self.domains

        # External import provenance. The v2 backend (cs/aws/v2/metadata.py) reads
        # these as flat top-level keys, so emit them flat (only the ones that are set).
        if self.external_doi:
            payload["external_doi"] = self.external_doi
        if self.external_url:
            payload["external_url"] = self.external_url
        if self.external_source:
            payload["external_source"] = self.external_source

        # Related works (from related_dois)
        if self.related_dois:
            payload["related_works"] = [
                {"identifier": doi, "identifier_type": "DOI", "relation_type": "References"}
                for doi in self.related_dois
            ]

        # Extensions (from custom, projects minus foundry)
        extensions: Dict[str, Any] = {}
        if self.custom:
            extensions.update(self.custom)
        if self.projects:
            for k, v in self.projects.items():
                if k != "foundry":
                    extensions[k] = v
        if extensions:
            payload["extensions"] = extensions

        return payload
