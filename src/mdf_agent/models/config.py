from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class Author(BaseModel):
    name: str
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
