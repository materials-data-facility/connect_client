from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from nameparser import HumanName
from pydantic import BaseModel, ConfigDict, Field

from mdf_agent.core.utils import deep_merge
from mdf_agent.models.config import Author, ManifestConfig


class Title(BaseModel):
    title: str


class Creator(BaseModel):
    creatorName: str
    familyName: Optional[str] = None
    givenName: Optional[str] = None
    affiliations: Optional[List[str]] = None
    nameIdentifiers: Optional[List[Dict[str, Any]]] = None

    model_config = ConfigDict(extra="allow")


class Description(BaseModel):
    description: str
    descriptionType: str = "Other"


class Identifier(BaseModel):
    identifier: str
    identifierType: str = "DOI"


class RelatedIdentifier(BaseModel):
    relatedIdentifier: str
    relatedIdentifierType: str = "DOI"
    relationType: str = "IsPartOf"


class Subject(BaseModel):
    subject: str


class ResourceType(BaseModel):
    resourceTypeGeneral: str = "Dataset"
    resourceType: str


class DataCite(BaseModel):
    titles: List[Title]
    creators: List[Creator]
    publisher: str
    publicationYear: str = Field(alias="publicationYear")
    resourceType: ResourceType
    descriptions: Optional[List[Description]] = None
    identifier: Optional[Identifier] = None
    relatedIdentifiers: Optional[List[RelatedIdentifier]] = None
    subjects: Optional[List[Subject]] = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @staticmethod
    def _build_creator(author: Author | str, fallback_affiliations: List[str]) -> Creator:
        if isinstance(author, Author):
            name = author.name
            affiliations = author.affiliations or []
            orcid = author.orcid
        else:
            name = author
            affiliations = fallback_affiliations
            orcid = None

        parsed = HumanName(name)
        given = "{} {}".format(parsed.first, parsed.middle).strip()
        family = "{} {}".format(parsed.last, parsed.suffix).strip()
        creator_name = "{}, {}".format(family, given).strip(" ,")
        if not creator_name:
            creator_name = name

        creator = Creator(
            creatorName=creator_name,
            familyName=family or None,
            givenName=given or None,
            affiliations=affiliations or None,
        )
        if orcid:
            creator.nameIdentifiers = [
                {
                    "nameIdentifier": orcid,
                    "nameIdentifierScheme": "ORCID",
                    "schemeUri": "https://orcid.org",
                }
            ]
        return creator

    @classmethod
    def from_manifest(cls, manifest: ManifestConfig) -> "DataCite":
        if not manifest.title or not manifest.authors:
            raise ValueError("Manifest requires 'title' and 'authors' to build DataCite")

        titles_raw = manifest.title if isinstance(manifest.title, list) else [manifest.title]
        titles = [Title(title=title) for title in titles_raw if title]

        authors_raw = manifest.authors
        if not isinstance(authors_raw, list):
            authors_raw = [authors_raw]

        fallback_affiliations: List[str] = []
        creators = [cls._build_creator(author, fallback_affiliations) for author in authors_raw]

        publisher = manifest.publisher or "Materials Data Facility"
        try:
            publication_year = str(int(manifest.publication_year))
        except (TypeError, ValueError):
            publication_year = str(datetime.now().year)

        resource_type = ResourceType(resourceType=manifest.resource_type or "Dataset")

        data = {
            "titles": titles,
            "creators": creators,
            "publisher": publisher,
            "publicationYear": publication_year,
            "resourceType": resource_type,
        }

        if manifest.description:
            data["descriptions"] = [Description(description=manifest.description)]

        if manifest.dataset_doi:
            data["identifier"] = Identifier(identifier=manifest.dataset_doi)

        if manifest.related_dois:
            data["relatedIdentifiers"] = [
                RelatedIdentifier(relatedIdentifier=doi) for doi in manifest.related_dois
            ]

        if manifest.subjects:
            data["subjects"] = [Subject(subject=sub) for sub in manifest.subjects]

        datacite = cls(**data)

        if manifest.dc:
            merged = deep_merge(datacite.model_dump(by_alias=True), manifest.dc)
            return cls.model_validate(merged)

        return datacite

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)
