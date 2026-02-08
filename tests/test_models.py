"""Tests for MDF Agent Pydantic models."""

import pytest
from datetime import datetime

from mdf_agent.models.config import Author, DataSource, DerivedFrom, ManifestConfig
from mdf_agent.models.datacite import DataCite, Creator, Title, Description
from mdf_agent.models.submission import Submission


class TestAuthor:
    """Tests for the Author model."""

    def test_author_minimal(self):
        """Author with just name."""
        author = Author(name="Jane Doe")
        assert author.name == "Jane Doe"
        assert author.affiliations == []
        assert author.orcid is None

    def test_author_full(self):
        """Author with all fields."""
        author = Author(
            name="Jane Doe",
            affiliations=["MIT", "Argonne"],
            orcid="0000-0001-2345-6789",
        )
        assert author.name == "Jane Doe"
        assert author.affiliations == ["MIT", "Argonne"]
        assert author.orcid == "0000-0001-2345-6789"

    def test_author_extra_fields_allowed(self):
        """Author allows extra fields."""
        author = Author(name="Jane Doe", email="jane@example.com")
        assert author.name == "Jane Doe"
        assert author.email == "jane@example.com"


class TestDataSource:
    """Tests for the DataSource model."""

    def test_data_source_minimal(self):
        """DataSource with just path."""
        ds = DataSource(path="./data")
        assert ds.path == "./data"
        assert ds.include == []
        assert ds.exclude == []

    def test_data_source_with_patterns(self):
        """DataSource with include/exclude patterns."""
        ds = DataSource(
            path="./data",
            include=["**/*.csv", "**/*.json"],
            exclude=["**/.git/**"],
        )
        assert ds.include == ["**/*.csv", "**/*.json"]
        assert ds.exclude == ["**/.git/**"]


class TestDerivedFrom:
    """Tests for the DerivedFrom model."""

    def test_derived_from_minimal(self):
        """DerivedFrom with just source_id."""
        derived = DerivedFrom(source_id="mdf-dataset-v1")
        assert derived.source_id == "mdf-dataset-v1"
        assert derived.relationship is None
        assert derived.description is None

    def test_derived_from_full(self):
        """DerivedFrom with all fields."""
        derived = DerivedFrom(
            source_id="mdf-dataset-v1",
            relationship="filtered_subset",
            description="Filtered to Fe-containing alloys only",
        )
        assert derived.relationship == "filtered_subset"
        assert derived.description == "Filtered to Fe-containing alloys only"


class TestManifestConfig:
    """Tests for the ManifestConfig model."""

    def test_manifest_empty(self):
        """Empty manifest is valid."""
        manifest = ManifestConfig()
        assert manifest.title is None
        assert manifest.authors is None
        assert manifest.data_sources == []

    def test_manifest_minimal(self):
        """Minimal manifest with required fields for submission."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
        )
        assert manifest.title == "My Dataset"
        assert manifest.authors == ["Jane Doe"]

    def test_manifest_with_author_objects(self):
        """Manifest with Author objects."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=[
                Author(name="Jane Doe", affiliations=["MIT"]),
                Author(name="John Smith", orcid="0000-0001-2345-6789"),
            ],
        )
        assert len(manifest.authors) == 2
        assert manifest.authors[0].name == "Jane Doe"

    def test_manifest_list_title(self):
        """Manifest with list of titles."""
        manifest = ManifestConfig(
            title=["Main Title", "Alternative Title"],
            authors=["Jane Doe"],
        )
        assert manifest.title == ["Main Title", "Alternative Title"]

    def test_manifest_data_sources_mixed(self):
        """Manifest with mixed string and DataSource objects."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            data_sources=[
                "globus://endpoint/path",
                DataSource(path="./local", include=["*.csv"]),
            ],
        )
        assert len(manifest.data_sources) == 2
        assert manifest.data_sources[0] == "globus://endpoint/path"
        assert manifest.data_sources[1].path == "./local"

    def test_manifest_all_fields(self):
        """Manifest with all common fields."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            description="A test dataset",
            publisher="Materials Data Facility",
            publication_year=2025,
            resource_type="Dataset",
            dataset_doi="10.1234/test",
            related_dois=["10.1234/paper1"],
            subjects=["materials science"],
            organization="CHiMaD",
            acl=["public"],
            tags=["DFT", "alloys"],
            auto_discover=True,
        )
        assert manifest.publisher == "Materials Data Facility"
        assert manifest.publication_year == 2025
        assert manifest.organization == "CHiMaD"
        assert manifest.auto_discover is True

    def test_manifest_with_domains(self):
        """Manifest with domains field."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            domains=["materials", "chemistry"],
        )
        assert manifest.domains == ["materials", "chemistry"]

    def test_manifest_domains_none_by_default(self):
        """Domains is None by default."""
        manifest = ManifestConfig()
        assert manifest.domains is None

    def test_manifest_domains_single(self):
        """Single domain in list."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            domains=["biology"],
        )
        assert manifest.domains == ["biology"]

    def test_manifest_with_external_import(self):
        """Manifest with external import fields."""
        manifest = ManifestConfig(
            title="Imported Dataset",
            authors=["Jane Doe"],
            external_doi="10.5281/zenodo.1234567",
            external_url="https://zenodo.org/record/1234567",
            external_source="Zenodo",
        )
        assert manifest.external_doi == "10.5281/zenodo.1234567"
        assert manifest.external_url == "https://zenodo.org/record/1234567"
        assert manifest.external_source == "Zenodo"

    def test_manifest_external_fields_none_by_default(self):
        """External import fields are None by default."""
        manifest = ManifestConfig()
        assert manifest.external_doi is None
        assert manifest.external_url is None
        assert manifest.external_source is None


class TestDataCiteCreatorParsing:
    """Tests for DataCite author name parsing."""

    def test_simple_name(self):
        """Parse simple first last name."""
        author = Author(name="Jane Doe")
        creator = DataCite._build_creator(author, [])
        assert creator.creatorName == "Doe, Jane"
        assert creator.familyName == "Doe"
        assert creator.givenName == "Jane"

    def test_name_with_middle(self):
        """Parse name with middle name."""
        author = Author(name="Jane Marie Doe")
        creator = DataCite._build_creator(author, [])
        assert creator.creatorName == "Doe, Jane Marie"
        assert creator.givenName == "Jane Marie"
        assert creator.familyName == "Doe"

    def test_last_first_format(self):
        """Parse Last, First format."""
        author = Author(name="Doe, Jane")
        creator = DataCite._build_creator(author, [])
        assert creator.creatorName == "Doe, Jane"
        assert creator.familyName == "Doe"
        assert creator.givenName == "Jane"

    def test_name_with_suffix(self):
        """Parse name with suffix (Jr, III, etc)."""
        author = Author(name="John Smith Jr")
        creator = DataCite._build_creator(author, [])
        assert "Smith" in creator.creatorName
        assert "Jr" in creator.familyName or "Jr" in creator.creatorName

    def test_string_author(self):
        """Parse string author (not Author object)."""
        creator = DataCite._build_creator("Jane Doe", ["MIT"])
        assert creator.creatorName == "Doe, Jane"
        assert creator.affiliations == ["MIT"]

    def test_author_with_orcid(self):
        """Author with ORCID gets name identifier."""
        author = Author(name="Jane Doe", orcid="0000-0001-2345-6789")
        creator = DataCite._build_creator(author, [])
        assert creator.nameIdentifiers is not None
        assert len(creator.nameIdentifiers) == 1
        assert creator.nameIdentifiers[0]["nameIdentifier"] == "0000-0001-2345-6789"
        assert creator.nameIdentifiers[0]["nameIdentifierScheme"] == "ORCID"

    def test_author_with_affiliations(self):
        """Author with affiliations."""
        author = Author(name="Jane Doe", affiliations=["MIT", "Argonne"])
        creator = DataCite._build_creator(author, [])
        assert creator.affiliations == ["MIT", "Argonne"]

    def test_single_name(self):
        """Handle single name (no space)."""
        author = Author(name="Madonna")
        creator = DataCite._build_creator(author, [])
        # Should handle gracefully, not crash
        assert creator.creatorName is not None


class TestDataCiteFromManifest:
    """Tests for DataCite.from_manifest()."""

    def test_minimal_manifest(self):
        """Build DataCite from minimal manifest."""
        manifest = ManifestConfig(title="My Dataset", authors=["Jane Doe"])
        dc = DataCite.from_manifest(manifest)
        assert len(dc.titles) == 1
        assert dc.titles[0].title == "My Dataset"
        assert len(dc.creators) == 1
        assert dc.creators[0].creatorName == "Doe, Jane"
        assert dc.publisher == "Materials Data Facility"
        assert dc.publicationYear == str(datetime.now().year)
        assert dc.resourceType.resourceTypeGeneral == "Dataset"

    def test_missing_title_raises(self):
        """Missing title raises ValueError."""
        manifest = ManifestConfig(authors=["Jane Doe"])
        with pytest.raises(ValueError, match="title"):
            DataCite.from_manifest(manifest)

    def test_missing_authors_raises(self):
        """Missing authors raises ValueError."""
        manifest = ManifestConfig(title="My Dataset")
        with pytest.raises(ValueError, match="authors"):
            DataCite.from_manifest(manifest)

    def test_multiple_titles(self):
        """Multiple titles are all included."""
        manifest = ManifestConfig(
            title=["Main Title", "Alternative Title"],
            authors=["Jane Doe"],
        )
        dc = DataCite.from_manifest(manifest)
        assert len(dc.titles) == 2
        assert dc.titles[0].title == "Main Title"
        assert dc.titles[1].title == "Alternative Title"

    def test_multiple_authors(self):
        """Multiple authors are all included."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe", "John Smith"],
        )
        dc = DataCite.from_manifest(manifest)
        assert len(dc.creators) == 2

    def test_with_description(self):
        """Description is included."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            description="A test dataset for testing.",
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.descriptions is not None
        assert len(dc.descriptions) == 1
        assert dc.descriptions[0].description == "A test dataset for testing."
        assert dc.descriptions[0].descriptionType == "Other"

    def test_with_doi(self):
        """DOI is included as identifier."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            dataset_doi="10.1234/test",
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.identifier is not None
        assert dc.identifier.identifier == "10.1234/test"
        assert dc.identifier.identifierType == "DOI"

    def test_with_related_dois(self):
        """Related DOIs are included."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            related_dois=["10.1234/paper1", "10.1234/paper2"],
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.relatedIdentifiers is not None
        assert len(dc.relatedIdentifiers) == 2
        assert dc.relatedIdentifiers[0].relatedIdentifier == "10.1234/paper1"

    def test_with_subjects(self):
        """Subjects are included."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            subjects=["materials science", "DFT"],
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.subjects is not None
        assert len(dc.subjects) == 2
        assert dc.subjects[0].subject == "materials science"

    def test_custom_publisher(self):
        """Custom publisher is used."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            publisher="My Organization",
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.publisher == "My Organization"

    def test_custom_publication_year(self):
        """Custom publication year is used."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            publication_year=2020,
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.publicationYear == "2020"

    def test_publication_year_string(self):
        """Publication year as string works."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            publication_year="2020",
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.publicationYear == "2020"

    def test_invalid_publication_year_uses_current(self):
        """Invalid publication year falls back to current year."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            publication_year="invalid",
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.publicationYear == str(datetime.now().year)

    def test_dc_override(self):
        """Manual dc block overrides generated values."""
        manifest = ManifestConfig(
            title="My Dataset",
            authors=["Jane Doe"],
            dc={"publisher": "Override Publisher"},
        )
        dc = DataCite.from_manifest(manifest)
        assert dc.publisher == "Override Publisher"

    def test_to_dict(self):
        """DataCite.to_dict() returns proper dict."""
        manifest = ManifestConfig(title="My Dataset", authors=["Jane Doe"])
        dc = DataCite.from_manifest(manifest)
        d = dc.to_dict()
        assert "titles" in d
        assert "creators" in d
        assert "publisher" in d
        assert "publicationYear" in d
        assert "resourceType" in d


class TestSubmission:
    """Tests for the Submission model."""

    def test_submission_minimal(self):
        """Minimal submission."""
        sub = Submission(dc={"titles": [{"title": "Test"}]}, data_sources=["https://example.com/data"])
        assert sub.test is False
        assert sub.update is False
        assert sub.update_metadata_only is False

    def test_submission_flags(self):
        """Submission with flags."""
        sub = Submission(
            dc={},
            data_sources=[],
            test=True,
            update=True,
            update_metadata_only=True,
        )
        assert sub.test is True
        assert sub.update is True
        assert sub.update_metadata_only is True

    def test_to_payload_minimal(self):
        """to_payload() with minimal data."""
        sub = Submission(dc={"titles": [{"title": "Test"}]}, data_sources=["https://example.com/data"])
        payload = sub.to_payload()
        assert payload["dc"] == {"titles": [{"title": "Test"}]}
        assert payload["data_sources"] == ["https://example.com/data"]
        assert payload["test"] is False
        assert payload["update"] is False
        assert payload["mdf"] == {}
        assert payload["update_metadata_only"] is False

    def test_to_payload_excludes_none(self):
        """to_payload() excludes None optional fields."""
        sub = Submission(dc={}, data_sources=[])
        payload = sub.to_payload()
        assert "mrr" not in payload
        assert "custom" not in payload
        assert "services" not in payload
        assert "tags" not in payload

    def test_to_payload_includes_populated(self):
        """to_payload() includes populated optional fields."""
        sub = Submission(
            dc={},
            data_sources=[],
            tags=["tag1", "tag2"],
            services={"mdf_publish": True},
            custom={"field1": "value1"},
        )
        payload = sub.to_payload()
        assert payload["tags"] == ["tag1", "tag2"]
        assert payload["services"] == {"mdf_publish": True}
        assert payload["custom"] == {"field1": "value1"}

    def test_to_payload_mdf_block(self):
        """to_payload() includes mdf block."""
        sub = Submission(
            dc={},
            data_sources=[],
            mdf={"organization": "CHiMaD", "acl": ["public"]},
        )
        payload = sub.to_payload()
        assert payload["mdf"] == {"organization": "CHiMaD", "acl": ["public"]}


class TestSubmissionPayloadShape:
    """Tests to verify submission payload matches MDF Connect API expectations."""

    def test_payload_has_required_keys(self):
        """Payload has all required top-level keys."""
        sub = Submission(dc={"titles": []}, data_sources=["https://example.com"])
        payload = sub.to_payload()
        assert "dc" in payload
        assert "data_sources" in payload
        assert "test" in payload
        assert "update" in payload
        assert "mdf" in payload
        assert "update_metadata_only" in payload

    def test_payload_data_types(self):
        """Payload values have correct types."""
        sub = Submission(
            dc={"titles": [{"title": "Test"}]},
            data_sources=["https://example.com"],
            test=True,
            tags=["tag1"],
        )
        payload = sub.to_payload()
        assert isinstance(payload["dc"], dict)
        assert isinstance(payload["data_sources"], list)
        assert isinstance(payload["test"], bool)
        assert isinstance(payload["update"], bool)
        assert isinstance(payload["mdf"], dict)
        assert isinstance(payload["tags"], list)

    def test_payload_includes_domains(self):
        """to_payload() includes domains when set."""
        sub = Submission(domains=["materials", "chemistry"])
        payload = sub.to_payload()
        assert payload["domains"] == ["materials", "chemistry"]

    def test_payload_excludes_domains_when_none(self):
        """to_payload() excludes domains when not set."""
        sub = Submission()
        payload = sub.to_payload()
        assert "domains" not in payload

    def test_payload_includes_external_import(self):
        """to_payload() includes external import fields when set."""
        sub = Submission(
            external_doi="10.5281/zenodo.1234567",
            external_url="https://zenodo.org/record/1234567",
            external_source="Zenodo",
        )
        payload = sub.to_payload()
        assert payload["external_doi"] == "10.5281/zenodo.1234567"
        assert payload["external_url"] == "https://zenodo.org/record/1234567"
        assert payload["external_source"] == "Zenodo"

    def test_payload_excludes_external_import_when_none(self):
        """to_payload() excludes external import fields when not set."""
        sub = Submission()
        payload = sub.to_payload()
        assert "external_doi" not in payload
        assert "external_url" not in payload
        assert "external_source" not in payload


class TestToMetadataPayload:
    """Tests for ManifestConfig.to_metadata_payload()."""

    def test_domains_in_payload(self):
        """Domains flow through to metadata payload."""
        manifest = ManifestConfig(
            title="Test",
            authors=["Jane Doe"],
            domains=["materials", "chemistry"],
        )
        payload = manifest.to_metadata_payload()
        assert payload["domains"] == ["materials", "chemistry"]

    def test_domains_omitted_when_none(self):
        """Domains not in payload when not set."""
        manifest = ManifestConfig(title="Test", authors=["Jane Doe"])
        payload = manifest.to_metadata_payload()
        assert "domains" not in payload

    def test_external_import_in_payload(self):
        """External import fields flow through to metadata payload."""
        manifest = ManifestConfig(
            title="Test",
            authors=["Jane Doe"],
            external_doi="10.5281/zenodo.1234567",
            external_url="https://zenodo.org/record/1234567",
            external_source="Zenodo",
        )
        payload = manifest.to_metadata_payload()
        assert payload["external_doi"] == "10.5281/zenodo.1234567"
        assert payload["external_url"] == "https://zenodo.org/record/1234567"
        assert payload["external_source"] == "Zenodo"

    def test_external_import_omitted_when_none(self):
        """External import fields not in payload when not set."""
        manifest = ManifestConfig(title="Test", authors=["Jane Doe"])
        payload = manifest.to_metadata_payload()
        assert "external_doi" not in payload
        assert "external_url" not in payload
        assert "external_source" not in payload

    def test_partial_external_import(self):
        """Only set external import fields appear in payload."""
        manifest = ManifestConfig(
            title="Test",
            authors=["Jane Doe"],
            external_doi="10.5281/zenodo.1234567",
        )
        payload = manifest.to_metadata_payload()
        assert payload["external_doi"] == "10.5281/zenodo.1234567"
        assert "external_url" not in payload
        assert "external_source" not in payload
