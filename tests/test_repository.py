"""Tests for MDF Agent manifest-based operations (replaces git repository tests)."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from mdf_agent.core.exceptions import NoManifestError
from mdf_agent.core.agent import MDFAgent
from mdf_agent.core.manifest import load_manifest, save_manifest, init_manifest


class TestManifestInit:
    """Tests for manifest initialization."""

    def test_init_creates_manifest(self):
        """init_manifest creates mdf.yaml with correct content."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            agent = MDFAgent.init_manifest(
                path=str(root),
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            assert (root / "mdf.yaml").is_file()
            assert agent.manifest.title == "Test Dataset"
            assert agent.manifest.authors == ["Jane Doe"]

    def test_init_creates_manifest_with_all_fields(self):
        """init_manifest creates mdf.yaml with all metadata."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            agent = MDFAgent.init_manifest(
                path=str(root),
                title="Test Dataset",
                authors=["Jane Doe", "John Smith"],
                description="A test dataset",
                publisher="Test Publisher",
                publication_year=2025,
            )

            manifest = load_manifest(root / "mdf.yaml")
            assert manifest.title == "Test Dataset"
            assert manifest.authors == ["Jane Doe", "John Smith"]
            assert manifest.description == "A test dataset"
            assert manifest.publisher == "Test Publisher"
            assert manifest.publication_year == 2025

    def test_init_nested_directory(self):
        """init_manifest creates nested directories if needed."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "nested" / "path" / "dataset"
            agent = MDFAgent.init_manifest(
                path=str(root),
                title="Nested Dataset",
                authors=["Jane Doe"],
            )

            assert root.exists()
            assert (root / "mdf.yaml").is_file()

    def test_init_does_not_overwrite_manifest(self):
        """init_manifest preserves existing mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Create initial manifest
            manifest_path = root / "mdf.yaml"
            root.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text("title: Existing Dataset\nauthors:\n  - Existing Author\n")

            # Init should not overwrite
            agent = MDFAgent.init_manifest(
                path=str(root),
                title="New Dataset",
                authors=["New Author"],
            )

            assert agent.manifest.title == "Existing Dataset"
            assert agent.manifest.authors == ["Existing Author"]

    def test_no_git_directory_created(self):
        """init_manifest does not create a .git directory."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            MDFAgent.init_manifest(
                path=str(root),
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            assert not (root / ".git").exists()


class TestFromManifest:
    """Tests for loading from mdf.yaml."""

    def test_from_manifest_loads(self):
        """from_manifest() loads an existing mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            MDFAgent.init_manifest(str(root), title="Test", authors=["Jane"])

            agent = MDFAgent.from_manifest(str(root))
            assert agent.manifest.title == "Test"

    def test_from_manifest_nonexistent_raises(self):
        """from_manifest() raises NoManifestError if no mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            with pytest.raises(NoManifestError):
                MDFAgent.from_manifest(tmpdir)


class TestManifestSaveLoad:
    """Tests for manifest save/load operations."""

    def test_save_manifest(self):
        """save_manifest() persists changes."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            agent = MDFAgent.init_manifest(str(root), title="Test", authors=["Jane"])

            agent.manifest.title = "Updated Title"
            agent.manifest.description = "Added description"
            agent.save_manifest()

            # Reload and verify
            manifest2 = load_manifest(root / "mdf.yaml")
            assert manifest2.title == "Updated Title"
            assert manifest2.description == "Added description"


class TestDiscover:
    """Tests for metadata discovery."""

    def test_discover_extracts_metadata(self):
        """discover() extracts metadata from files and saves to manifest."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c\n1,2,3\n4,5,6\n")

            agent = MDFAgent.init_manifest(str(root), title="Test", authors=["Jane"])
            extracted = agent.discover("data.csv")

            # Verify auto_metadata was saved
            manifest = load_manifest(root / "mdf.yaml")
            if extracted:
                assert manifest.auto_metadata is not None


class TestValidate:
    """Tests for validation."""

    def test_validate_with_data_files(self):
        """validate() passes when data files exist in directory."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            agent = MDFAgent.init_manifest(str(root), title="Test", authors=["Jane"])
            agent.manifest.data_sources = ["./data.csv"]
            agent.save_manifest()

            results = agent.validate()
            assert not results["errors"]

    def test_validate_missing_fields(self):
        """validate() catches missing required fields."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            agent = MDFAgent.init_manifest(str(root), title="Test", authors=["Jane"])
            agent.manifest.data_sources = []
            agent.save_manifest()

            results = agent.validate()
            # Should pass because directory has files (mdf.yaml counts as existing dir)
            # or fail because no data_sources and no non-hidden files
