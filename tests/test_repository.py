"""Tests for MDF Agent Repository operations."""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from mdf_agent.core.exceptions import NotARepositoryError
from mdf_agent.core.repository import Repository
from mdf_agent.models.state import Commit, RepositoryState


class TestRepositoryInit:
    """Tests for repository initialization."""

    def test_init_creates_structure(self):
        """init_repo creates .mdf/ and mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(
                root=root,
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            assert (root / ".mdf").is_dir()
            assert (root / ".mdf" / "state.json").is_file()
            assert (root / "mdf.yaml").is_file()

    def test_init_creates_manifest_with_metadata(self):
        """init_repo creates mdf.yaml with correct content."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(
                root=root,
                title="Test Dataset",
                authors=["Jane Doe", "John Smith"],
                description="A test dataset",
                publisher="Test Publisher",
                publication_year=2025,
            )

            manifest = repo.load_manifest()
            assert manifest.title == "Test Dataset"
            assert manifest.authors == ["Jane Doe", "John Smith"]
            assert manifest.description == "A test dataset"
            assert manifest.publisher == "Test Publisher"
            assert manifest.publication_year == 2025

    def test_init_creates_state_json(self):
        """init_repo creates valid state.json."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(
                root=root,
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            state_path = root / ".mdf" / "state.json"
            state_data = json.loads(state_path.read_text())
            assert state_data["version"] == "1"
            assert state_data["root"] == str(root)
            assert state_data["staged_files"] == []
            assert state_data["commits"] == []

    def test_init_nested_directory(self):
        """init_repo creates nested directories if needed."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "nested" / "path" / "dataset"
            repo = Repository.init_repo(
                root=root,
                title="Nested Dataset",
                authors=["Jane Doe"],
            )

            assert root.exists()
            assert (root / ".mdf").is_dir()
            assert (root / "mdf.yaml").is_file()

    def test_init_does_not_overwrite_manifest(self):
        """init_repo preserves existing mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Create initial manifest
            manifest_path = root / "mdf.yaml"
            root.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text("title: Existing Dataset\nauthors:\n  - Existing Author\n")

            # Init should not overwrite
            repo = Repository.init_repo(
                root=root,
                title="New Dataset",
                authors=["New Author"],
            )

            manifest = repo.load_manifest()
            assert manifest.title == "Existing Dataset"
            assert manifest.authors == ["Existing Author"]


class TestRepositoryLoad:
    """Tests for loading existing repositories."""

    def test_load_existing_repo(self):
        """load() loads an existing repository."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Create a repo
            Repository.init_repo(root, title="Test", authors=["Jane"])

            # Load it
            repo = Repository.load(root)
            assert repo.root == root
            assert repo.state.staged_files == []

    def test_load_nonexistent_raises(self):
        """load() raises NotARepositoryError if .mdf/state.json doesn't exist."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with pytest.raises(NotARepositoryError):
                Repository.load(root)

    def test_load_preserves_state(self):
        """load() preserves staged files and commits."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Create repo with some files
            (root / "data.csv").write_text("a,b,c")
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            repo.commit("Add data")

            # Load and verify
            repo2 = Repository.load(root)
            assert len(repo2.state.commits) == 1
            assert repo2.state.commits[0].message == "Add data"


class TestRepositoryStage:
    """Tests for staging files."""

    def test_stage_single_file(self):
        """stage() adds a single file."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            staged = repo.stage(["data.csv"])

            assert staged == ["data.csv"]
            assert "data.csv" in repo.state.staged_files

    def test_stage_multiple_files(self):
        """stage() adds multiple files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data1.csv").write_text("a,b,c")
            (root / "data2.csv").write_text("d,e,f")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            staged = repo.stage(["data1.csv", "data2.csv"])

            assert len(staged) == 2
            assert "data1.csv" in repo.state.staged_files
            assert "data2.csv" in repo.state.staged_files

    def test_stage_glob_pattern(self):
        """stage() supports glob patterns."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data1.csv").write_text("a,b,c")
            (root / "data2.csv").write_text("d,e,f")
            (root / "readme.txt").write_text("test")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            staged = repo.stage(["*.csv"])

            assert len(staged) == 2
            assert all(f.endswith(".csv") for f in staged)

    def test_stage_directory(self):
        """stage() handles directories."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            data_dir.mkdir()
            (data_dir / "file1.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            staged = repo.stage(["data"])

            assert "data" in staged

    def test_stage_no_match_raises(self):
        """stage() raises if no files match."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            with pytest.raises(FileNotFoundError, match="No files matched"):
                repo.stage(["nonexistent.csv"])

    def test_stage_deduplicates(self):
        """stage() doesn't duplicate already-staged files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            repo.stage(["data.csv"])  # Stage again

            assert repo.state.staged_files.count("data.csv") == 1

    def test_stage_persists_to_disk(self):
        """stage() saves state to disk."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])

            # Read state from disk
            state_data = json.loads((root / ".mdf" / "state.json").read_text())
            assert "data.csv" in state_data["staged_files"]


class TestRepositoryCommit:
    """Tests for committing staged files."""

    def test_commit_creates_record(self):
        """commit() creates a commit record."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            commit = repo.commit("Add data file")

            assert commit.message == "Add data file"
            assert "data.csv" in commit.staged_files
            assert len(repo.state.commits) == 1

    def test_commit_clears_staged(self):
        """commit() clears staged files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            repo.commit("Add data")

            assert repo.state.staged_files == []

    def test_commit_no_staged_raises(self):
        """commit() raises if nothing is staged."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            with pytest.raises(ValueError, match="No staged files"):
                repo.commit("Empty commit")

    def test_commit_has_timestamp(self):
        """commit() includes timestamp."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            commit = repo.commit("Add data")

            assert commit.timestamp is not None
            assert "T" in commit.timestamp  # ISO format

    def test_multiple_commits(self):
        """Multiple commits are recorded in order."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data1.csv").write_text("a,b,c")
            (root / "data2.csv").write_text("d,e,f")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            repo.stage(["data1.csv"])
            repo.commit("First commit")

            repo.stage(["data2.csv"])
            repo.commit("Second commit")

            assert len(repo.state.commits) == 2
            assert repo.state.commits[0].message == "First commit"
            assert repo.state.commits[1].message == "Second commit"

    def test_commit_persists_to_disk(self):
        """commit() saves state to disk."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            repo.commit("Add data")

            # Read state from disk
            state_data = json.loads((root / ".mdf" / "state.json").read_text())
            assert len(state_data["commits"]) == 1
            assert state_data["commits"][0]["message"] == "Add data"


class TestRepositoryManifest:
    """Tests for manifest operations."""

    def test_load_manifest(self):
        """load_manifest() returns ManifestConfig."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            manifest = repo.load_manifest()
            assert manifest.title == "Test"
            assert manifest.authors == ["Jane"]

    def test_save_manifest(self):
        """save_manifest() persists changes."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            manifest = repo.load_manifest()
            manifest.title = "Updated Title"
            manifest.description = "Added description"
            repo.save_manifest(manifest)

            # Reload and verify
            manifest2 = repo.load_manifest()
            assert manifest2.title == "Updated Title"
            assert manifest2.description == "Added description"


class TestStateModels:
    """Tests for state Pydantic models."""

    def test_commit_defaults(self):
        """Commit has sensible defaults."""
        commit = Commit(message="Test")
        assert commit.message == "Test"
        assert commit.timestamp is not None
        assert commit.staged_files == []

    def test_commit_with_files(self):
        """Commit stores staged files."""
        commit = Commit(message="Test", staged_files=["a.csv", "b.csv"])
        assert commit.staged_files == ["a.csv", "b.csv"]

    def test_repository_state_defaults(self):
        """RepositoryState has sensible defaults."""
        state = RepositoryState(root="/path/to/repo")
        assert state.version == "1"
        assert state.root == "/path/to/repo"
        assert state.staged_files == []
        assert state.commits == []

    def test_repository_state_serialization(self):
        """RepositoryState can serialize to/from JSON."""
        state = RepositoryState(
            root="/path/to/repo",
            staged_files=["a.csv"],
            commits=[Commit(message="Test", staged_files=["a.csv"])],
        )

        # Serialize
        data = state.model_dump()
        assert data["root"] == "/path/to/repo"
        assert data["staged_files"] == ["a.csv"]

        # Deserialize
        state2 = RepositoryState.model_validate(data)
        assert state2.root == "/path/to/repo"
        assert len(state2.commits) == 1
