"""Tests for MDF Agent git-backed Repository operations."""

import subprocess
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from mdf_agent.core.exceptions import NotARepositoryError
from mdf_agent.core.repository import Repository


def _git(args, cwd):
    """Helper to run git commands in tests."""
    return subprocess.run(
        ["git"] + args, cwd=str(cwd), capture_output=True, text=True, check=True,
    )


class TestRepositoryInit:
    """Tests for repository initialization."""

    def test_init_creates_structure(self):
        """init_repo creates .git/, .gitignore, and mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(
                root=root,
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            assert (root / ".git").is_dir()
            assert (root / "mdf.yaml").is_file()
            assert (root / ".gitignore").is_file()

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

    def test_init_creates_git_commit(self):
        """init_repo creates an initial git commit."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            Repository.init_repo(
                root=root,
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            result = _git(["log", "--oneline"], cwd=root)
            assert "Initialize MDF dataset" in result.stdout

    def test_init_nested_directory(self):
        """init_repo creates nested directories if needed."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "nested" / "path" / "dataset"
            Repository.init_repo(
                root=root,
                title="Nested Dataset",
                authors=["Jane Doe"],
            )

            assert root.exists()
            assert (root / ".git").is_dir()
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

    def test_init_existing_git_repo(self):
        """init_repo works in an existing git repo without re-initializing."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _git(["init"], cwd=root)
            # Create an existing commit
            (root / "README.md").write_text("hello")
            _git(["add", "README.md"], cwd=root)
            _git(["commit", "-m", "Initial commit"], cwd=root)

            repo = Repository.init_repo(
                root=root,
                title="Test Dataset",
                authors=["Jane Doe"],
            )

            # Should have both commits
            result = _git(["log", "--oneline"], cwd=root)
            assert "Initial commit" in result.stdout
            assert "Initialize MDF dataset" in result.stdout

    def test_init_gitignore_contains_mdf(self):
        """init_repo creates .gitignore that excludes .mdf/."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            Repository.init_repo(root=root, title="Test", authors=["Jane"])

            gitignore = (root / ".gitignore").read_text()
            assert ".mdf/" in gitignore


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

    def test_load_nonexistent_raises(self):
        """load() raises NotARepositoryError if no mdf.yaml or not a git repo."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with pytest.raises(NotARepositoryError):
                Repository.load(root)

    def test_load_no_git_raises(self):
        """load() raises NotARepositoryError if mdf.yaml exists but no git repo."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "mdf.yaml").write_text("title: Test\nauthors:\n  - Jane\n")
            with pytest.raises(NotARepositoryError):
                Repository.load(root)


class TestRepositoryStage:
    """Tests for staging files."""

    def test_stage_single_file(self):
        """stage() adds a single file to git staging area."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            staged = repo.stage(["data.csv"])

            assert staged == ["data.csv"]
            # Verify git sees it as staged
            result = _git(["diff", "--cached", "--name-only"], cwd=root)
            assert "data.csv" in result.stdout

    def test_stage_multiple_files(self):
        """stage() adds multiple files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data1.csv").write_text("a,b,c")
            (root / "data2.csv").write_text("d,e,f")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            staged = repo.stage(["data1.csv", "data2.csv"])

            assert len(staged) == 2

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


class TestRepositoryCommit:
    """Tests for committing staged files."""

    def test_commit_creates_git_commit(self):
        """commit() creates a real git commit."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            commit_data = repo.commit("Add data file")

            assert commit_data["message"] == "Add data file"
            assert "data.csv" in commit_data["staged_files"]
            assert len(commit_data["hash"]) == 40  # full SHA

            # Verify in git log
            result = _git(["log", "--oneline"], cwd=root)
            assert "Add data file" in result.stdout

    def test_commit_no_staged_raises(self):
        """commit() raises if nothing is staged."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            with pytest.raises(ValueError, match="No staged files"):
                repo.commit("Empty commit")

    def test_multiple_commits(self):
        """Multiple commits are recorded in git log."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data1.csv").write_text("a,b,c")
            (root / "data2.csv").write_text("d,e,f")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            repo.stage(["data1.csv"])
            repo.commit("First commit")

            repo.stage(["data2.csv"])
            repo.commit("Second commit")

            result = _git(["log", "--oneline"], cwd=root)
            assert "First commit" in result.stdout
            assert "Second commit" in result.stdout


class TestRepositoryStatus:
    """Tests for repository status."""

    def test_get_status_clean(self):
        """get_status() shows clean state after commit."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            status = repo.get_status()
            assert status["staged_files"] == []
            assert status["modified_files"] == []
            assert status["untracked_files"] == []
            assert len(status["commits"]) >= 1

    def test_get_status_with_staged(self):
        """get_status() shows staged files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])

            status = repo.get_status()
            assert "data.csv" in status["staged_files"]

    def test_get_status_with_untracked(self):
        """get_status() shows untracked files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            (root / "new_file.csv").write_text("data")

            status = repo.get_status()
            assert "new_file.csv" in status["untracked_files"]

    def test_get_status_with_modified(self):
        """get_status() shows modified tracked files."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            repo.commit("Add data")

            # Modify the file
            (root / "data.csv").write_text("a,b,c,d")

            status = repo.get_status()
            assert "data.csv" in status["modified_files"]


class TestRepositoryTrackedFiles:
    """Tests for get_tracked_files."""

    def test_tracked_excludes_infrastructure(self):
        """get_tracked_files() excludes mdf.yaml and .gitignore."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            repo.stage(["data.csv"])
            repo.commit("Add data")

            tracked = repo.get_tracked_files()
            assert "data.csv" in tracked
            assert "mdf.yaml" not in tracked
            assert ".gitignore" not in tracked


class TestRepositoryTags:
    """Tests for git tag operations."""

    def test_tag_and_list(self):
        """tag() creates tags, get_tags() lists them."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])

            repo.tag("mdf/v1.0")
            tags = repo.get_tags()
            assert "mdf/v1.0" in tags

    def test_has_commits(self):
        """has_commits() returns True after init."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = Repository.init_repo(root, title="Test", authors=["Jane"])
            assert repo.has_commits()


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
