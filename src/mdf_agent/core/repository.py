"""Repository management for git-backed MDF workflow.

This module provides the Repository class which manages a git-backed
dataset directory for tracking staged files, commits, and metadata.

The repository structure:
    my_dataset/
    ├── mdf.yaml           # Manifest configuration
    ├── .gitignore          # Ignores .mdf/ transient state
    └── data/              # User's data files
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterable, List, Optional

from mdf_agent.core.exceptions import NotARepositoryError
from mdf_agent.core.manifest import init_manifest, load_manifest, save_manifest


def _run_git(args: List[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    """Run a git command and return the result."""
    return subprocess.run(
        ["git"] + args,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=check,
    )


class Repository:
    """Manages a git-backed MDF repository.

    A repository tracks the state of a dataset directory using git,
    including staging, commits, and metadata via mdf.yaml.

    Attributes:
        root: The root directory of the repository.
        manifest_file: Relative path to mdf.yaml.
    """

    manifest_file = Path("mdf.yaml")

    def __init__(self, root: Path) -> None:
        self.root = root

    @classmethod
    def init_repo(
        cls,
        root: Path,
        title: str,
        authors: List[str],
        description: str | None = None,
        publisher: str | None = None,
        publication_year: int | str | None = None,
    ) -> "Repository":
        root.mkdir(parents=True, exist_ok=True)

        # Initialize git repo if not already one
        if not (root / ".git").is_dir():
            _run_git(["init"], cwd=root)

        # Create mdf.yaml if it doesn't exist
        manifest_path = root / cls.manifest_file
        if not manifest_path.exists():
            init_manifest(
                manifest_path,
                title=title,
                authors=authors,
                description=description,
                publisher=publisher,
                publication_year=publication_year,
            )

        # Create .gitignore if it doesn't exist
        gitignore_path = root / ".gitignore"
        if not gitignore_path.exists():
            gitignore_path.write_text(".mdf/\n", encoding="utf-8")

        # Stage and commit initial files
        files_to_add = ["mdf.yaml"]
        if gitignore_path.exists():
            files_to_add.append(".gitignore")
        _run_git(["add"] + files_to_add, cwd=root)

        # Only commit if there are staged changes
        status = _run_git(["diff", "--cached", "--quiet"], cwd=root, check=False)
        if status.returncode != 0:
            _run_git(["commit", "-m", "Initialize MDF dataset"], cwd=root)

        return cls(root=root)

    @classmethod
    def load(cls, root: Path) -> "Repository":
        """Load an existing MDF repository.

        A valid MDF repository is a git repo containing mdf.yaml.
        """
        manifest_path = root / cls.manifest_file
        if not manifest_path.exists():
            raise NotARepositoryError(str(root))
        # Verify it's a git repo
        result = _run_git(["rev-parse", "--git-dir"], cwd=root, check=False)
        if result.returncode != 0:
            raise NotARepositoryError(str(root))
        return cls(root=root)

    def load_manifest(self):
        return load_manifest(self.root / self.manifest_file)

    def save_manifest(self, config) -> None:
        save_manifest(config, self.root / self.manifest_file)

    def stage(self, paths: Iterable[str]) -> List[str]:
        """Stage files for the next commit using git add.

        Args:
            paths: File paths or glob patterns to stage.

        Returns:
            List of resolved file paths that were staged.

        Raises:
            FileNotFoundError: If no files match the provided paths.
        """
        resolved: List[str] = []
        for pattern in paths:
            matches = list(self.root.glob(pattern))
            if not matches:
                candidate = self.root / pattern
                if candidate.exists():
                    matches = [candidate]
            for match in matches:
                resolved.append(str(match.relative_to(self.root)))
        if not resolved:
            raise FileNotFoundError("No files matched the provided paths")

        _run_git(["add"] + resolved, cwd=self.root)
        return resolved

    def commit(self, message: str) -> dict:
        """Commit staged changes using git commit.

        Args:
            message: Commit message.

        Returns:
            Dict with commit info: message, files, hash.

        Raises:
            ValueError: If nothing is staged.
        """
        # Check if there are staged changes
        status = _run_git(["diff", "--cached", "--quiet"], cwd=self.root, check=False)
        if status.returncode == 0:
            raise ValueError("No staged files to commit")

        # Get list of staged files before committing
        diff_result = _run_git(
            ["diff", "--cached", "--name-only"],
            cwd=self.root,
        )
        staged_files = [f for f in diff_result.stdout.strip().split("\n") if f]

        _run_git(["commit", "-m", message], cwd=self.root)

        # Get the commit hash
        log_result = _run_git(["rev-parse", "HEAD"], cwd=self.root)
        commit_hash = log_result.stdout.strip()

        return {
            "message": message,
            "staged_files": staged_files,
            "hash": commit_hash,
        }

    def get_status(self) -> dict:
        """Get repository status using git status.

        Returns:
            Dict with staged, modified, untracked file lists and commit log.
        """
        # Staged files
        staged_result = _run_git(
            ["diff", "--cached", "--name-only"],
            cwd=self.root,
        )
        staged = [f for f in staged_result.stdout.strip().split("\n") if f]

        # Modified (unstaged)
        modified_result = _run_git(
            ["diff", "--name-only"],
            cwd=self.root,
        )
        modified = [f for f in modified_result.stdout.strip().split("\n") if f]

        # Untracked files
        untracked_result = _run_git(
            ["ls-files", "--others", "--exclude-standard"],
            cwd=self.root,
        )
        untracked = [f for f in untracked_result.stdout.strip().split("\n") if f]

        # Recent commits
        log_result = _run_git(
            ["log", "--oneline", "-20", "--format=%H\t%s\t%aI"],
            cwd=self.root,
            check=False,
        )
        commits = []
        if log_result.returncode == 0 and log_result.stdout.strip():
            for line in log_result.stdout.strip().split("\n"):
                parts = line.split("\t", 2)
                if len(parts) == 3:
                    commits.append({
                        "hash": parts[0],
                        "message": parts[1],
                        "timestamp": parts[2],
                    })

        return {
            "staged_files": staged,
            "modified_files": modified,
            "untracked_files": untracked,
            "commits": commits,
        }

    def get_tracked_files(self) -> List[str]:
        """Get all tracked files using git ls-files.

        Returns files tracked by git, excluding mdf.yaml and .gitignore.
        """
        result = _run_git(["ls-files"], cwd=self.root)
        all_files = [f for f in result.stdout.strip().split("\n") if f]
        # Filter out MDF infrastructure files
        return [f for f in all_files if f not in ("mdf.yaml", ".gitignore")]

    def has_commits(self) -> bool:
        """Check if the repo has any commits."""
        result = _run_git(["rev-parse", "HEAD"], cwd=self.root, check=False)
        return result.returncode == 0

    def tag(self, tag_name: str) -> None:
        """Create a git tag."""
        _run_git(["tag", tag_name], cwd=self.root)

    def get_tags(self) -> List[str]:
        """List all git tags."""
        result = _run_git(["tag", "-l"], cwd=self.root, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            return []
        return result.stdout.strip().split("\n")
