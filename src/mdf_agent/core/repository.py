"""Repository management for git-style MDF workflow.

This module provides the Repository class which manages the .mdf/ directory
and tracks the state of a dataset through staging and commits.

The repository structure:
    my_dataset/
    ├── mdf.yaml           # Manifest configuration
    ├── .mdf/              # Repository state directory
    │   └── state.json     # Staged files, commits, etc.
    └── data/              # User's data files
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List

from mdf_agent.core.exceptions import NotARepositoryError
from mdf_agent.core.manifest import init_manifest, load_manifest, save_manifest
from mdf_agent.models.state import Commit, RepositoryState


class Repository:
    """Manages a git-style MDF repository.

    A repository tracks the state of a dataset directory, including:
    - Staged files (ready for the next commit)
    - Commit history
    - Manifest configuration (mdf.yaml)

    Attributes:
        root: The root directory of the repository.
        state: The current RepositoryState.
        state_file: Relative path to state.json.
        manifest_file: Relative path to mdf.yaml.
    """

    state_file = Path(".mdf/state.json")
    manifest_file = Path("mdf.yaml")

    def __init__(self, root: Path, state: RepositoryState) -> None:
        """Initialize a Repository instance.

        Args:
            root: Root directory of the repository.
            state: The RepositoryState to use.
        """
        self.root = root
        self.state = state

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
        (root / ".mdf").mkdir(parents=True, exist_ok=True)
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
        state = RepositoryState(root=str(root))
        repo = cls(root=root, state=state)
        repo._save_state()
        return repo

    @classmethod
    def load(cls, root: Path) -> "Repository":
        state_path = root / cls.state_file
        if not state_path.exists():
            raise NotARepositoryError(str(root))
        state_data = json.loads(state_path.read_text(encoding="utf-8"))
        state = RepositoryState(**state_data)
        return cls(root=root, state=state)

    def load_manifest(self):
        return load_manifest(self.root / self.manifest_file)

    def save_manifest(self, config) -> None:
        save_manifest(config, self.root / self.manifest_file)

    def stage(self, paths: Iterable[str]) -> List[str]:
        resolved: List[str] = []
        for pattern in paths:
            matches = list(self.root.glob(pattern))
            if not matches:
                candidate = self.root / pattern
                if candidate.exists():
                    matches = [candidate]
            for match in matches:
                if match.is_dir():
                    resolved.append(str(match.relative_to(self.root)))
                else:
                    resolved.append(str(match.relative_to(self.root)))
        if not resolved:
            raise FileNotFoundError("No files matched the provided paths")
        staged = set(self.state.staged_files)
        staged.update(resolved)
        self.state.staged_files = sorted(staged)
        self._save_state()
        return resolved

    def commit(self, message: str) -> Commit:
        if not self.state.staged_files:
            raise ValueError("No staged files to commit")
        commit = Commit(message=message, staged_files=list(self.state.staged_files))
        self.state.commits.append(commit)
        self.state.staged_files = []
        self._save_state()
        return commit

    def _save_state(self) -> None:
        state_path = self.root / self.state_file
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps(self.state.model_dump(), indent=2),
            encoding="utf-8",
        )
