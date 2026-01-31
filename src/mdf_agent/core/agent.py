"""MDF Agent - Primary Python API.

This module provides the main MDFAgent class, which is the primary interface
for interacting with MDF datasets programmatically. It supports two modes:

1. Repository mode: Work with a git-style .mdf/ repository for tracking changes
2. Direct mode: Build and submit datasets without local state

Examples:
    Repository mode::

        agent = MDFAgent.init("./my_dataset", title="My Dataset", authors=["Jane Doe"])
        agent.add("data/*.csv", discover=True)
        agent.commit("Add experimental data")
        result = agent.publish(test=True)

    Direct mode::

        agent = MDFAgent()
        agent.set_title("My Dataset")
        agent.add_author("Jane Doe", affiliations=["MIT"])
        agent.add_data_source("globus://endpoint/path")
        result = agent.publish()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from mdf_agent.core.manifest import load_manifest, save_manifest
from mdf_agent.core.repository import Repository
from mdf_agent.core.submission import build_submission, submit_submission
from mdf_agent.core.validation import validate_manifest
from mdf_agent.extractors.registry import discover_metadata
from mdf_agent.models.config import Author, ManifestConfig


class MDFAgent:
    """Primary Python API for MDF Agent.

    This class provides a high-level interface for creating, managing, and
    publishing datasets to the Materials Data Facility (MDF).

    Attributes:
        root: Path to the dataset directory (repository mode) or None (direct mode).
        repo: Repository instance for git-style operations, or None.
        manifest: The ManifestConfig containing dataset metadata.
    """

    def __init__(self, root: Optional[Path] = None, manifest: Optional[ManifestConfig] = None):
        self.root = root
        self.repo: Optional[Repository] = None
        if root is not None:
            self.repo = Repository.load(root)
            self.manifest = self.repo.load_manifest()
        else:
            self.manifest = manifest or ManifestConfig()

    @classmethod
    def init(
        cls,
        path: str,
        title: str,
        authors: List[str],
        description: Optional[str] = None,
        publisher: Optional[str] = None,
        publication_year: Optional[int] = None,
    ) -> "MDFAgent":
        root = Path(path).resolve()
        repo = Repository.init_repo(
            root,
            title=title,
            authors=authors,
            description=description,
            publisher=publisher,
            publication_year=publication_year,
        )
        agent = cls(root=root)
        agent.repo = repo
        agent.manifest = repo.load_manifest()
        return agent

    @classmethod
    def from_repo(cls, path: str) -> "MDFAgent":
        root = Path(path).resolve()
        return cls(root=root)

    def save_manifest(self) -> None:
        if self.repo is None:
            raise ValueError("No repository attached")
        self.repo.save_manifest(self.manifest)

    def add(self, *paths: str, discover: Optional[bool] = None) -> List[str]:
        if self.repo is None:
            raise ValueError("Repository mode required for add")
        staged = self.repo.stage(paths)

        if discover or (discover is None and self.manifest.auto_discover):
            files = [str((self.root / path).resolve()) for path in staged]
            extracted = discover_metadata(files)
            if extracted:
                current = self.manifest.auto_metadata or {}
                current.update(extracted)
                self.manifest.auto_metadata = current
                self.save_manifest()
        return staged

    def commit(self, message: str) -> Dict[str, Any]:
        if self.repo is None:
            raise ValueError("Repository mode required for commit")
        commit = self.repo.commit(message)
        return commit.model_dump()

    def status(self) -> Dict[str, Any]:
        if self.repo is None:
            raise ValueError("Repository mode required for status")
        return self.repo.state.model_dump()

    def validate(self) -> Dict[str, List[str]]:
        errors, warnings = validate_manifest(self.manifest)
        return {"errors": errors, "warnings": warnings}

    def build_submission(self, test: bool = False, update: bool = False) -> Dict[str, Any]:
        root = self.root or Path.cwd()
        return build_submission(self.manifest, root, test=test, update=update)

    def publish(
        self,
        test: bool = False,
        update: bool = False,
        dry_run: bool = True,
        authorizer: Optional[Any] = None,
        service_instance: str = "prod",
    ) -> Dict[str, Any]:
        payload = self.build_submission(test=test, update=update)
        if dry_run:
            return {"success": True, "payload": payload}
        return submit_submission(payload, authorizer=authorizer, service_instance=service_instance)

    def set_title(self, title: str) -> None:
        self.manifest.title = title

    def add_author(self, name: str, affiliations: Optional[List[str]] = None, orcid: Optional[str] = None) -> None:
        if self.manifest.authors is None:
            self.manifest.authors = []
        author = Author(name=name, affiliations=affiliations or [], orcid=orcid)
        self.manifest.authors.append(author)

    def add_data_source(self, source: str) -> None:
        if self.manifest.data_sources is None:
            self.manifest.data_sources = []
        self.manifest.data_sources.append(source)
