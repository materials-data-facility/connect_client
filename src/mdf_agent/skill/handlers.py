from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from mdf_agent.auth.globus import get_authorizer
from mdf_agent.core.agent import MDFAgent
from mdf_agent.extractors.registry import discover_metadata


def scan_folder(path: str) -> Dict[str, Any]:
    root = Path(path)
    files = [str(p) for p in root.rglob("*") if p.is_file()]
    return discover_metadata(files)


def create_manifest(
    path: str,
    title: str,
    authors: List[str],
    description: Optional[str] = None,
) -> Dict[str, Any]:
    agent = MDFAgent.init(path=path, title=title, authors=authors, description=description)
    return agent.manifest.model_dump()


def suggest_mappings(headers: List[str]) -> Dict[str, str]:
    suggestions: Dict[str, str] = {}
    for header in headers:
        key = header.lower()
        if "temp" in key:
            suggestions[header] = "measurement.temperature"
        elif "energy" in key:
            suggestions[header] = "dft.formation_energy"
        elif "comp" in key:
            suggestions[header] = "material.composition"
    return suggestions


def validate_and_preview(path: str) -> Dict[str, Any]:
    agent = MDFAgent.from_repo(path)
    validation = agent.validate()
    submission = agent.build_submission()
    return {"validation": validation, "submission": submission}


def publish(
    path: str,
    test: bool = False,
    update: bool = False,
    submit: bool = False,
    token: Optional[str] = None,
    client_id: Optional[str] = None,
    scope: Optional[str] = None,
) -> Dict[str, Any]:
    agent = MDFAgent.from_repo(path)
    authorizer = None
    if submit:
        authorizer = get_authorizer(token=token, client_id=client_id, scope=scope)
    return agent.publish(test=test, update=update, dry_run=not submit, authorizer=authorizer)
