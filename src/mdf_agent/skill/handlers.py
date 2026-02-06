from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from mdf_agent.core.backend_client import BackendClient
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
    service_instance: str = "prod",
    api_url: Optional[str] = None,
    dev_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    # client_id/scope are retained for backward compatibility with existing skill callers.
    _ = (client_id, scope)
    agent = MDFAgent.from_repo(path)
    return agent.publish(
        test=test,
        update=update,
        dry_run=not submit,
        token=token,
        service_instance=service_instance,
        api_url=api_url,
        dev_user_id=dev_user_id,
    )


def stream_create(
    title: str,
    lab_id: str | None = None,
    organization: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
    result = client.stream_create(title, lab_id=lab_id, organization=organization)
    client.close()
    return result


def stream_append(
    stream_id: str,
    files: list | None = None,
    file_count: int | None = None,
    total_bytes: int | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
    result = client.stream_append(stream_id, files=files, file_count=file_count, total_bytes=total_bytes)
    client.close()
    return result


def stream_status(
    stream_id: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
    result = client.stream_status(stream_id)
    client.close()
    return result


def stream_close(
    stream_id: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
    result = client.stream_close(stream_id)
    client.close()
    return result


def stream_snapshot(
    stream_id: str,
    title: str | None = None,
    update: bool = False,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
    result = client.stream_snapshot(stream_id, title=title, update=update)
    client.close()
    return result
