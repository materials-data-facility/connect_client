from __future__ import annotations

import functools
from pathlib import Path
from typing import Any, Dict, List, Optional

from mdf_agent.core.agent import MDFAgent
from mdf_agent.core.backend_client import BackendClient
from mdf_agent.extractors.registry import discover_metadata


def agent_safe(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            if isinstance(result, dict) and "success" not in result:
                result["success"] = True
            return result
        except Exception as exc:
            return {
                "success": False,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }

    return wrapper


def _run_backend_action(
    action: str,
    *,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
    **kwargs,
) -> Dict[str, Any]:
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
    try:
        method = getattr(client, action)
        return method(**kwargs)
    finally:
        client.close()


@agent_safe
def scan_folder(path: str) -> Dict[str, Any]:
    root = Path(path)
    files = [str(p) for p in root.rglob("*") if p.is_file()]
    return discover_metadata(files)


@agent_safe
def create_manifest(
    path: str,
    title: str,
    authors: List[str],
    description: Optional[str] = None,
) -> Dict[str, Any]:
    agent = MDFAgent.init_manifest(path=path, title=title, authors=authors, description=description)
    return agent.manifest.model_dump()


@agent_safe
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


@agent_safe
def validate_and_preview(path: str) -> Dict[str, Any]:
    agent = MDFAgent.from_manifest(path)
    validation = agent.validate()
    submission = agent.build_submission()
    return {"validation": validation, "submission": submission}


@agent_safe
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
    agent = MDFAgent.from_manifest(path)
    return agent.publish(
        test=test,
        update=update,
        dry_run=not submit,
        token=token,
        service_instance=service_instance,
        api_url=api_url,
        dev_user_id=dev_user_id,
    )


@agent_safe
def stream_create(
    title: str,
    lab_id: str | None = None,
    organization: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "stream_create",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        title=title,
        lab_id=lab_id,
        organization=organization,
    )


@agent_safe
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
    return _run_backend_action(
        "stream_append",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        stream_id=stream_id,
        files=files,
        file_count=file_count,
        total_bytes=total_bytes,
    )


@agent_safe
def stream_status(
    stream_id: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "stream_status",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        stream_id=stream_id,
    )


@agent_safe
def stream_close(
    stream_id: str,
    mint_doi: bool | None = None,
    title: str | None = None,
    description: str | None = None,
    authors: list | None = None,
    keywords: list | None = None,
    license: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "stream_close",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        stream_id=stream_id,
        mint_doi=mint_doi,
        title=title,
        description=description,
        authors=authors,
        keywords=keywords,
        license=license,
    )


@agent_safe
def stream_snapshot(
    stream_id: str,
    title: str | None = None,
    update: bool = False,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "stream_snapshot",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        stream_id=stream_id,
        title=title,
        update=update,
    )


@agent_safe
def curation_list_pending(
    limit: int = 50,
    offset: int = 0,
    organization: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "curation_pending",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        limit=limit,
        offset=offset,
        organization=organization,
    )


@agent_safe
def curation_review(
    source_id: str,
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "curation_detail",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        version=version,
    )


@agent_safe
def curation_approve(
    source_id: str,
    mint_doi: bool = True,
    notes: str | None = None,
    metadata_updates: dict | None = None,
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "curation_approve",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        mint_doi=mint_doi,
        notes=notes,
        metadata_updates=metadata_updates,
        version=version,
    )


@agent_safe
def curation_reject(
    source_id: str,
    reason: str,
    suggestions: str | None = None,
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "curation_reject",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        reason=reason,
        suggestions=suggestions,
        version=version,
    )


@agent_safe
def check_status(
    source_id: str,
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "status",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        version=version,
    )


@agent_safe
def list_submissions(
    organization: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "submissions",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        organization=organization,
    )


@agent_safe
def search_datasets(
    query: str,
    search_type: str = "all",
    limit: int = 20,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "search",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        query=query,
        search_type=search_type,
        limit=limit,
    )


@agent_safe
def get_citation(
    source_id: str,
    format: str = "all",
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "get_citation",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        format=format,
        version=version,
    )


@agent_safe
def get_card(
    source_id: str,
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "get_card",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        version=version,
    )


@agent_safe
def dataset_preview(
    source_id: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "dataset_preview",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
    )


@agent_safe
def dataset_sample(
    source_id: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "dataset_sample",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
    )


@agent_safe
def edit_metadata(
    source_id: str,
    version: str | None = None,
    title: str | None = None,
    description: str | None = None,
    keywords: list | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    fields = {}
    if title is not None:
        fields["title"] = title
    if description is not None:
        fields["description"] = description
    if keywords is not None:
        fields["keywords"] = keywords
    return _run_backend_action(
        "edit_metadata",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        version=version,
        **fields,
    )


@agent_safe
def withdraw(
    source_id: str,
    reason: str = "",
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "withdraw",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        reason=reason,
        version=version,
    )


@agent_safe
def resubmit(
    source_id: str,
    notes: str = "",
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "resubmit",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        notes=notes,
        version=version,
    )


@agent_safe
def version_diff(
    source_id: str,
    from_version: str,
    to_version: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "version_diff",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        from_version=from_version,
        to_version=to_version,
    )


@agent_safe
def delete_submission(
    source_id: str,
    reason: str,
    version: str | None = None,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "delete_submission",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
        reason=reason,
        version=version,
    )


@agent_safe
def admin_stats(
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "admin_stats",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )


@agent_safe
def dataset_stats(
    source_id: str,
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "dataset_stats",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
        source_id=source_id,
    )


@agent_safe
def health_check(
    api_url: str | None = None,
    token: str | None = None,
    service_instance: str = "prod",
    dev_user_id: str | None = None,
) -> Dict[str, Any]:
    return _run_backend_action(
        "health",
        api_url=api_url,
        token=token,
        service_instance=service_instance,
        dev_user_id=dev_user_id,
    )
