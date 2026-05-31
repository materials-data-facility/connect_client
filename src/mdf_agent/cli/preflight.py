from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from mdf_agent.auth.globus import is_logged_in
from mdf_agent.core.agent import _extract_doi, _resolve_doi
from mdf_agent.core.backend_client import BackendClient
from mdf_agent.core.validation import validate_manifest
from mdf_agent.models.config import DataSource, ManifestConfig


REMOTE_PREFIXES = ("globus://", "https://", "http://", "stream://")
_SKIP_FILENAMES = {"mdf.yaml", ".gitignore"}


def _is_hidden_relative(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return any(part.startswith(".") for part in relative.parts)


def _iter_visible_files(root: Path) -> Iterable[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return (
        path
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and path.name not in _SKIP_FILENAMES
        and not _is_hidden_relative(path, root)
    )


def suggest_data_sources(root: Path, limit: int = 3) -> List[str]:
    """Suggest a few data source candidates from a dataset directory."""
    if not root.exists() or not root.is_dir():
        return []

    visible_dirs: List[str] = []
    visible_files: List[str] = []

    for child in sorted(root.iterdir()):
        if child.name.startswith(".") or child.name in _SKIP_FILENAMES:
            continue
        if child.is_dir():
            try:
                has_visible_files = any(_iter_visible_files(child))
            except OSError:
                has_visible_files = False
            if has_visible_files:
                visible_dirs.append(f"./{child.name}")
        elif child.is_file():
            visible_files.append(f"./{child.name}")

    suggestions = visible_dirs[:limit]
    if len(suggestions) < limit:
        suggestions.extend(visible_files[: limit - len(suggestions)])

    if suggestions:
        return suggestions[:limit]

    if any(_iter_visible_files(root)):
        return ["."]
    return []


def auth_ready(service: str, token: Optional[str] = None, dev_user: Optional[str] = None) -> bool:
    normalized = (service or "staging").strip().lower()
    if normalized == "local":
        return True
    if token or os.environ.get("MDF_CONNECT_TOKEN"):
        return True
    if dev_user or os.environ.get("MDF_DEV_USER_ID"):
        return True
    if os.environ.get("MDF_CLIENT_ID") and os.environ.get("MDF_CLIENT_SECRET"):
        return True
    return is_logged_in(service_instance=normalized)


def resolve_dataset_identifier(raw_id: str, client: Any) -> str:
    doi = _extract_doi(raw_id)
    if not doi:
        return raw_id
    resolved = _resolve_doi(client, doi)
    return resolved or raw_id


def _add_issue(issues: List[Dict[str, str]], severity: str, message: str, hint: Optional[str] = None) -> None:
    issue = {"severity": severity, "message": message}
    if hint:
        issue["hint"] = hint
    issues.append(issue)


def _inspect_local_path(path: Path, root: Path) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "kind": "local",
        "resolved": str(path),
        "file_count": 0,
        "total_bytes": 0,
        "hidden_skipped": 0,
    }
    if path.is_file():
        info["file_count"] = 1
        info["total_bytes"] = path.stat().st_size
        return info

    visible_files = list(_iter_visible_files(path))
    info["file_count"] = len(visible_files)
    info["total_bytes"] = sum(file_path.stat().st_size for file_path in visible_files)

    hidden_skipped = 0
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.name in _SKIP_FILENAMES or _is_hidden_relative(file_path, path):
            hidden_skipped += 1
    info["hidden_skipped"] = hidden_skipped

    if path.is_symlink():
        try:
            resolved = path.resolve()
            if not str(resolved).startswith(str(root.resolve())):
                info["outside_root"] = True
        except OSError:
            info["outside_root"] = True
    return info


def run_preflight(
    manifest: ManifestConfig,
    root: Path,
    service: str,
    api_url: Optional[str] = None,
    token: Optional[str] = None,
    dev_user: Optional[str] = None,
    submit: bool = False,
) -> Dict[str, Any]:
    """Run local and optional remote preflight checks for publish/update flows."""
    issues: List[Dict[str, str]] = []
    inspected_sources: List[Dict[str, Any]] = []

    has_visible_root_files = any(_iter_visible_files(root))
    errors, warnings = validate_manifest(manifest, has_data_files=has_visible_root_files)
    for message in errors:
        _add_issue(issues, "blocking", message)
    for message in warnings:
        _add_issue(issues, "warning", message)

    total_files = 0
    total_bytes = 0

    if manifest.data_sources:
        for source in manifest.data_sources:
            if isinstance(source, DataSource):
                source_label = source.path
                base_path = (root / source.path).resolve()
            else:
                source_label = str(source)
                if source_label.startswith(REMOTE_PREFIXES):
                    inspected_sources.append(
                        {
                            "source": source_label,
                            "kind": "remote",
                            "resolved": source_label,
                            "file_count": None,
                            "total_bytes": None,
                        }
                    )
                    continue
                base_path = (root / source_label).resolve()

            if not base_path.exists():
                _add_issue(
                    issues,
                    "blocking",
                    f"Data source does not exist: {source_label}",
                    "Fix the path in mdf.yaml or pass an existing file/directory to the command.",
                )
                continue

            info = _inspect_local_path(base_path, root)
            info["source"] = source_label

            if isinstance(source, DataSource) and source.include:
                matched = info["file_count"]
                if matched == 0:
                    _add_issue(
                        issues,
                        "blocking",
                        f"Data source include patterns matched no files: {source_label}",
                        "Update the include/exclude rules or point at a directory with visible files.",
                    )
                info["include_patterns"] = list(source.include)
            elif base_path.is_dir() and info["file_count"] == 0:
                _add_issue(
                    issues,
                    "blocking",
                    f"Data source directory is empty: {source_label}",
                    "Add visible files to the directory or remove it from data_sources.",
                )

            if info.get("hidden_skipped"):
                _add_issue(
                    issues,
                    "info",
                    f"{info['hidden_skipped']} hidden file(s) will be skipped under {source_label}",
                )
            if info.get("outside_root"):
                _add_issue(
                    issues,
                    "warning",
                    f"Symlinked source resolves outside the dataset root: {source_label}",
                    "Verify that the resolved target is intended before submitting.",
                )

            total_files += int(info["file_count"] or 0)
            total_bytes += int(info["total_bytes"] or 0)
            inspected_sources.append(info)
    elif has_visible_root_files:
        auto_files = list(_iter_visible_files(root))
        total_files = len(auto_files)
        total_bytes = sum(path.stat().st_size for path in auto_files)
        hidden_skipped = sum(
            1
            for path in root.rglob("*")
            if path.is_file() and (path.name in _SKIP_FILENAMES or _is_hidden_relative(path, root))
        )
        inspected_sources.append(
            {
                "source": ".",
                "kind": "auto-scan",
                "resolved": str(root),
                "file_count": total_files,
                "total_bytes": total_bytes,
                "hidden_skipped": hidden_skipped,
            }
        )
        _add_issue(
            issues,
            "info",
            "No data_sources set; publish/update will auto-include visible files from the current directory.",
            "Add data_sources to mdf.yaml if you want a narrower upload scope.",
        )

    service_target = api_url or os.environ.get("MDF_API_URL")
    if submit:
        if not auth_ready(service, token=token, dev_user=dev_user):
            _add_issue(
                issues,
                "blocking",
                f"Authentication is not ready for service '{service}'.",
                f"Run `mdf login --service {service}`, set MDF_CONNECT_TOKEN, or set MDF_CLIENT_ID + MDF_CLIENT_SECRET for CI/automation.",
            )
        if service_target:
            try:
                client = BackendClient(base_url=service_target)
                try:
                    health = client.health()
                finally:
                    client.close()
                if not health.get("success", health.get("status") == "ok"):
                    _add_issue(
                        issues,
                        "warning",
                        f"Backend health check returned an unexpected response for {service_target}.",
                    )
            except Exception as exc:
                _add_issue(
                    issues,
                    "blocking",
                    f"Cannot reach backend at {service_target}: {exc}",
                    "Fix the service selection or API URL before submitting.",
                )

    blocking = [issue for issue in issues if issue["severity"] == "blocking"]
    return {
        "success": not blocking,
        "issues": issues,
        "source_summary": {
            "count": len(inspected_sources),
            "files": total_files,
            "bytes": total_bytes,
        },
        "sources": inspected_sources,
        "service": service,
        "target_url": service_target,
    }
