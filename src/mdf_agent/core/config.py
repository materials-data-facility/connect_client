"""Global configuration for MDF Agent CLI.

Persists user defaults and last-publish state to ~/.config/mdf_agent/config.json.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


_CONFIG_DIR = Path.home() / ".config" / "mdf_agent"
_CONFIG_PATH = _CONFIG_DIR / "config.json"


class GlobalConfig:
    """JSON-backed global config at ~/.config/mdf_agent/config.json."""

    def __init__(self, path: Optional[Path] = None):
        self._path = path or _CONFIG_PATH
        self._data: dict = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self._data = {}
        else:
            self._data = {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, indent=2) + "\n", encoding="utf-8")

    # --- Typed properties ---

    @property
    def service(self) -> Optional[str]:
        return self.get("defaults.service")

    @property
    def last_source_id(self) -> Optional[str]:
        return self.get("last_publish.source_id")

    @property
    def last_version(self) -> Optional[str]:
        return self.get("last_publish.version")

    @property
    def organization(self) -> Optional[str]:
        return self.get("user.organization")

    @property
    def publisher(self) -> Optional[str]:
        return self.get("user.publisher")

    # --- Dotted key access ---

    def get(self, dotted_key: str, default: Any = None) -> Any:
        keys = dotted_key.split(".")
        node = self._data
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    def set(self, dotted_key: str, value: Any) -> None:
        keys = dotted_key.split(".")
        node = self._data
        for k in keys[:-1]:
            if k not in node or not isinstance(node[k], dict):
                node[k] = {}
            node = node[k]
        node[keys[-1]] = value
        self._save()

    # --- Convenience ---

    def record_publish(self, source_id: str, version: Optional[str], service: str) -> None:
        self._data.setdefault("last_publish", {})
        self._data["last_publish"].update(
            source_id=source_id,
            version=version,
            service=service,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self._save()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def data(self) -> dict:
        return self._data


def resolve_service(explicit: Optional[str]) -> str:
    """Resolve the service instance: explicit flag > config default > 'staging'."""
    if explicit:
        return explicit
    cfg = GlobalConfig()
    return cfg.service or "staging"
