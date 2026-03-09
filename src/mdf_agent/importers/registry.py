"""Adapter registry — resolves identifiers to the appropriate adapter."""

from __future__ import annotations

from typing import Optional

from mdf_agent.importers.base import ExternalRepoAdapter
from mdf_agent.importers.zenodo import ZenodoAdapter

# Register adapters in priority order
_ADAPTERS = [
    ZenodoAdapter(),
]


def resolve_adapter(identifier: str) -> Optional[ExternalRepoAdapter]:
    """Return the first adapter that can handle the given identifier, or None."""
    for adapter in _ADAPTERS:
        if adapter.match(identifier):
            return adapter
    return None
