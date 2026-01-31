from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pydantic import BaseModel, ConfigDict, Field


class Commit(BaseModel):
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    staged_files: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


class RepositoryState(BaseModel):
    version: str = "1"
    root: str
    staged_files: List[str] = Field(default_factory=list)
    commits: List[Commit] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")
