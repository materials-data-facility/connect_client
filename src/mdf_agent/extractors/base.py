from __future__ import annotations

from pathlib import Path
from typing import Dict, List


class BaseExtractor:
    extensions: List[str] = []

    @classmethod
    def can_extract(cls, path: Path) -> bool:
        return path.suffix.lower() in cls.extensions

    @classmethod
    def extract(cls, path: Path) -> Dict:
        raise NotImplementedError
