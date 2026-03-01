from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import yaml

from mdf_agent.extractors.base import BaseExtractor


class JsonYamlExtractor(BaseExtractor):
    extensions = [".json", ".yml", ".yaml"]

    @classmethod
    def extract(cls, path: Path) -> Dict:
        data = cls._load(path)
        if data is None:
            return {}

        schema = cls._infer_schema(data)
        return {"mdf": {"json_schema": {"file": path.name, "schema": schema}}}

    @staticmethod
    def _load(path: Path) -> Any:
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            return None

        try:
            if path.suffix.lower() == ".json":
                return json.loads(content)
            return yaml.safe_load(content)
        except Exception:
            return None

    @classmethod
    def _infer_schema(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return {key: cls._infer_schema(value) for key, value in data.items()}
        if isinstance(data, list):
            if not data:
                return []
            return [cls._infer_schema(data[0])]
        return type(data).__name__
