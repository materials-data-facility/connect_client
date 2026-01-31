from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

from mdf_agent.core.utils import deep_merge
from mdf_agent.extractors.json_yaml import JsonYamlExtractor
from mdf_agent.extractors.pdf import PDFExtractor
from mdf_agent.extractors.tabular import TabularExtractor

EXTRACTORS = [PDFExtractor, TabularExtractor, JsonYamlExtractor]


def discover_metadata(paths: Iterable[str]) -> Dict:
    aggregated: Dict = {}
    for path_str in paths:
        path = Path(path_str)
        for extractor in EXTRACTORS:
            if extractor.can_extract(path):
                extracted = extractor.extract(path)
                if extracted:
                    aggregated = deep_merge(aggregated, extracted)
                break
    return aggregated
