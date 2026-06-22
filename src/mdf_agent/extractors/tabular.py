from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

from mdf_agent.extractors.base import BaseExtractor


class TabularExtractor(BaseExtractor):
    extensions = [".csv", ".tsv", ".xlsx", ".xls"]

    @classmethod
    def extract(cls, path: Path) -> Dict:
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return cls._extract_excel(path)
        return cls._extract_csv(path)

    @staticmethod
    def _extract_excel(path: Path) -> Dict:
        try:
            import pandas as pd
        except Exception:
            return {}

        try:
            df = pd.read_excel(path)
        except Exception:
            return {}

        return {
            "mdf": {
                "table_schema": {
                    "file": path.name,
                    "columns": [{"name": col, "dtype": str(dtype)} for col, dtype in df.dtypes.items()],
                    "row_count": int(df.shape[0]),
                }
            }
        }

    @staticmethod
    def _extract_csv(path: Path) -> Dict:
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                # Tab-separated files need a tab delimiter, otherwise the whole
                # header row collapses into a single mis-named column.
                delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
                reader = csv.reader(handle, delimiter=delimiter)
                headers = next(reader, [])
                row_count = sum(1 for _ in reader)
        except Exception:
            return {}

        columns: List[Dict[str, str]] = [{"name": header} for header in headers if header]
        return {
            "mdf": {
                "table_schema": {
                    "file": path.name,
                    "columns": columns,
                    "row_count": row_count,
                }
            }
        }
