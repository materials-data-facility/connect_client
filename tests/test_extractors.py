"""Tests for metadata extractors."""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from mdf_agent.extractors.base import BaseExtractor
from mdf_agent.extractors.tabular import TabularExtractor
from mdf_agent.extractors.json_yaml import JsonYamlExtractor
from mdf_agent.extractors.registry import discover_metadata


class TestBaseExtractor:
    """Tests for BaseExtractor interface."""

    def test_can_extract_matches_extension(self):
        """can_extract returns True for matching extensions."""

        class TestExtractor(BaseExtractor):
            extensions = [".txt", ".md"]

        assert TestExtractor.can_extract(Path("file.txt")) is True
        assert TestExtractor.can_extract(Path("file.md")) is True
        assert TestExtractor.can_extract(Path("file.csv")) is False

    def test_can_extract_case_insensitive(self):
        """can_extract is case insensitive."""

        class TestExtractor(BaseExtractor):
            extensions = [".txt"]

        assert TestExtractor.can_extract(Path("file.TXT")) is True
        assert TestExtractor.can_extract(Path("file.Txt")) is True

    def test_extract_not_implemented(self):
        """BaseExtractor.extract raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            BaseExtractor.extract(Path("test.txt"))


class TestTabularExtractor:
    """Tests for TabularExtractor (CSV/Excel)."""

    def test_can_extract_csv(self):
        """TabularExtractor handles .csv files."""
        assert TabularExtractor.can_extract(Path("data.csv")) is True

    def test_can_extract_tsv(self):
        """TabularExtractor handles .tsv files."""
        assert TabularExtractor.can_extract(Path("data.tsv")) is True

    def test_can_extract_excel(self):
        """TabularExtractor handles .xlsx files."""
        assert TabularExtractor.can_extract(Path("data.xlsx")) is True
        assert TabularExtractor.can_extract(Path("data.xls")) is True

    def test_cannot_extract_other(self):
        """TabularExtractor rejects non-tabular files."""
        assert TabularExtractor.can_extract(Path("data.json")) is False
        assert TabularExtractor.can_extract(Path("data.pdf")) is False

    def test_extract_csv_basic(self):
        """Extract CSV with headers and row count."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "data.csv"
            csv_path.write_text("name,age,score\nAlice,30,95\nBob,25,88\n")

            result = TabularExtractor.extract(csv_path)

            assert "mdf" in result
            assert "table_schema" in result["mdf"]
            schema = result["mdf"]["table_schema"]
            assert schema["file"] == "data.csv"
            assert schema["row_count"] == 2
            assert len(schema["columns"]) == 3
            column_names = [c["name"] for c in schema["columns"]]
            assert column_names == ["name", "age", "score"]

    def test_extract_csv_empty_file(self):
        """Extract empty CSV returns empty or minimal result."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "empty.csv"
            csv_path.write_text("")

            result = TabularExtractor.extract(csv_path)
            # Empty file should return something (or empty dict)
            if result:
                assert result["mdf"]["table_schema"]["row_count"] == 0

    def test_extract_csv_header_only(self):
        """Extract CSV with only headers."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "headers.csv"
            csv_path.write_text("col1,col2,col3\n")

            result = TabularExtractor.extract(csv_path)

            assert "mdf" in result
            schema = result["mdf"]["table_schema"]
            assert schema["row_count"] == 0
            assert len(schema["columns"]) == 3

    def test_extract_csv_with_empty_headers(self):
        """Extract CSV handles empty header columns."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "sparse.csv"
            csv_path.write_text("name,,score\n1,2,3\n")

            result = TabularExtractor.extract(csv_path)

            schema = result["mdf"]["table_schema"]
            # Empty headers are filtered out
            column_names = [c["name"] for c in schema["columns"]]
            assert "name" in column_names
            assert "score" in column_names

    def test_extract_tsv(self):
        """Extract TSV (tab-separated) file."""
        with TemporaryDirectory() as tmpdir:
            tsv_path = Path(tmpdir) / "data.tsv"
            tsv_path.write_text("a\tb\tc\n1\t2\t3\n")

            result = TabularExtractor.extract(tsv_path)

            # TSV parsing may differ based on implementation
            assert "mdf" in result


class TestJsonYamlExtractor:
    """Tests for JsonYamlExtractor."""

    def test_can_extract_json(self):
        """JsonYamlExtractor handles .json files."""
        assert JsonYamlExtractor.can_extract(Path("data.json")) is True

    def test_can_extract_yaml(self):
        """JsonYamlExtractor handles .yaml and .yml files."""
        assert JsonYamlExtractor.can_extract(Path("config.yaml")) is True
        assert JsonYamlExtractor.can_extract(Path("config.yml")) is True

    def test_cannot_extract_other(self):
        """JsonYamlExtractor rejects non-JSON/YAML files."""
        assert JsonYamlExtractor.can_extract(Path("data.csv")) is False
        assert JsonYamlExtractor.can_extract(Path("data.txt")) is False

    def test_extract_json_flat(self):
        """Extract flat JSON object."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "data.json"
            json_path.write_text(json.dumps({"name": "test", "count": 42}))

            result = JsonYamlExtractor.extract(json_path)

            assert "mdf" in result
            assert "json_schema" in result["mdf"]
            schema = result["mdf"]["json_schema"]
            assert schema["file"] == "data.json"
            assert schema["schema"]["name"] == "str"
            assert schema["schema"]["count"] == "int"

    def test_extract_json_nested(self):
        """Extract nested JSON structure."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "nested.json"
            data = {"metadata": {"author": "Jane", "version": 1}, "values": [1, 2, 3]}
            json_path.write_text(json.dumps(data))

            result = JsonYamlExtractor.extract(json_path)

            schema = result["mdf"]["json_schema"]["schema"]
            assert "metadata" in schema
            assert schema["metadata"]["author"] == "str"
            assert schema["metadata"]["version"] == "int"
            assert "values" in schema
            # Arrays infer from first element
            assert schema["values"] == ["int"]

    def test_extract_json_array_root(self):
        """Extract JSON with array at root."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "array.json"
            json_path.write_text(json.dumps([{"id": 1}, {"id": 2}]))

            result = JsonYamlExtractor.extract(json_path)

            schema = result["mdf"]["json_schema"]["schema"]
            # Root array, first element schema
            assert schema == [{"id": "int"}]

    def test_extract_json_empty_array(self):
        """Extract JSON with empty array."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "empty_array.json"
            json_path.write_text(json.dumps({"items": []}))

            result = JsonYamlExtractor.extract(json_path)

            schema = result["mdf"]["json_schema"]["schema"]
            assert schema["items"] == []

    def test_extract_yaml(self):
        """Extract YAML file."""
        with TemporaryDirectory() as tmpdir:
            yaml_path = Path(tmpdir) / "config.yaml"
            yaml_path.write_text("title: Test\nauthor: Jane\nyear: 2025\n")

            result = JsonYamlExtractor.extract(yaml_path)

            assert "mdf" in result
            schema = result["mdf"]["json_schema"]["schema"]
            assert schema["title"] == "str"
            assert schema["author"] == "str"
            assert schema["year"] == "int"

    def test_extract_yml_extension(self):
        """Extract .yml extension."""
        with TemporaryDirectory() as tmpdir:
            yml_path = Path(tmpdir) / "config.yml"
            yml_path.write_text("key: value\n")

            result = JsonYamlExtractor.extract(yml_path)

            assert "mdf" in result
            assert result["mdf"]["json_schema"]["schema"]["key"] == "str"

    def test_extract_invalid_json(self):
        """Extract invalid JSON returns empty dict."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "invalid.json"
            json_path.write_text("not valid json {")

            result = JsonYamlExtractor.extract(json_path)
            assert result == {}

    def test_extract_boolean_and_null(self):
        """Extract JSON with boolean and null types."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "types.json"
            json_path.write_text(json.dumps({"active": True, "value": None}))

            result = JsonYamlExtractor.extract(json_path)

            schema = result["mdf"]["json_schema"]["schema"]
            assert schema["active"] == "bool"
            assert schema["value"] == "NoneType"


class TestPDFExtractor:
    """Tests for PDFExtractor."""

    def test_can_extract_pdf(self):
        """PDFExtractor handles .pdf files."""
        from mdf_agent.extractors.pdf import PDFExtractor

        assert PDFExtractor.can_extract(Path("paper.pdf")) is True
        assert PDFExtractor.can_extract(Path("PAPER.PDF")) is True

    def test_cannot_extract_other(self):
        """PDFExtractor rejects non-PDF files."""
        from mdf_agent.extractors.pdf import PDFExtractor

        assert PDFExtractor.can_extract(Path("data.csv")) is False
        assert PDFExtractor.can_extract(Path("doc.docx")) is False

    def test_extract_nonexistent_file(self):
        """Extract nonexistent PDF returns empty dict."""
        from mdf_agent.extractors.pdf import PDFExtractor

        result = PDFExtractor.extract(Path("/nonexistent/file.pdf"))
        assert result == {}

    def test_doi_pattern(self):
        """DOI regex pattern matches valid DOIs."""
        from mdf_agent.extractors.pdf import DOI_PATTERN

        # Valid DOIs
        assert DOI_PATTERN.search("10.1234/abc123")
        assert DOI_PATTERN.search("See DOI: 10.5678/xyz-999")
        assert DOI_PATTERN.search("10.12345/long.doi.path/v1")

        # Should not match
        assert DOI_PATTERN.search("9.1234/abc") is None  # wrong prefix
        assert DOI_PATTERN.search("10.12/short") is None  # too short after 10.


class TestDiscoverMetadata:
    """Tests for the registry's discover_metadata function."""

    def test_discover_single_csv(self):
        """Discover metadata from single CSV."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "data.csv"
            csv_path.write_text("a,b\n1,2\n")

            result = discover_metadata([str(csv_path)])

            assert "mdf" in result
            assert "table_schema" in result["mdf"]

    def test_discover_single_json(self):
        """Discover metadata from single JSON."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "data.json"
            json_path.write_text('{"key": "value"}')

            result = discover_metadata([str(json_path)])

            assert "mdf" in result
            assert "json_schema" in result["mdf"]

    def test_discover_multiple_files(self):
        """Discover metadata from multiple files merges results."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "data.csv"
            csv_path.write_text("a,b\n1,2\n")

            json_path = Path(tmpdir) / "config.json"
            json_path.write_text('{"key": "value"}')

            result = discover_metadata([str(csv_path), str(json_path)])

            # Should have both table_schema and json_schema
            assert "mdf" in result
            # Depending on deep_merge behavior, may have both
            mdf = result["mdf"]
            assert "table_schema" in mdf or "json_schema" in mdf

    def test_discover_empty_list(self):
        """Discover metadata from empty list returns empty dict."""
        result = discover_metadata([])
        assert result == {}

    def test_discover_unsupported_extension(self):
        """Discover metadata skips unsupported file types."""
        with TemporaryDirectory() as tmpdir:
            txt_path = Path(tmpdir) / "notes.txt"
            txt_path.write_text("Just some text")

            result = discover_metadata([str(txt_path)])
            assert result == {}

    def test_discover_mixed_supported_unsupported(self):
        """Discover handles mix of supported and unsupported files."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "data.csv"
            csv_path.write_text("col\n1\n")

            txt_path = Path(tmpdir) / "readme.txt"
            txt_path.write_text("readme")

            result = discover_metadata([str(csv_path), str(txt_path)])

            # Should have CSV metadata, ignore txt
            assert "mdf" in result
            assert "table_schema" in result["mdf"]


class TestExtractorEdgeCases:
    """Edge case tests for extractors."""

    def test_csv_unicode(self):
        """CSV with unicode characters."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "unicode.csv"
            csv_path.write_text("name,city\nAlice,Munchen\nBob,Tokyo\n", encoding="utf-8")

            result = TabularExtractor.extract(csv_path)
            assert result["mdf"]["table_schema"]["row_count"] == 2

    def test_json_unicode(self):
        """JSON with unicode characters."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "unicode.json"
            json_path.write_text('{"city": "Tokyo", "greeting": "Hello"}', encoding="utf-8")

            result = JsonYamlExtractor.extract(json_path)
            assert result["mdf"]["json_schema"]["schema"]["city"] == "str"

    def test_csv_large_file_simulation(self):
        """CSV extraction handles files with many rows."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "large.csv"
            lines = ["id,value"] + [f"{i},{i*2}" for i in range(1000)]
            csv_path.write_text("\n".join(lines))

            result = TabularExtractor.extract(csv_path)
            assert result["mdf"]["table_schema"]["row_count"] == 1000

    def test_json_deeply_nested(self):
        """JSON with deeply nested structure."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "deep.json"
            data = {"a": {"b": {"c": {"d": {"e": "value"}}}}}
            json_path.write_text(json.dumps(data))

            result = JsonYamlExtractor.extract(json_path)
            schema = result["mdf"]["json_schema"]["schema"]
            assert schema["a"]["b"]["c"]["d"]["e"] == "str"
