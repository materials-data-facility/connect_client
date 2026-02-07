"""Tests for data source URL normalization."""

import pytest

from mdf_agent.core.submission import normalize_data_source, resolve_data_sources
from mdf_agent.auth.globus import NCSA_MDF_COLLECTION_UUID


class TestNormalizeDataSource:
    """Tests for normalize_data_source()."""

    def test_globus_file_manager_url(self):
        """Globus File Manager URL is converted to globus:// URI."""
        url = (
            "https://app.globus.org/file-manager"
            "?origin_id=82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
            "&origin_path=%2Ftmp%2Fstaging%2F"
        )
        result = normalize_data_source(url)
        assert result == "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/tmp/staging/"

    def test_globus_file_manager_with_file(self):
        """Globus File Manager URL with file path."""
        url = (
            "https://app.globus.org/file-manager"
            "?origin_id=aaaa1111-2222-3333-4444-555566667777"
            "&origin_path=%2Fdata%2Ftest.csv"
        )
        result = normalize_data_source(url)
        assert result == "globus://aaaa1111-2222-3333-4444-555566667777/data/test.csv"

    def test_mdf_data_domain(self):
        """data.materialsdatafacility.org URL maps to NCSA collection."""
        url = "https://data.materialsdatafacility.org/tmp/staging/file.csv"
        result = normalize_data_source(url)
        assert result == f"globus://{NCSA_MDF_COLLECTION_UUID}/tmp/staging/file.csv"

    def test_mdf_data_domain_root_path(self):
        """data.materialsdatafacility.org with just a path."""
        url = "https://data.materialsdatafacility.org/my-dataset/"
        result = normalize_data_source(url)
        assert result == f"globus://{NCSA_MDF_COLLECTION_UUID}/my-dataset/"

    def test_globus_uri_passthrough(self):
        """globus:// URIs pass through unchanged."""
        url = "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/path/to/data"
        assert normalize_data_source(url) == url

    def test_stream_uri_passthrough(self):
        """stream:// URIs pass through unchanged."""
        url = "stream://my-stream-id"
        assert normalize_data_source(url) == url

    def test_external_https_passthrough(self):
        """External HTTPS URLs pass through unchanged."""
        url = "https://zenodo.org/record/12345/files/data.zip"
        assert normalize_data_source(url) == url

    def test_http_passthrough(self):
        """HTTP URLs pass through unchanged."""
        url = "http://example.com/data.csv"
        assert normalize_data_source(url) == url

    def test_globus_file_manager_missing_origin_id(self):
        """Globus File Manager URL without origin_id passes through."""
        url = "https://app.globus.org/file-manager?origin_path=%2Ftmp%2F"
        assert normalize_data_source(url) == url

    def test_globus_file_manager_encoded_path(self):
        """URL-encoded path components are decoded."""
        url = (
            "https://app.globus.org/file-manager"
            "?origin_id=82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
            "&origin_path=%2Fpath%20with%20spaces%2Ffile.csv"
        )
        result = normalize_data_source(url)
        assert result == "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/path with spaces/file.csv"


class TestResolveDataSourcesNormalization:
    """Tests that resolve_data_sources() applies normalization."""

    def test_normalizes_https_urls(self, tmp_path):
        """HTTPS URLs go through normalize_data_source."""
        sources = ["https://data.materialsdatafacility.org/test/data.csv"]
        result = resolve_data_sources(sources, tmp_path)
        assert result == [f"globus://{NCSA_MDF_COLLECTION_UUID}/test/data.csv"]

    def test_stream_urls_pass_through(self, tmp_path):
        """stream:// URLs pass through normalization unchanged."""
        sources = ["stream://my-stream"]
        result = resolve_data_sources(sources, tmp_path)
        assert result == ["stream://my-stream"]

    def test_globus_urls_pass_through(self, tmp_path):
        """globus:// URLs pass through normalization unchanged."""
        sources = ["globus://uuid-here/path/data"]
        result = resolve_data_sources(sources, tmp_path)
        assert result == ["globus://uuid-here/path/data"]

    def test_local_paths_resolved(self, tmp_path):
        """Local paths are resolved relative to root."""
        (tmp_path / "data.csv").touch()
        result = resolve_data_sources(["data.csv"], tmp_path)
        assert result == [str(tmp_path / "data.csv")]
