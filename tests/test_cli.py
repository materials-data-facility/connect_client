"""Tests for MDF Agent CLI commands."""

from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os

import pytest

from typer.testing import CliRunner

from mdf_agent.cli.main import app

runner = CliRunner()


class TestManifestInitCommand:
    """Tests for 'mdf manifest init' command."""

    def test_manifest_init_creates_yaml(self):
        """mdf manifest init creates mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                ["manifest", "init", tmpdir, "--title", "Test Dataset", "--author", "Jane Doe"],
            )
            assert result.exit_code == 0
            assert "Created mdf.yaml" in result.stdout
            assert (Path(tmpdir) / "mdf.yaml").is_file()
            # No .git directory created
            assert not (Path(tmpdir) / ".git").is_dir()

    def test_manifest_init_with_multiple_authors(self):
        """mdf manifest init with multiple --author options."""
        with TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                [
                    "manifest", "init", tmpdir,
                    "--title", "Test Dataset",
                    "--author", "Jane Doe",
                    "--author", "John Smith",
                ],
            )
            assert result.exit_code == 0

            import yaml
            manifest = yaml.safe_load((Path(tmpdir) / "mdf.yaml").read_text())
            assert len(manifest["authors"]) == 2

    def test_manifest_init_with_all_options(self):
        """mdf manifest init with all options."""
        with TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                [
                    "manifest", "init", tmpdir,
                    "--title", "Test Dataset",
                    "--author", "Jane Doe",
                    "--description", "A test dataset",
                    "--publisher", "Test Publisher",
                    "--year", "2025",
                ],
            )
            assert result.exit_code == 0

            import yaml
            manifest = yaml.safe_load((Path(tmpdir) / "mdf.yaml").read_text())
            assert manifest["title"] == "Test Dataset"
            assert manifest["description"] == "A test dataset"
            assert manifest["publisher"] == "Test Publisher"
            assert manifest["publication_year"] == 2025

    def test_manifest_init_missing_title_fails(self):
        """mdf manifest init without --title fails."""
        with TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                ["manifest", "init", tmpdir, "--author", "Jane Doe"],
            )
            assert result.exit_code != 0

    def test_manifest_init_missing_author_fails(self):
        """mdf manifest init without --author fails."""
        with TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                app,
                ["manifest", "init", tmpdir, "--title", "Test"],
            )
            assert result.exit_code != 0

    def test_manifest_init_no_overwrite(self):
        """mdf manifest init refuses to overwrite existing mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "mdf.yaml").write_text("title: Existing\n")
            result = runner.invoke(
                app,
                ["manifest", "init", tmpdir, "--title", "New", "--author", "Jane"],
            )
            assert result.exit_code != 0
            assert "already exists" in result.stdout


class TestValidateCommand:
    """Tests for 'mdf validate' command."""

    def test_validate_valid_manifest(self):
        """mdf validate passes for valid manifest."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            # Create manifest with data source
            runner.invoke(
                app,
                ["manifest", "init", str(root), "--title", "Test", "--author", "Jane"],
            )

            import yaml
            manifest_path = root / "mdf.yaml"
            manifest = yaml.safe_load(manifest_path.read_text())
            manifest["data_sources"] = ["./data.csv"]
            manifest_path.write_text(yaml.safe_dump(manifest))

            original_cwd = os.getcwd()
            try:
                os.chdir(root)
                result = runner.invoke(app, ["validate"])
                assert result.exit_code == 0
                assert "Validation passed" in result.stdout
            finally:
                os.chdir(original_cwd)


class TestPublishCommand:
    """Tests for 'mdf publish' command."""

    def test_publish_direct_dry_run(self):
        """mdf publish with data args shows dry run payload."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            result = runner.invoke(
                app,
                ["publish", str(root / "data.csv"), "--title", "Test", "--author", "Jane"],
            )
            assert result.exit_code == 0
            assert "Dry run" in result.stdout

    def test_publish_manifest_mode_dry_run(self):
        """mdf publish reads mdf.yaml when no data args provided."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.csv").write_text("a,b,c")

            runner.invoke(
                app,
                ["manifest", "init", str(root), "--title", "Test", "--author", "Jane"],
            )

            import yaml
            manifest_path = root / "mdf.yaml"
            manifest = yaml.safe_load(manifest_path.read_text())
            manifest["data_sources"] = ["./data.csv"]
            manifest_path.write_text(yaml.safe_dump(manifest))

            original_cwd = os.getcwd()
            try:
                os.chdir(root)
                result = runner.invoke(app, ["publish"])
                assert result.exit_code == 0
                assert "Dry run" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_publish_no_data_no_manifest_fails(self):
        """mdf publish fails when no data paths and no mdf.yaml."""
        with TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                result = runner.invoke(app, ["publish"])
                assert result.exit_code != 0
                assert "No data paths" in result.stdout or "no mdf.yaml" in result.stdout
            finally:
                os.chdir(original_cwd)


class TestCloneCommand:
    """Tests for 'mdf clone' command."""

    def test_clone_help(self):
        """mdf clone --help shows download options."""
        result = runner.invoke(app, ["clone", "--help"])
        assert result.exit_code == 0
        assert "--transfer" in result.stdout
        assert "--derive" in result.stdout


class TestCLIHelp:
    """Tests for CLI help output."""

    def test_main_help(self):
        """mdf --help shows usage."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "MDF Agent CLI" in result.stdout

    def test_manifest_help(self):
        """mdf manifest --help shows subcommands."""
        result = runner.invoke(app, ["manifest", "--help"])
        assert result.exit_code == 0
        assert "init" in result.stdout
        assert "discover" in result.stdout

    def test_publish_help(self):
        """mdf publish --help shows options."""
        result = runner.invoke(app, ["publish", "--help"])
        assert result.exit_code == 0
        assert "--test" in result.stdout or "--dry-run" in result.stdout

    def test_validate_help(self):
        """mdf validate --help shows usage."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0

    def test_clone_help(self):
        """mdf clone --help shows options."""
        result = runner.invoke(app, ["clone", "--help"])
        assert result.exit_code == 0

    def test_no_init_command(self):
        """mdf init is no longer a command."""
        result = runner.invoke(app, ["init", "--help"])
        # Should fail or show error — init is gone
        assert result.exit_code != 0

    def test_no_add_command(self):
        """mdf add is no longer a command."""
        result = runner.invoke(app, ["add", "--help"])
        assert result.exit_code != 0

    def test_no_commit_command(self):
        """mdf commit is no longer a command."""
        result = runner.invoke(app, ["commit", "--help"])
        assert result.exit_code != 0


class TestAuthCommands:
    """Tests for login/logout/whoami commands."""

    def test_login_invokes_auth(self, monkeypatch):
        called = {"ok": False, "service": None, "token": None}

        def fake_get_authorizer(*, token=None, service_instance="prod", **kwargs):
            called["ok"] = True
            called["service"] = service_instance
            called["token"] = token
            return object()

        monkeypatch.setattr("mdf_agent.auth.globus.get_authorizer", fake_get_authorizer)

        result = runner.invoke(app, ["login", "--service", "dev", "--token", "abc123"])
        assert result.exit_code == 0
        assert "Authentication ready" in result.stdout
        assert called["ok"] is True
        assert called["service"] == "dev"
        assert called["token"] == "abc123"

    def test_logout_reports_success(self, monkeypatch):
        monkeypatch.setattr("mdf_agent.auth.globus.logout", lambda: True)
        result = runner.invoke(app, ["logout"])
        assert result.exit_code == 0
        assert "Logged out" in result.stdout

    def test_whoami_uses_env_token_status(self, monkeypatch):
        monkeypatch.setattr("mdf_agent.auth.globus.is_logged_in", lambda service_instance="prod": False)
        monkeypatch.setenv("MDF_CONNECT_TOKEN", "env-token")
        result = runner.invoke(app, ["whoami", "--service", "prod"])
        assert result.exit_code == 0
        assert "authenticated" in result.stdout
        assert "MDF_CONNECT_TOKEN is set in environment" in result.stdout
