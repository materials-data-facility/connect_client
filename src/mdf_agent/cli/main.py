"""MDF Agent CLI - Main entry point.

This module provides the command-line interface for MDF Agent.
Commands are organized as direct subcommands of the main `mdf` command.

Usage:
    mdf init --title "My Dataset" --author "Jane Doe"
    mdf publish --submit
    mdf publish ./data/ --title "Test" --author "Jane" --submit
"""

from __future__ import annotations

import json
import os
import sys
import time
import webbrowser
from pathlib import Path
from typing import List, Optional

import typer
from rich.table import Table
from rich.panel import Panel

from mdf_agent.version import __version__
from mdf_agent.core.agent import MDFAgent
from mdf_agent.core.backend_client import BackendClient, _api_url_for_service
from mdf_agent.core.config import GlobalConfig, resolve_service
from mdf_agent.cli.formatting import (
    api_spinner,
    console,
    format_status_badge,
    json_or_rich,
    require_success,
)
from mdf_agent.cli.backend import app as backend_app
from mdf_agent.cli.stream import app as stream_app
from mdf_agent.cli.config_cmd import app as config_app
from mdf_agent.cli.manifest_cmd import app as manifest_app


app = typer.Typer(
    help="MDF Agent CLI - Materials Data Facility dataset management",
    add_completion=True,
    invoke_without_command=True,
)

app.add_typer(backend_app, name="backend", hidden=True)
app.add_typer(stream_app, name="stream")
app.add_typer(config_app, name="config")
app.add_typer(manifest_app, name="manifest")


# ---------------------------------------------------------------------------
# Main callback — first-run welcome or help
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main_callback(ctx: typer.Context):
    """MDF Agent CLI - Materials Data Facility dataset management."""
    if ctx.invoked_subcommand is not None:
        return

    # First-run: no config file yet
    cfg_path = Path.home() / ".config" / "mdf_agent" / "config.json"
    if not cfg_path.exists():
        console.print(Panel(
            f"[bold]MDF Agent[/bold] v{__version__}\n"
            "\n"
            "Get started:\n"
            "  [cyan]mdf init[/cyan]                  Create a dataset manifest\n"
            "  [cyan]mdf publish --submit[/cyan]      Publish to MDF Connect\n"
            '  [cyan]mdf search "perovskite"[/cyan]   Find datasets\n'
            "  [cyan]mdf login[/cyan]                 Authenticate with Globus\n"
            "\n"
            "Run [bold]mdf --help[/bold] for all commands.",
            border_style="blue",
        ))
    else:
        # Show help
        console.print(ctx.get_help())


# ---------------------------------------------------------------------------
# mdf init — top-level shortcut for manifest init
# ---------------------------------------------------------------------------

@app.command()
def init(
    path: str = typer.Argument(".", help="Directory to create mdf.yaml in"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Dataset title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    publisher: Optional[str] = typer.Option(None, "--publisher", help="Dataset publisher"),
    publication_year: Optional[int] = typer.Option(None, "--year", "-y", help="Publication year"),
):
    """Create an mdf.yaml manifest for a dataset directory.

    Examples:
        mdf init --title "My Dataset" --author "Jane Doe"
        mdf init ./my_data --title "Test" --author "Jane"
    """
    from mdf_agent.cli.manifest_cmd import manifest_init
    manifest_init(
        path=path,
        title=title,
        author=author,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )


# ---------------------------------------------------------------------------
# Auth commands
# ---------------------------------------------------------------------------

@app.command()
def login(
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev)"),
    token: Optional[str] = typer.Option(None, "--token", help="Use an explicit access token"),
):
    """Authenticate with Globus for MDF Connect."""
    from mdf_agent.auth.globus import (
        DEFAULT_TOKEN_PATH,
        DATA_MDF_SCOPE,
        TRANSFER_SCOPE,
        get_authorizer_for_scopes,
        get_scopes_for_service,
    )

    resolved = resolve_service(service)

    if token:
        from mdf_agent.auth.globus import get_authorizer
        get_authorizer(token=token, service_instance=resolved)
    else:
        scope, _rs = get_scopes_for_service(resolved)
        get_authorizer_for_scopes([scope, DATA_MDF_SCOPE, TRANSFER_SCOPE])
    console.print("[green]Authentication ready[/green]")
    console.print(f"[dim]Token store:[/dim] {DEFAULT_TOKEN_PATH}")
    if token:
        console.print("[dim]Using token from --token for this invocation.[/dim]")


@app.command()
def logout():
    """Clear cached Globus credentials."""
    from mdf_agent.auth.globus import DEFAULT_TOKEN_PATH, logout as clear_cached_tokens

    removed = clear_cached_tokens()
    if removed:
        console.print("[green]Logged out[/green]")
    else:
        console.print(f"[yellow]No cached token file found[/yellow] ({DEFAULT_TOKEN_PATH})")


@app.command()
def whoami(
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev)"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
):
    """Show current authentication status."""
    from mdf_agent.auth.globus import DEFAULT_TOKEN_PATH, is_logged_in

    resolved = resolve_service(service)
    cached = is_logged_in(service_instance=resolved)
    env_token = bool(os.environ.get("MDF_CONNECT_TOKEN"))
    status_str = "authenticated" if (cached or env_token) else "not authenticated"

    result = {
        "success": True,
        "service": resolved,
        "status": status_str,
        "token_store": str(DEFAULT_TOKEN_PATH),
        "env_token_set": env_token,
    }

    if json_output:
        print(json.dumps(result, indent=2))
        return

    console.print(f"[bold]Service:[/bold] {resolved}")
    console.print(f"[bold]Status:[/bold] {status_str}")
    console.print(f"[bold]Token store:[/bold] {DEFAULT_TOKEN_PATH}")
    if env_token:
        console.print("[dim]MDF_CONNECT_TOKEN is set in environment[/dim]")


# ---------------------------------------------------------------------------
# Status / show / versions
# ---------------------------------------------------------------------------

@app.command()
def status(
    source_id: Optional[str] = typer.Argument(None, help="Source ID to check (default: last published)"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Show dataset status from backend.

    With source_id: shows backend status for that dataset.
    Without: shows backend status of last published dataset.
    """
    resolved = resolve_service(service)

    lookup_id = source_id
    if not lookup_id:
        cfg = GlobalConfig()
        lookup_id = cfg.last_source_id
        if not lookup_id:
            console.print("\n[dim]No source_id provided and no last published dataset.[/dim]")
            console.print("[dim]Usage: mdf status <source_id>[/dim]")
            return

    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Checking status..."):
        result = client.status(lookup_id, version=version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        return

    sub = result.get("submission") or result
    sid = sub.get("source_id")
    if sid:
        ver = sub.get("version", "?")
        st = sub.get("status", "unknown")
        console.print(f"\n[bold]Backend status:[/bold] [cyan]{sid}[/cyan] v{ver}")
        console.print(f"  [dim]Status:[/dim] {format_status_badge(st)}")
        # Parse dataset_mdata for extra fields
        mdata = sub.get("dataset_mdata", {})
        if isinstance(mdata, str):
            try:
                mdata = json.loads(mdata)
            except Exception:
                mdata = {}
        if isinstance(mdata, dict):
            if mdata.get("title"):
                console.print(f"  [dim]Title:[/dim] {mdata['title']}")
        if sub.get("dataset_doi") or sub.get("doi"):
            doi = sub.get("dataset_doi") or sub.get("doi")
            console.print(f"  [dim]DOI:[/dim] https://doi.org/{doi}")

        # Transfer progress
        transfer = result.get("transfer", {})
        if transfer and transfer.get("status"):
            t_status = transfer["status"]
            t_bytes = transfer.get("bytes_transferred", 0)
            t_files = transfer.get("files_transferred", 0)
            if t_status == "active":
                console.print(f"  [dim]Transfer:[/dim] [cyan]active[/cyan] — {t_files} files, {t_bytes:,} bytes transferred")
            elif t_status == "succeeded":
                console.print(f"  [dim]Transfer:[/dim] [green]complete[/green] — {t_files} files, {t_bytes:,} bytes")
            elif t_status == "failed":
                console.print(f"  [dim]Transfer:[/dim] [red]failed[/red]")

        # Version chain context
        if isinstance(mdata, dict):
            is_latest = mdata.get("latest", True)
            root_version = mdata.get("root_version", "")
            prev_version = mdata.get("previous_version", "")
            if prev_version:
                console.print(f"  [dim]Previous version:[/dim] {prev_version}")
            if root_version and root_version != f"{sid}-{ver}":
                console.print(f"  [dim]Root version:[/dim] {root_version}")
            if not is_latest:
                console.print("  [yellow]This is not the latest version[/yellow]")

        # Next action hint
        if st == "pending_curation":
            console.print("  [dim]Next:[/dim] Waiting for curation review")
        elif st == "approved":
            console.print("  [dim]Next:[/dim] Publishing in progress...")
        elif st == "published":
            console.print("  [dim]Next:[/dim] Dataset is live!")
        elif st == "rejected":
            console.print("  [dim]Next:[/dim] Review feedback and resubmit with --update")

    elif result.get("error"):
        console.print(f"\n[yellow]Backend:[/yellow] {result.get('error')}")
    else:
        console.print(f"\n[dim]No backend record for {lookup_id}[/dim]")


@app.command()
def validate(
    data: Optional[List[str]] = typer.Argument(None, help="Data paths to validate (optional)"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
):
    """Validate manifest before publishing.

    Inside a directory with mdf.yaml: validates the manifest.
    With data args: validates the provided paths.
    Returns exit code 1 if validation fails.
    """
    manifest_path = Path.cwd() / "mdf.yaml"
    if manifest_path.exists():
        agent = MDFAgent.from_manifest(".")
    elif data:
        from mdf_agent.models.config import ManifestConfig
        agent = MDFAgent(manifest=ManifestConfig(data_sources=list(data)))
        console.print("[yellow]No mdf.yaml found — validating data paths only[/yellow]")
    else:
        console.print("[red]No mdf.yaml found and no data paths provided[/red]")
        console.print("[dim]Run [/dim][cyan]mdf manifest init[/cyan][dim] to create one, or pass data paths.[/dim]")
        raise typer.Exit(code=1)

    results = agent.validate()
    errors = results.get("errors", [])
    warnings = results.get("warnings", [])

    if json_output:
        print(json.dumps({"success": not errors, "errors": errors, "warnings": warnings}, indent=2))
        if errors:
            raise typer.Exit(code=1)
        return

    if errors:
        console.print("\n[bold red]Errors:[/bold red]")
        for error in errors:
            console.print(f"  [red]x[/red] {error}")

    if warnings:
        console.print("\n[bold yellow]Warnings:[/bold yellow]")
        for warning in warnings:
            console.print(f"  [yellow]![/yellow] {warning}")

    if errors:
        console.print("\n[red]Validation failed[/red]")
        raise typer.Exit(code=1)

    console.print("\n[bold green]Validation passed[/bold green]")


# ---------------------------------------------------------------------------
# Publish / update
# ---------------------------------------------------------------------------

@app.command()
def publish(
    data: Optional[List[str]] = typer.Argument(None, help="Data paths/URIs to publish"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Dataset title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit", help="Preview without submitting"),
    test: bool = typer.Option(False, "--test", help="Submit to test environment"),
    update: bool = typer.Option(False, "--update", "-u", help="Update existing dataset"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL for local backend"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """Publish dataset to MDF Connect.

    Three modes:

    1. Direct (data args provided):
        mdf publish ./data/ --title "My Dataset" --author "Jane" --submit

    2. Manifest (mdf.yaml in current directory):
        mdf publish --submit

    3. Manifest + overrides:
        mdf publish --title "New Title" --submit

    By default, performs a dry run showing the payload.
    Use --submit to actually send to MDF Connect.
    """
    from rich.syntax import Syntax
    from mdf_agent.models.config import ManifestConfig

    resolved = resolve_service(service)

    manifest_path = Path.cwd() / "mdf.yaml"

    if data:
        if not title:
            console.print("[red]Direct mode requires --title[/red]")
            console.print("[dim]Example: mdf publish ./data/ --title \"My Dataset\" --author \"Jane\"[/dim]")
            raise typer.Exit(code=1)
        if not author:
            console.print("[red]Direct mode requires --author[/red]")
            console.print("[dim]Example: mdf publish ./data/ --title \"My Dataset\" --author \"Jane\"[/dim]")
            raise typer.Exit(code=1)

        cfg = GlobalConfig()
        manifest = ManifestConfig(
            title=title,
            authors=author,
            description=description,
            data_sources=list(data),
            publisher=cfg.publisher,
            organization=cfg.organization,
        )
        agent = MDFAgent(root=None, manifest=manifest)

    elif manifest_path.exists():
        agent = MDFAgent.from_manifest(".")
        if title:
            agent.manifest.title = title
        if author:
            agent.manifest.authors = author
        if description:
            agent.manifest.description = description
        if not agent.manifest.data_sources:
            agent.manifest.data_sources = ["."]

    else:
        console.print("\n[red]No data paths provided and no mdf.yaml found[/red]")
        console.print("[dim]Direct mode:[/dim]    mdf publish ./data/ --title \"My Dataset\" --author \"Jane\" --submit")
        console.print("[dim]Manifest mode:[/dim]  mdf manifest init --title \"My Dataset\" --author \"Jane\"")
        console.print("[dim]                 mdf publish --submit[/dim]")
        raise typer.Exit(code=1)

    payload = agent.build_submission(test=test, update=update)

    if dry_run:
        if json_output:
            print(json.dumps({"success": True, "dry_run": True, "payload": payload}, indent=2))
            return
        console.print("\n[bold cyan]Dry run - would submit:[/bold cyan]")
        target = api_url or _api_url_for_service(resolved)
        console.print(f"[dim]Target: {target} ({resolved})[/dim]")
        syntax = Syntax(json.dumps(payload, indent=2), "json", theme="monokai")
        console.print(syntax)
        return

    # Build a rich progress callback for file uploads
    from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn

    progress_callback = None
    progress_ctx = None
    file_tasks: dict = {}

    if sys.stderr.isatty():
        progress_ctx = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
            transient=True,
        )
        progress_ctx.start()

        def _progress_cb(filename: str, bytes_sent: int, total_bytes: int) -> None:
            if filename not in file_tasks:
                file_tasks[filename] = progress_ctx.add_task(filename, total=total_bytes)
            progress_ctx.update(file_tasks[filename], completed=bytes_sent)

        progress_callback = _progress_cb

    try:
        with api_spinner("Publishing..."):
            result = agent.publish(
                test=test,
                update=update,
                dry_run=False,
                token=token,
                service_instance=resolved,
                api_url=api_url,
                dev_user_id=dev_user,
                progress_callback=progress_callback,
            )
    finally:
        if progress_ctx is not None:
            progress_ctx.stop()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print("\n[bold green]Published successfully![/bold green]")
        source_id_val = result.get("source_id")
        version_val = result.get("version")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{source_id_val}[/cyan]")
        if version_val:
            console.print(f"  [dim]Version:[/dim] [cyan]{version_val}[/cyan]")

        if source_id_val:
            cfg = GlobalConfig()
            cfg.record_publish(source_id_val, version_val, resolved)
    else:
        require_success(result, error_prefix="Publish failed")


@app.command("list")
def list_datasets(
    limit: int = typer.Option(50, "--limit", "-n", help="Max results"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    """List your submitted datasets.

    Examples:
        mdf list
        mdf list --limit 50
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading datasets..."):
        result = client.submissions()
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        return

    submissions = result.get("submissions", [])
    if not submissions:
        console.print("\n[dim]No datasets found.[/dim]")
        return

    console.print(f"\n[bold]Your datasets ({len(submissions)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Source ID", no_wrap=True)
    table.add_column("Title", max_width=40)
    table.add_column("Version", style="dim", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Updated", style="dim", no_wrap=True)

    for sub in submissions[:limit]:
        mdata = sub.get("dataset_mdata", {})
        if isinstance(mdata, str):
            try:
                mdata = json.loads(mdata)
            except Exception:
                mdata = {}
        title_str = mdata.get("title") or sub.get("title") or "Untitled"
        status_str = format_status_badge(sub.get("status", ""))
        updated = (sub.get("updated_at") or sub.get("created_at") or "")[:19]
        table.add_row(
            sub.get("source_id", ""),
            title_str,
            sub.get("version", ""),
            status_str,
            updated,
        )
    console.print(table)


@app.command()
def show(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    cite: bool = typer.Option(False, "--cite", "-c", help="Include citation"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Show dataset detail view.

    Examples:
        mdf show my_dataset_v1
        mdf show my_dataset_v1 --cite
        mdf show my_dataset_v1 --json
    """
    from rich import box

    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading dataset..."):
        result = client.get_card(source_id, version=version)
        citation_result = None
        if cite and result.get("success"):
            citation_result = client.get_citation(source_id, format="apa", version=version)
    client.close()

    if json_output:
        output = {"card": result}
        if citation_result:
            output["citation"] = citation_result
        print(json.dumps(output, indent=2))
        return

    if not result.get("success"):
        require_success(result, error_prefix="Show failed")

    c = result.get("card", {})

    console.print()
    console.print(Panel(
        f"[bold]{c.get('title', 'Untitled')}[/bold]\n"
        f"[dim]{c.get('description', 'No description')}[/dim]",
        title=f"[cyan]{c.get('source_id')}[/cyan] v{c.get('version', '1.0')}",
        border_style="blue",
    ))

    table = Table(show_header=False, box=box.SIMPLE, padding=(0, 2))
    table.add_column("Field", style="dim", width=15)
    table.add_column("Value")

    if c.get("authors"):
        table.add_row("Authors", ", ".join(c["authors"]))
    if c.get("publisher"):
        table.add_row("Publisher", c["publisher"])
    if c.get("publication_year"):
        table.add_row("Year", str(c["publication_year"]))
    if c.get("organization"):
        table.add_row("Organization", c["organization"])
    if c.get("keywords"):
        table.add_row("Keywords", ", ".join(c["keywords"]))
    if c.get("doi"):
        table.add_row("DOI", f"https://doi.org/{c['doi']}")
    if c.get("license"):
        table.add_row("License", c["license"])
    table.add_row("Status", format_status_badge(c.get("status", "unknown")))

    ml = c.get("ml") or {}
    if ml:
        if ml.get("task_type"):
            table.add_row("ML Task", ml["task_type"])
        if ml.get("data_format"):
            table.add_row("ML Format", ml["data_format"])

    console.print(table)

    stats = c.get("stats", {})
    if stats:
        console.print(f"\n[dim]Files:[/dim] {stats.get('data_sources_count', 0)} sources | Types: {', '.join(stats.get('file_types', []))}")

    if citation_result and citation_result.get("success"):
        apa = citation_result.get("apa", "")
        if apa:
            console.print(f"\n[bold]Citation:[/bold]\n  {apa}")

    console.print()


@app.command()
def versions(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max versions to return"),
    offset: int = typer.Option(0, "--offset", help="Skip first N versions"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Show version history for a dataset.

    Examples:
        mdf versions my_dataset_v1
        mdf versions my_dataset_v1 --limit 10 --offset 5
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading versions..."):
        result = client.versions(source_id, limit=limit, offset=offset)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        return

    if not result.get("success"):
        console.print(f"[yellow]{result.get('error', 'No versions found')}[/yellow]")
        return

    version_list = result.get("versions", [])
    if not version_list:
        console.print("[dim]No versions found.[/dim]")
        return

    console.print(f"\n[bold]Versions for[/bold] [cyan]{source_id}[/cyan]\n")

    table = Table(show_header=True, header_style="bold")
    table.add_column("Version", no_wrap=True)
    table.add_column("Title", max_width=40)
    table.add_column("Status", no_wrap=True)
    table.add_column("DOI", style="dim")
    table.add_column("Updated", style="dim", no_wrap=True)

    for v in version_list:
        doi = v.get("doi") or ""
        if doi:
            doi = f"https://doi.org/{doi}"
        table.add_row(
            v.get("version", ""),
            v.get("title", ""),
            format_status_badge(v.get("status", "")),
            doi,
            (v.get("updated_at") or v.get("created_at") or "")[:19],
        )
    console.print(table)

    dataset_doi = result.get("dataset_doi")
    if dataset_doi:
        console.print(f"\n[dim]Dataset DOI:[/dim] https://doi.org/{dataset_doi}")
    console.print()


@app.command()
def clone(
    source_id: str = typer.Argument(..., help="Source dataset ID to download"),
    output_dir: str = typer.Argument(".", help="Output directory"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific version to clone"),
    transfer: bool = typer.Option(False, "--transfer", help="Use Globus Transfer (requires GCP)"),
    derive: bool = typer.Option(False, "--derive", help="Create mdf.yaml with derived-from lineage"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Download a dataset's files from MDF.

    Downloads using the fastest available method: zip archive (if available),
    HTTPS file-by-file, or Globus Transfer (with --transfer flag).

    Examples:
        mdf clone my_dataset_v1.1
        mdf clone my_dataset_v1.1 ./local_copy
        mdf clone my_dataset_v1.1 --transfer
        mdf clone my_dataset_v1.1 --derive
    """
    import threading
    from rich.live import Live
    from rich.console import Group
    from rich.progress import (
        BarColumn, DownloadColumn, MofNCompleteColumn,
        Progress, SpinnerColumn, TextColumn, TransferSpeedColumn,
    )

    resolved = resolve_service(service)
    method = "transfer" if transfer else "auto"

    agent = MDFAgent()

    resolved_token = token or os.environ.get("MDF_CONNECT_TOKEN")
    resolved_dev_user = dev_user or os.environ.get("MDF_DEV_USER_ID")

    console.print(f"\n[bold]Cloning[/bold] [cyan]{source_id}[/cyan]", end="")
    if version:
        console.print(f" [dim]v{version}[/dim]", end="")
    console.print()

    progress_callback = None
    on_files_resolved_cb = None
    on_file_done_cb = None
    live_ctx = None

    if sys.stderr.isatty():
        # Overall progress: file count
        overall_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            console=console,
        )
        # Per-file progress: byte-level with speed
        file_progress = Progress(
            SpinnerColumn("dots2"),
            TextColumn("[dim]{task.description}[/dim]"),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        )

        overall_task_id = None
        total_file_count = 0
        file_tasks: dict = {}
        file_lock = threading.Lock()

        def on_files_resolved_cb(count: int) -> None:
            nonlocal overall_task_id, total_file_count
            total_file_count = count
            overall_task_id = overall_progress.add_task(
                f"[cyan]{source_id}[/cyan]", total=count
            )

        def progress_callback(rel_path: str, bytes_sent: int, total_bytes: int) -> None:
            filename = Path(rel_path).name
            with file_lock:
                if rel_path not in file_tasks:
                    file_tasks[rel_path] = file_progress.add_task(
                        filename, total=max(total_bytes, 1)
                    )
                file_progress.update(file_tasks[rel_path], completed=bytes_sent)

        def on_file_done_cb(rel_path: str, _size: int) -> None:
            with file_lock:
                tid = file_tasks.pop(rel_path, None)
                if tid is not None:
                    file_progress.remove_task(tid)
            if overall_task_id is not None:
                overall_progress.advance(overall_task_id, 1)
                task = overall_progress.tasks[overall_task_id]
                if task.completed >= task.total:
                    overall_progress.update(
                        overall_task_id,
                        description=f"[bold green]{source_id}[/bold green]",
                    )

        live_ctx = Live(
            Group(overall_progress, file_progress),
            console=console,
            refresh_per_second=15,
        )
        live_ctx.start()

    try:
        result = agent.clone(
            source_id=source_id,
            output_dir=output_dir,
            version=version,
            method=method,
            progress_callback=progress_callback,
            on_files_resolved=on_files_resolved_cb,
            on_file_done=on_file_done_cb,
            api_url=api_url,
            token=resolved_token,
            service_instance=resolved,
            dev_user_id=resolved_dev_user,
        )
    except RuntimeError as exc:
        if live_ctx is not None:
            live_ctx.stop()
        console.print(f"\n[red]Error:[/red] {exc}")
        raise typer.Exit(code=1)
    finally:
        if live_ctx is not None:
            live_ctx.stop()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if not result.get("success"):
        require_success(result, error_prefix="Clone failed")

    console.print(f"\n[bold green]Clone complete![/bold green]")
    if result.get("title"):
        console.print(f"  [dim]Title:[/dim] {result['title']}")
    console.print(f"  [dim]Method:[/dim] {result['method']}")
    if result.get("files_count"):
        console.print(f"  [dim]Files:[/dim] {result['files_count']}")
    console.print(f"  [dim]Path:[/dim] {result['path']}")
    if result.get("task_id"):
        console.print(f"  [dim]Transfer task:[/dim] {result['task_id']}")
        console.print(f"  [dim]Monitor:[/dim] {result['monitor_url']}")

    if derive:
        from mdf_agent.models.config import DerivedFrom

        derive_root = Path(output_dir).resolve()
        derive_agent = MDFAgent.init_manifest(
            path=str(derive_root),
            title=f"Derived from {source_id}",
            authors=["Unknown"],
        )
        derive_agent.manifest.derived_from = [
            DerivedFrom(source_id=source_id, relationship="derived")
        ]
        derive_agent.save_manifest()
        console.print(f"\n  [green]Created mdf.yaml with derived-from lineage[/green]")
    console.print()


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    search_type: str = typer.Option("all", "--type", "-t", help="all, datasets, or streams"),
    limit: int = typer.Option(20, "--limit", "-n", help="Max results"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Search datasets and streams in MDF.

    Examples:
        mdf search "perovskite"
        mdf search "XRD" --type streams
        mdf search "iron oxide" --limit 5
    """
    resolved = resolve_service(service)
    resolved_token = token or os.environ.get("MDF_CONNECT_TOKEN")
    resolved_dev_user = dev_user or os.environ.get("MDF_DEV_USER_ID")
    if resolved_token or resolved_dev_user:
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=resolved,
            dev_user_id=dev_user,
        )
    else:
        client = BackendClient(base_url=api_url or _api_url_for_service(resolved))

    with api_spinner("Searching..."):
        result = client.search(query, search_type=search_type, limit=limit)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        return

    if result.get("results"):
        total = result.get("total", 0)
        console.print(f"\n[bold]Found {total} results for[/bold] [cyan]'{result.get('query')}'[/cyan]\n")

        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=3)
        table.add_column("Type", width=8)
        table.add_column("Title", max_width=40)
        table.add_column("ID", no_wrap=True)
        table.add_column("Status", style="dim", no_wrap=True)

        for i, item in enumerate(result["results"], 1):
            if item.get("type") == "dataset":
                table.add_row(
                    str(i),
                    "[blue]dataset[/blue]",
                    item.get("title", "Untitled"),
                    f"{item.get('source_id')} v{item.get('version')}",
                    item.get("status", ""),
                )
            else:
                table.add_row(
                    str(i),
                    "[green]stream[/green]",
                    item.get("title", "Untitled"),
                    item.get("stream_id", ""),
                    f"{item.get('file_count', 0)} files",
                )

        console.print(table)
    else:
        console.print(f"\n[dim]No results found for '{query}'[/dim]")


# ---------------------------------------------------------------------------
# Curation
# ---------------------------------------------------------------------------

@app.command()
def pending(
    limit: int = typer.Option(20, "--limit", "-n", help="Max results"),
    organization: Optional[str] = typer.Option(None, "--organization", "-o", help="Filter by organization"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """List datasets awaiting curation.

    Examples:
        mdf pending
        mdf pending --organization argonne
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading pending..."):
        result = client.curation_pending(limit=limit, organization=organization)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        return

    submissions = result.get("submissions", [])
    if not submissions:
        console.print("\n[dim]No datasets pending curation.[/dim]")
        return

    console.print(f"\n[bold]Pending curation ({len(submissions)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=3)
    table.add_column("Source ID", no_wrap=True)
    table.add_column("Title", max_width=40)
    table.add_column("Version", style="dim", no_wrap=True)
    table.add_column("Submitted", style="dim", no_wrap=True)

    for i, sub in enumerate(submissions, 1):
        table.add_row(
            str(i),
            sub.get("source_id", ""),
            sub.get("title") or "Untitled",
            sub.get("version", ""),
            (sub.get("submitted_at") or sub.get("created_at") or "")[:19],
        )
    console.print(table)


@app.command()
def approve(
    source_id: str = typer.Argument(..., help="Source ID of dataset to approve"),
    mint_doi: bool = typer.Option(True, "--mint-doi/--no-mint-doi", help="Mint a DOI"),
    notes: Optional[str] = typer.Option(None, "--notes", "-n", help="Curator notes"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Approve a dataset for publication.

    Examples:
        mdf approve my_dataset_v1
        mdf approve my_dataset_v1 --notes "LGTM" --no-mint-doi
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Approving..."):
        result = client.curation_approve(
            source_id=source_id,
            mint_doi=mint_doi,
            notes=notes,
            version=version,
        )
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print(f"\n[bold green]Approved:[/bold green] [cyan]{source_id}[/cyan]")
        if result.get("doi"):
            console.print(f"  [dim]DOI:[/dim] https://doi.org/{result.get('doi')}")
    else:
        require_success(result, error_prefix="Approve failed")


@app.command()
def reject(
    source_id: str = typer.Argument(..., help="Source ID of dataset to reject"),
    reason: str = typer.Option(..., "--reason", "-r", help="Rejection reason"),
    suggestions: Optional[str] = typer.Option(None, "--suggestions", help="Suggestions for improvement"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Reject a dataset and return to submitter.

    Examples:
        mdf reject my_dataset_v1 --reason "Missing methods section"
        mdf reject my_dataset_v1 --reason "Bad data" --suggestions "Re-run experiment"
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Rejecting..."):
        result = client.curation_reject(
            source_id=source_id,
            reason=reason,
            suggestions=suggestions,
            version=version,
        )
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print(f"\n[bold yellow]Rejected:[/bold yellow] [cyan]{source_id}[/cyan]")
        console.print(f"  [dim]Reason:[/dim] {reason}")
    else:
        require_success(result, error_prefix="Reject failed")


# ---------------------------------------------------------------------------
# Lifecycle: edit, withdraw, resubmit, diff, delete, stats
# ---------------------------------------------------------------------------

@app.command()
def edit(
    source_id: str = typer.Argument(..., help="Source ID of dataset to edit"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="New title"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="New description"),
    keywords: Optional[List[str]] = typer.Option(None, "--keyword", "-k", help="Keywords (repeatable)"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Target version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Edit metadata on a submission.

    For published datasets, creates a minor version bump automatically.

    Examples:
        mdf edit my_dataset_v1 --title "New Title" --submit
        mdf edit my_dataset_v1 --keyword "XRD" --keyword "diffraction"
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    fields = {}
    if title is not None:
        fields["title"] = title
    if description is not None:
        fields["description"] = description
    if keywords is not None:
        fields["keywords"] = keywords

    if not fields:
        console.print("[red]No fields provided to edit.[/red]")
        console.print("[dim]Use --title, --description, or --keyword flags.[/dim]")
        raise typer.Exit(code=1)

    with api_spinner("Editing metadata..."):
        result = client.edit_metadata(source_id, version=version, **fields)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print(f"\n[bold green]Metadata updated:[/bold green] [cyan]{source_id}[/cyan]")
        if result.get("updated_fields"):
            console.print(f"  [dim]Fields:[/dim] {', '.join(result['updated_fields'])}")
        if result.get("new_version"):
            console.print(f"  [dim]New version:[/dim] {result['new_version']}")
    else:
        require_success(result, error_prefix="Edit failed")


@app.command()
def withdraw(
    source_id: str = typer.Argument(..., help="Source ID of dataset to withdraw"),
    reason: Optional[str] = typer.Option(None, "--reason", "-r", help="Withdrawal reason"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Target version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Withdraw a pending_curation submission.

    Examples:
        mdf withdraw my_dataset_v1
        mdf withdraw my_dataset_v1 --reason "Duplicate submission"
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Withdrawing..."):
        result = client.withdraw(source_id, reason=reason or "", version=version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print(f"\n[bold yellow]Withdrawn:[/bold yellow] [cyan]{source_id}[/cyan]")
    else:
        require_success(result, error_prefix="Withdraw failed")


@app.command()
def resubmit(
    source_id: str = typer.Argument(..., help="Source ID of rejected dataset to resubmit"),
    notes: Optional[str] = typer.Option(None, "--notes", "-n", help="Notes for the curator"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Target version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Resubmit a rejected dataset back to the curation queue.

    Examples:
        mdf resubmit my_dataset_v1 --notes "Fixed title and added authors"
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Resubmitting..."):
        result = client.resubmit(source_id, notes=notes or "", version=version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print(f"\n[bold green]Resubmitted:[/bold green] [cyan]{source_id}[/cyan]")
        console.print(f"  [dim]Status:[/dim] {format_status_badge('pending_curation')}")
    else:
        require_success(result, error_prefix="Resubmit failed")


@app.command()
def diff(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    from_version: str = typer.Option(..., "--from", help="From version"),
    to_version: str = typer.Option(..., "--to", help="To version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Show metadata diff between two versions.

    Examples:
        mdf diff my_dataset_v1 --from 1.0 --to 2.0
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading diff..."):
        result = client.version_diff(source_id, from_version, to_version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if not result.get("success"):
        require_success(result, error_prefix="Diff failed")

    d = result.get("diff", {})
    fv = result.get("from_version", {})
    tv = result.get("to_version", {})

    console.print(f"\n[bold]Diff[/bold] [cyan]{source_id}[/cyan]  "
                  f"v{fv.get('version', from_version)} -> v{tv.get('version', to_version)}\n")

    table = Table(show_header=True, header_style="bold")
    table.add_column("Change", no_wrap=True, width=8)
    table.add_column("Field", no_wrap=True)
    table.add_column("Value")

    for field, value in d.get("added", {}).items():
        table.add_row("[green]+[/green]", field, str(value)[:80])
    for field, value in d.get("removed", {}).items():
        table.add_row("[red]-[/red]", field, str(value)[:80])
    for field, change in d.get("changed", {}).items():
        table.add_row("[yellow]~[/yellow]", field,
                      f"{str(change.get('from', ''))[:35]} -> {str(change.get('to', ''))[:35]}")

    console.print(table)

    unchanged = d.get("unchanged", [])
    if unchanged:
        console.print(f"\n[dim]Unchanged: {', '.join(unchanged)}[/dim]")
    console.print()


@app.command("delete")
def delete_dataset(
    source_id: str = typer.Argument(..., help="Source ID of dataset to delete"),
    reason: str = typer.Option(..., "--reason", "-r", help="Deletion reason (required)"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Target version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Soft-delete a submission (curator-only).

    Examples:
        mdf delete my_dataset_v1 --reason "spam"
        mdf delete my_dataset_v1 --reason "duplicate" --version 1.0
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Deleting..."):
        result = client.delete_submission(source_id, reason=reason, version=version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        console.print(f"\n[bold red]Deleted:[/bold red] [cyan]{source_id}[/cyan]")
        console.print(f"  [dim]Reason:[/dim] {reason}")
    else:
        require_success(result, error_prefix="Delete failed")


@app.command()
def stats(
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Show admin statistics (curator-only).

    Displays submission counts by status.

    Examples:
        mdf stats
        mdf stats --json
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading stats..."):
        result = client.admin_stats()
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if not result.get("success"):
        require_success(result, error_prefix="Stats failed")

    console.print(f"\n[bold]Admin Statistics[/bold]\n")
    console.print(f"  [dim]Total submissions:[/dim] {result.get('total', 0)}")

    by_status = result.get("by_status", {})
    if by_status:
        table = Table(show_header=True, header_style="bold")
        table.add_column("Status", no_wrap=True)
        table.add_column("Count", justify="right", no_wrap=True)
        for st, count in sorted(by_status.items()):
            table.add_row(format_status_badge(st), str(count))
        console.print(table)

    access = result.get("access_totals", {})
    if access:
        console.print(f"\n  [dim]Total views:[/dim] {access.get('view_count', 0)}")
        console.print(f"  [dim]Total downloads:[/dim] {access.get('download_count', 0)}")

    console.print()


@app.command()
def update(
    source_id: Optional[str] = typer.Argument(None, help="Source ID of dataset to update (default: last published)"),
    data: Optional[List[str]] = typer.Option(None, "--data", "-D", help="Data paths/URIs"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Updated title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Updated description"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit", help="Preview without submitting"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Update an existing published dataset.

    Creates a new version of an existing dataset. Defaults to the last published
    dataset from config if source_id is not given.

    Examples:
        mdf update --data ./new_data/ --submit
        mdf update my_dataset_v1 --title "Updated Title" --submit
    """
    from rich.syntax import Syntax
    from mdf_agent.models.config import ManifestConfig

    resolved = resolve_service(service)

    resolved_source_id = source_id
    if not resolved_source_id:
        cfg = GlobalConfig()
        resolved_source_id = cfg.last_source_id
        if not resolved_source_id:
            console.print("[red]No source_id provided and no last published dataset.[/red]")
            console.print("[dim]Usage: mdf update <source_id> --data ./new_data/ --submit[/dim]")
            raise typer.Exit(code=1)

    # Fetch existing metadata so user doesn't have to re-supply title/authors
    cfg = GlobalConfig()
    existing_title = resolved_source_id
    existing_authors: List[str] = []
    try:
        _base = api_url or _api_url_for_service(resolved)
        _client = BackendClient.authenticated(
            base_url=_base, token=token, service_instance=resolved, dev_user_id=dev_user,
        )
        try:
            with api_spinner("Fetching existing metadata..."):
                card = _client.get_card(resolved_source_id)
            if card.get("success") and card.get("card"):
                c = card["card"]
                existing_title = c.get("title", resolved_source_id)
                existing_authors = c.get("authors", [])
        finally:
            _client.close()
    except Exception:
        pass

    data_sources = list(data) if data else []
    manifest = ManifestConfig(
        title=title or existing_title,
        authors=author or existing_authors or [resolved_source_id],
        description=description,
        data_sources=data_sources,
        publisher=cfg.publisher,
        organization=cfg.organization,
    )
    manifest.custom = {"mdf_source_id": resolved_source_id}

    agent = MDFAgent(root=None, manifest=manifest)
    payload = agent.build_submission(test=False, update=True)

    if dry_run:
        if json_output:
            print(json.dumps({"success": True, "dry_run": True, "payload": payload}, indent=2))
            return
        console.print("\n[bold cyan]Dry run — would update:[/bold cyan]")
        target = api_url or _api_url_for_service(resolved)
        console.print(f"[dim]Target: {target} ({resolved})[/dim]")
        console.print(f"[dim]Source ID: {resolved_source_id}[/dim]")
        syntax = Syntax(json.dumps(payload, indent=2), "json", theme="monokai")
        console.print(syntax)
        return

    # Build progress callback for uploads
    from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn

    progress_callback = None
    progress_ctx = None
    file_tasks: dict = {}

    if sys.stderr.isatty():
        progress_ctx = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
            transient=True,
        )
        progress_ctx.start()

        def _progress_cb(filename: str, bytes_sent: int, total_bytes: int) -> None:
            if filename not in file_tasks:
                file_tasks[filename] = progress_ctx.add_task(filename, total=total_bytes)
            progress_ctx.update(file_tasks[filename], completed=bytes_sent)

        progress_callback = _progress_cb

    try:
        with api_spinner("Updating..."):
            result = agent.publish(
                test=False,
                update=True,
                dry_run=False,
                token=token,
                service_instance=resolved,
                api_url=api_url,
                dev_user_id=dev_user,
                progress_callback=progress_callback,
            )
    finally:
        if progress_ctx is not None:
            progress_ctx.stop()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        new_version = result.get("version")
        console.print(f"\n[bold green]Updated successfully![/bold green]")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{result.get('source_id')}[/cyan]")
        if new_version:
            console.print(f"  [dim]Version:[/dim] [cyan]{new_version}[/cyan]")

        if result.get("source_id"):
            cfg = GlobalConfig()
            cfg.record_publish(result.get("source_id"), new_version, resolved)
    else:
        require_success(result, error_prefix="Update failed")


# ---------------------------------------------------------------------------
# Top-level: cite, open, preview (promoted from backend)
# ---------------------------------------------------------------------------

@app.command()
def cite(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    format: str = typer.Option("apa", "--format", "-f", help="Citation format: bibtex, ris, apa, datacite"),
    copy: bool = typer.Option(False, "--copy", help="Copy citation to clipboard"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Get citation for a dataset.

    Examples:
        mdf cite my_dataset                       # APA format
        mdf cite my_dataset -f bibtex             # BibTeX for papers
        mdf cite my_dataset --copy                # Copy to clipboard
        mdf cite my_dataset --json                # Raw JSON
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Fetching citation..."):
        result = client.get_citation(source_id, format=format, version=version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if not result.get("success"):
        require_success(result, error_prefix="Citation failed")

    citation_text = result.get(format) or result.get("apa", "")

    if copy:
        _copy_to_clipboard(citation_text)

    console.print()
    if format == "bibtex":
        console.print(Panel(citation_text, title="BibTeX", border_style="green"))
    elif format == "ris":
        console.print(Panel(citation_text, title="RIS", border_style="green"))
    elif format == "datacite":
        console.print(Panel(citation_text, title="DataCite XML", border_style="green"))
    else:
        console.print(Panel(citation_text, title="APA Citation", border_style="green"))
    console.print()


def _copy_to_clipboard(text: str) -> None:
    """Try to copy text to clipboard. Silent fail if not available."""
    import subprocess
    import shutil

    if shutil.which("pbcopy"):
        cmd = ["pbcopy"]
    elif shutil.which("xclip"):
        cmd = ["xclip", "-selection", "clipboard"]
    elif shutil.which("xsel"):
        cmd = ["xsel", "--clipboard", "--input"]
    else:
        console.print("[dim]Clipboard not available[/dim]")
        return

    try:
        subprocess.run(cmd, input=text.encode(), check=True, timeout=5)
        console.print("[dim]Copied to clipboard[/dim]")
    except Exception:
        pass


@app.command("open")
def open_dataset(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    url: bool = typer.Option(False, "--url", help="Print URL instead of opening browser"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Open a dataset in the browser.

    Opens the DOI URL if available, otherwise falls back to the MDF portal.

    Examples:
        mdf open my_dataset_v1
        mdf open my_dataset_v1 --url
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    with api_spinner("Loading dataset..."):
        result = client.get_card(source_id, version=version)
    client.close()

    if not result.get("success"):
        require_success(result, error_prefix="Open failed")

    card = result.get("card", {})
    doi = card.get("doi")
    if doi:
        target_url = f"https://doi.org/{doi}"
    else:
        target_url = f"https://materialsdatafacility.org/datasets/{source_id}"

    if url:
        print(target_url)
    else:
        console.print(f"[dim]Opening:[/dim] {target_url}")
        webbrowser.open(target_url)


@app.command()
def preview(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    sample: bool = typer.Option(False, "--sample", help="Show tabular data sample"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Preview a dataset's files and data.

    Shows file listing by default. With --sample, shows tabular data preview.

    Examples:
        mdf preview my_dataset_v1
        mdf preview my_dataset_v1 --sample
        mdf preview my_dataset_v1 --json
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )

    if sample:
        with api_spinner("Loading sample..."):
            result = client.dataset_sample(source_id)
    else:
        with api_spinner("Loading preview..."):
            result = client.dataset_preview(source_id)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success", True):
            raise typer.Exit(code=1)
        return

    if not result.get("success", True):
        require_success(result, error_prefix="Preview failed")

    if sample:
        _render_sample(result)
    else:
        _render_preview(result)


def _render_preview(result: dict) -> None:
    """Render dataset preview as a Rich table of files."""
    files = result.get("files", result.get("data_sources", []))
    if not files:
        console.print("[dim]No file information available.[/dim]")
        return

    console.print(f"\n[bold]Files ({len(files)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Name", max_width=50)
    table.add_column("Size", style="dim", no_wrap=True, justify="right")
    table.add_column("Type", style="dim", no_wrap=True)

    for f in files:
        name = f.get("filename") or f.get("name") or f.get("path", "?")
        size = f.get("size") or f.get("length", "")
        if isinstance(size, (int, float)) and size > 0:
            size = _human_size(size)
        else:
            size = str(size) if size else ""
        ftype = f.get("mime_type") or f.get("type") or ""
        table.add_row(name, str(size), ftype)

    console.print(table)


def _render_sample(result: dict) -> None:
    """Render dataset sample as a Rich table."""
    rows = result.get("rows") or result.get("data", [])
    columns = result.get("columns", [])

    if not rows:
        console.print("[dim]No sample data available.[/dim]")
        return

    # Infer columns from first row if not provided
    if not columns and rows:
        first = rows[0]
        if isinstance(first, dict):
            columns = list(first.keys())

    console.print(f"\n[bold]Sample ({len(rows)} rows):[/bold]\n")
    table = Table(show_header=True, header_style="bold")
    for col in columns[:10]:  # cap at 10 columns
        table.add_column(str(col), max_width=30)

    for row in rows[:20]:  # cap at 20 rows
        if isinstance(row, dict):
            table.add_row(*[str(row.get(c, ""))[:30] for c in columns[:10]])
        elif isinstance(row, (list, tuple)):
            table.add_row(*[str(v)[:30] for v in row[:10]])

    console.print(table)
    if len(rows) > 20:
        console.print(f"[dim]... and {len(rows) - 20} more rows[/dim]")


def _human_size(nbytes: float) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


# ---------------------------------------------------------------------------
# mdf watch — poll until done
# ---------------------------------------------------------------------------

@app.command()
def watch(
    source_id: Optional[str] = typer.Argument(None, help="Source ID (default: last published)"),
    interval: int = typer.Option(10, "--interval", "-i", help="Poll interval in seconds"),
    timeout: int = typer.Option(1800, "--timeout", help="Timeout in seconds (default 30min)"),
    json_output: bool = typer.Option(False, "--json", help="Emit one JSON line per poll"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Watch a dataset until it reaches a terminal state.

    Polls the backend every --interval seconds until the dataset is
    published, rejected, or failed (or timeout is reached).

    Examples:
        mdf publish --submit && mdf watch
        mdf watch my_dataset_v1 --interval 5
    """
    resolved = resolve_service(service)

    lookup_id = source_id
    if not lookup_id:
        cfg = GlobalConfig()
        lookup_id = cfg.last_source_id
        if not lookup_id:
            console.print("[red]No source_id provided and no last published dataset.[/red]")
            console.print("[dim]Usage: mdf watch <source_id>[/dim]")
            raise typer.Exit(code=1)

    terminal_states = {"published", "rejected", "failed", "deleted"}
    start = time.monotonic()

    if not json_output:
        console.print(f"\n[bold]Watching[/bold] [cyan]{lookup_id}[/cyan] (poll every {interval}s, timeout {timeout}s)\n")

    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            if json_output:
                print(json.dumps({"source_id": lookup_id, "status": "timeout", "elapsed": round(elapsed)}))
            else:
                console.print(f"\n[yellow]Timeout after {timeout}s[/yellow]")
            raise typer.Exit(code=1)

        try:
            client = BackendClient.authenticated(
                base_url=api_url,
                token=token,
                service_instance=resolved,
                dev_user_id=dev_user,
            )
            result = client.status(lookup_id)
            client.close()
        except Exception as e:
            if json_output:
                print(json.dumps({"source_id": lookup_id, "error": str(e), "elapsed": round(elapsed)}))
            else:
                console.print(f"  [yellow]Error:[/yellow] {e}")
            time.sleep(interval)
            continue

        sub = result.get("submission") or result
        st = sub.get("status", "unknown")

        if json_output:
            print(json.dumps({"source_id": lookup_id, "status": st, "elapsed": round(elapsed)}))
        else:
            console.print(f"  {format_status_badge(st)}  [dim]({round(elapsed)}s)[/dim]")

        if st in terminal_states:
            if st == "published":
                if not json_output:
                    doi = sub.get("dataset_doi") or sub.get("doi")
                    if doi:
                        console.print(f"\n  [dim]DOI:[/dim] https://doi.org/{doi}")
                    console.print("\n[bold green]Done![/bold green]")
                raise typer.Exit(code=0)
            else:
                if not json_output:
                    console.print(f"\n[red]Terminal state: {st}[/red]")
                raise typer.Exit(code=1)

        time.sleep(interval)


# ---------------------------------------------------------------------------
# mdf doctor — self-diagnostic
# ---------------------------------------------------------------------------

@app.command()
def doctor(
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
):
    """Run diagnostics on your MDF Agent setup.

    Checks config, auth, service, connectivity, and local manifest.

    Examples:
        mdf doctor
        mdf doctor --json
    """
    from mdf_agent.auth.globus import is_logged_in

    resolved = resolve_service(service)
    checks: list[dict] = []

    # 1. Config
    cfg_path = Path.home() / ".config" / "mdf_agent" / "config.json"
    checks.append({
        "name": "Config",
        "ok": cfg_path.exists(),
        "detail": str(cfg_path),
    })

    # 2. Auth
    try:
        authed = is_logged_in(service_instance=resolved)
        env_tok = bool(os.environ.get("MDF_CONNECT_TOKEN"))
        auth_ok = authed or env_tok
        auth_detail = "Logged in (token cached)" if authed else ("MDF_CONNECT_TOKEN set" if env_tok else "Not authenticated")
    except Exception:
        auth_ok = False
        auth_detail = "Error checking auth"
    checks.append({"name": "Auth", "ok": auth_ok, "detail": auth_detail})

    # 3. Service resolution
    try:
        base = api_url or _api_url_for_service(resolved)
        service_detail = f"{resolved} ({base[:30]}...)" if len(base) > 30 else f"{resolved} ({base})"
        checks.append({"name": "Service", "ok": True, "detail": service_detail})
    except Exception as e:
        checks.append({"name": "Service", "ok": False, "detail": str(e)})
        base = None

    # 4. Connectivity
    if base:
        try:
            t0 = time.monotonic()
            client = BackendClient(base_url=base)
            with api_spinner("Checking connectivity..."):
                health = client.health()
            client.close()
            ms = round((time.monotonic() - t0) * 1000)
            conn_ok = health.get("success", health.get("status") == "ok")
            checks.append({"name": "Connectivity", "ok": conn_ok, "detail": f"Backend healthy ({ms}ms)"})
        except Exception as e:
            checks.append({"name": "Connectivity", "ok": False, "detail": str(e)})
    else:
        checks.append({"name": "Connectivity", "ok": False, "detail": "No service URL"})

    # 5. Manifest
    mdf_yaml = Path.cwd() / "mdf.yaml"
    if mdf_yaml.exists():
        try:
            MDFAgent.from_manifest(".")
            checks.append({"name": "Manifest", "ok": True, "detail": "mdf.yaml valid"})
        except Exception as e:
            checks.append({"name": "Manifest", "ok": False, "detail": f"mdf.yaml invalid: {e}"})
    else:
        checks.append({"name": "Manifest", "ok": False, "detail": "No mdf.yaml in current directory"})

    if json_output:
        print(json.dumps({"success": all(c["ok"] for c in checks), "checks": checks}, indent=2))
        return

    console.print(f"\n[bold]MDF Agent[/bold] v{__version__}\n")
    for c in checks:
        symbol = "[green]*[/green]" if c["ok"] else "[dim]-[/dim]"
        console.print(f"  {symbol}  [bold]{c['name']:14s}[/bold] {c['detail']}")
    console.print()


if __name__ == "__main__":
    app()
