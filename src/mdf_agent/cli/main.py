"""MDF Agent CLI - Main entry point.

This module provides the command-line interface for MDF Agent.
Commands are organized as direct subcommands of the main `mdf` command.

Usage:
    mdf init ./my_dataset --title "My Dataset" --author "Jane Doe"
    mdf add *.csv
    mdf commit -m "Add experimental data"
    mdf publish --test
    mdf publish ./data/ --title "Test" --author "Jane" --submit
"""

from __future__ import annotations

from functools import wraps
import os
import sys
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from mdf_agent.core.agent import MDFAgent
from mdf_agent.core.backend_client import BackendClient, _api_url_for_service
from mdf_agent.core.config import GlobalConfig, resolve_service
from mdf_agent.core.exceptions import NotARepositoryError
from mdf_agent.cli.backend import app as backend_app
from mdf_agent.cli.stream import app as stream_app
from mdf_agent.cli.config_cmd import app as config_app

console = Console()


def handle_repo_error(func):
    """Decorator to catch NotARepositoryError and display a clean message."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotARepositoryError:
            console.print("\n[red]Not an MDF repository[/red]")
            console.print("[dim]Run [/dim][cyan]mdf init[/cyan][dim] to create one here, or cd to an existing repository.[/dim]\n")
            raise typer.Exit(code=1)
    return wrapper


app = typer.Typer(
    help="MDF Agent CLI - Materials Data Facility dataset management",
    add_completion=False,
    no_args_is_help=True,
)

app.add_typer(backend_app, name="backend")
app.add_typer(stream_app, name="stream")
app.add_typer(config_app, name="config")


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
        # Explicit token: just use get_authorizer (no multi-scope needed)
        from mdf_agent.auth.globus import get_authorizer
        get_authorizer(token=token, service_instance=resolved)
    else:
        scope, _rs = get_scopes_for_service(resolved)
        # Request scopes upfront so one login covers publish, upload,
        # and transfer operations.
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
):
    """Show current authentication status."""
    from mdf_agent.auth.globus import DEFAULT_TOKEN_PATH, is_logged_in

    resolved = resolve_service(service)
    cached = is_logged_in(service_instance=resolved)
    env_token = bool(os.environ.get("MDF_CONNECT_TOKEN"))
    status = "authenticated" if (cached or env_token) else "not authenticated"
    console.print(f"[bold]Service:[/bold] {resolved}")
    console.print(f"[bold]Status:[/bold] {status}")
    console.print(f"[bold]Token store:[/bold] {DEFAULT_TOKEN_PATH}")
    if env_token:
        console.print("[dim]MDF_CONNECT_TOKEN is set in environment[/dim]")


@app.command()
def init(
    path: str = typer.Argument(".", help="Repository path"),
    title: str = typer.Option(..., "--title", "-t", help="Dataset title"),
    author: List[str] = typer.Option(..., "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    publisher: Optional[str] = typer.Option(None, "--publisher", help="Dataset publisher"),
    publication_year: Optional[int] = typer.Option(None, "--year", "-y", help="Publication year"),
):
    """Initialize an MDF dataset repository.

    Creates a new mdf.yaml manifest and .mdf/ state directory.
    """
    MDFAgent.init(
        path=path,
        title=title,
        authors=author,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )
    console.print(f"[green]Initialized MDF repository at[/green] [bold]{path}[/bold]")
    console.print(f"  [dim]Title:[/dim] {title}")
    console.print(f"  [dim]Authors:[/dim] {', '.join(author)}")


@app.command()
@handle_repo_error
def add(
    paths: List[str] = typer.Argument(..., help="Files or globs to stage"),
    discover: Optional[bool] = typer.Option(
        None, "--discover/--no-discover", help="Auto-discover metadata from files"
    ),
):
    """Stage files for the next commit.

    Supports glob patterns like *.csv or data/**/*.json.
    Use --discover to automatically extract metadata from PDFs and data files.
    """
    agent = MDFAgent.from_repo(".")
    staged = agent.add(*paths, discover=discover)
    console.print("[green]Staged:[/green]")
    for file_path in staged:
        console.print(f"  [cyan]+[/cyan] {file_path}")


@app.command()
@handle_repo_error
def commit(
    message: str = typer.Option(..., "--message", "-m", help="Commit message"),
):
    """Record staged files as a commit.

    Creates a local checkpoint that can later be published to MDF Connect.
    """
    agent = MDFAgent.from_repo(".")
    commit_data = agent.commit(message)
    file_count = len(commit_data.get('staged_files', []))
    console.print(f"[green]Committed:[/green] {commit_data.get('message', '')}")
    console.print(f"  [dim]{file_count} file{'s' if file_count != 1 else ''} recorded[/dim]")


@app.command()
def status(
    source_id: Optional[str] = typer.Argument(None, help="Source ID to check (default: last published)"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Show dataset status.

    With no args inside a repo: shows local repo status + backend status.
    With no args outside a repo: shows backend status of last published dataset.
    With source_id: shows backend status for that dataset.
    """
    resolved = resolve_service(service)

    # Try to show repo status if we're in a repo
    in_repo = False
    try:
        agent = MDFAgent.from_repo(".")
        in_repo = True
        state = agent.status()

        staged = state.get("staged_files", [])
        if staged:
            console.print("\n[bold]Staged files:[/bold]")
            for f in staged:
                console.print(f"  [green]+[/green] {f}")
        else:
            console.print("\n[dim]No files staged[/dim]")

        commits = state.get("commits", [])
        if commits:
            console.print(f"\n[bold]Commits ({len(commits)}):[/bold]")
            table = Table(show_header=True, header_style="bold")
            table.add_column("#", style="dim", width=4)
            table.add_column("Message")
            table.add_column("Files", justify="right")
            table.add_column("Time", style="dim")

            for i, c in enumerate(commits, 1):
                table.add_row(
                    str(i),
                    c.get("message", ""),
                    str(len(c.get("staged_files", []))),
                    c.get("timestamp", "")[:19] if c.get("timestamp") else "",
                )
            console.print(table)
        else:
            console.print("\n[dim]No commits yet[/dim]")
    except NotARepositoryError:
        pass

    # Resolve source_id for backend lookup
    lookup_id = source_id
    if not lookup_id:
        cfg = GlobalConfig()
        lookup_id = cfg.last_source_id
        if not lookup_id:
            if not in_repo:
                console.print("\n[dim]No source_id provided and no last published dataset.[/dim]")
                console.print("[dim]Usage: mdf status <source_id>[/dim]")
            return

    # Backend status lookup
    try:
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=resolved,
            dev_user_id=dev_user,
        )
        result = client.status(lookup_id, version=version)
        client.close()

        if result.get("source_id"):
            console.print(f"\n[bold]Backend status:[/bold] [cyan]{result.get('source_id')}[/cyan] v{result.get('version', '?')}")
            console.print(f"  [dim]Status:[/dim] {result.get('status', 'unknown')}")
            if result.get("title"):
                console.print(f"  [dim]Title:[/dim] {result.get('title')}")
            if result.get("doi"):
                console.print(f"  [dim]DOI:[/dim] https://doi.org/{result.get('doi')}")
        elif result.get("error"):
            console.print(f"\n[yellow]Backend:[/yellow] {result.get('error')}")
        else:
            console.print(f"\n[dim]No backend record for {lookup_id}[/dim]")
    except Exception as e:
        console.print(f"\n[yellow]Could not reach backend:[/yellow] {e}")


@app.command()
@handle_repo_error
def validate():
    """Validate manifest before publishing.

    Checks for required fields and common issues.
    Returns exit code 1 if validation fails.
    """
    agent = MDFAgent.from_repo(".")
    results = agent.validate()
    errors = results.get("errors", [])
    warnings = results.get("warnings", [])

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


@app.command()
def publish(
    data: Optional[List[str]] = typer.Argument(None, help="Data paths/URIs to publish (direct mode)"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Dataset title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit", help="Preview without submitting"),
    test: bool = typer.Option(False, "--test", help="Submit to test environment"),
    update: bool = typer.Option(False, "--update", "-u", help="Update existing dataset"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL for local backend"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """Publish dataset to MDF Connect.

    Direct mode (data args provided):
        mdf publish ./data/ --title "My Dataset" --author "Jane" --submit

    Repo mode (inside an MDF repository):
        mdf publish --submit

    By default, performs a dry run showing the payload.
    Use --submit to actually send to MDF Connect.
    """
    import json
    from rich.syntax import Syntax
    from mdf_agent.models.config import ManifestConfig

    resolved = resolve_service(service)

    # Mode detection
    if data:
        # Direct mode: build manifest on the fly
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
    else:
        # Repo mode
        try:
            agent = MDFAgent.from_repo(".")
        except NotARepositoryError:
            console.print("\n[red]No data paths provided and not in an MDF repository[/red]")
            console.print("[dim]Direct mode:[/dim]  mdf publish ./data/ --title \"My Dataset\" --author \"Jane\" --submit")
            console.print("[dim]Repo mode:[/dim]    cd my_repo && mdf publish --submit")
            raise typer.Exit(code=1)

        # In repo mode, --title and --author are optional overrides
        if title:
            agent.manifest.title = title
        if author:
            agent.manifest.authors = author
        if description:
            agent.manifest.description = description

    payload = agent.build_submission(test=test, update=update)

    if dry_run:
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

    if result.get("success"):
        console.print("\n[bold green]Published successfully![/bold green]")
        source_id = result.get("source_id")
        version = result.get("version")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{source_id}[/cyan]")
        if version:
            console.print(f"  [dim]Version:[/dim] [cyan]{version}[/cyan]")

        # Save to global config
        if source_id:
            cfg = GlobalConfig()
            cfg.record_publish(source_id, version, resolved)
    else:
        console.print(f"\n[bold red]Publish failed:[/bold red] {result.get('error')}")
        raise typer.Exit(code=1)


@app.command()
def clone(
    source_id: str = typer.Argument(..., help="Source dataset ID to derive from"),
    output: str = typer.Option(".", "--output", "-o", help="Output directory"),
    title: str = typer.Option(..., "--title", "-t", help="New dataset title"),
    author: List[str] = typer.Option(..., "--author", "-a", help="Author name (repeatable)"),
    relationship: Optional[str] = typer.Option(
        None, "--relationship", "-r", help="Relationship type (e.g., filtered_subset)"
    ),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Description of derivation"),
):
    """Create a new dataset derived from an existing MDF dataset.

    Sets up the derived_from field to track lineage.
    """
    from pathlib import Path
    from mdf_agent.models.config import DerivedFrom
    from mdf_agent.core.repository import Repository

    root = Path(output)

    # Create repository with derived_from set
    repo = Repository.init_repo(
        root=root,
        title=title,
        authors=author,
    )

    # Load and update manifest with derived_from
    manifest = repo.load_manifest()
    manifest.derived_from = [
        DerivedFrom(
            source_id=source_id,
            relationship=relationship,
            description=description,
        )
    ]
    repo.save_manifest(manifest)

    console.print(f"[green]Created derived dataset at[/green] [bold]{output}[/bold]")
    console.print(f"  [dim]Derived from:[/dim] [cyan]{source_id}[/cyan]")
    if relationship:
        console.print(f"  [dim]Relationship:[/dim] {relationship}")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    search_type: str = typer.Option("all", "--type", "-t", help="all, datasets, or streams"),
    limit: int = typer.Option(20, "--limit", "-n", help="Max results"),
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
    result = client.search(query, search_type=search_type, limit=limit)
    client.close()

    if result.get("results"):
        total = result.get("total", 0)
        console.print(f"\n[bold]Found {total} results for[/bold] [cyan]'{result.get('query')}'[/cyan]\n")

        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=3)
        table.add_column("Type", width=8)
        table.add_column("Title")
        table.add_column("ID")
        table.add_column("Status", style="dim")

        for i, item in enumerate(result["results"], 1):
            if item.get("type") == "dataset":
                table.add_row(
                    str(i),
                    "[blue]dataset[/blue]",
                    item.get("title", "Untitled")[:40],
                    f"{item.get('source_id')} v{item.get('version')}",
                    item.get("status", ""),
                )
            else:
                table.add_row(
                    str(i),
                    "[green]stream[/green]",
                    item.get("title", "Untitled")[:40],
                    item.get("stream_id", ""),
                    f"{item.get('file_count', 0)} files",
                )

        console.print(table)
    else:
        console.print(f"\n[dim]No results found for '{query}'[/dim]")


@app.command()
def pending(
    limit: int = typer.Option(20, "--limit", "-n", help="Max results"),
    organization: Optional[str] = typer.Option(None, "--organization", "-o", help="Filter by organization"),
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
    result = client.curation_pending(limit=limit, organization=organization)
    client.close()

    submissions = result.get("submissions", [])
    if not submissions:
        console.print("\n[dim]No datasets pending curation.[/dim]")
        return

    console.print(f"\n[bold]Pending curation ({len(submissions)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=3)
    table.add_column("Source ID")
    table.add_column("Title")
    table.add_column("Version", style="dim")
    table.add_column("Submitted", style="dim")

    for i, sub in enumerate(submissions, 1):
        table.add_row(
            str(i),
            sub.get("source_id", ""),
            (sub.get("title") or "Untitled")[:40],
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
    result = client.curation_approve(
        source_id=source_id,
        mint_doi=mint_doi,
        notes=notes,
        version=version,
    )
    client.close()

    if result.get("success"):
        console.print(f"\n[bold green]Approved:[/bold green] [cyan]{source_id}[/cyan]")
        if result.get("doi"):
            console.print(f"  [dim]DOI:[/dim] https://doi.org/{result.get('doi')}")
    else:
        console.print(f"\n[bold red]Approve failed:[/bold red] {result.get('error', 'Unknown error')}")
        raise typer.Exit(code=1)


@app.command()
def reject(
    source_id: str = typer.Argument(..., help="Source ID of dataset to reject"),
    reason: str = typer.Option(..., "--reason", "-r", help="Rejection reason"),
    suggestions: Optional[str] = typer.Option(None, "--suggestions", help="Suggestions for improvement"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
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
    result = client.curation_reject(
        source_id=source_id,
        reason=reason,
        suggestions=suggestions,
        version=version,
    )
    client.close()

    if result.get("success"):
        console.print(f"\n[bold yellow]Rejected:[/bold yellow] [cyan]{source_id}[/cyan]")
        console.print(f"  [dim]Reason:[/dim] {reason}")
    else:
        console.print(f"\n[bold red]Reject failed:[/bold red] {result.get('error', 'Unknown error')}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
