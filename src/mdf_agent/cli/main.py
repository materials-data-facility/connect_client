"""MDF Agent CLI - Main entry point.

This module provides the command-line interface for MDF Agent.
Commands are organized as direct subcommands of the main `mdf` command.

Usage:
    mdf init ./my_dataset --title "My Dataset" --author "Jane Doe"
    mdf add *.csv
    mdf commit -m "Add experimental data"
    mdf publish --test
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
from mdf_agent.core.exceptions import NotARepositoryError
from mdf_agent.cli.backend import app as backend_app
from mdf_agent.cli.stream import app as stream_app

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


@app.command()
def login(
    service: str = typer.Option("staging", "--service", "-s", help="Service instance (staging/prod/dev)"),
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

    if token:
        # Explicit token: just use get_authorizer (no multi-scope needed)
        from mdf_agent.auth.globus import get_authorizer
        get_authorizer(token=token, service_instance=service)
    else:
        scope, _rs = get_scopes_for_service(service)
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
    service: str = typer.Option("staging", "--service", "-s", help="Service instance (staging/prod/dev)"),
):
    """Show current authentication status."""
    from mdf_agent.auth.globus import DEFAULT_TOKEN_PATH, is_logged_in

    cached = is_logged_in(service_instance=service)
    env_token = bool(os.environ.get("MDF_CONNECT_TOKEN"))
    status = "authenticated" if (cached or env_token) else "not authenticated"
    console.print(f"[bold]Service:[/bold] {service}")
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
@handle_repo_error
def status():
    """Show repository status.

    Displays staged files, commits, and current state.
    """
    agent = MDFAgent.from_repo(".")
    state = agent.status()

    # Display staged files
    staged = state.get("staged_files", [])
    if staged:
        console.print("\n[bold]Staged files:[/bold]")
        for f in staged:
            console.print(f"  [green]+[/green] {f}")
    else:
        console.print("\n[dim]No files staged[/dim]")

    # Display commits
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
@handle_repo_error
def publish(
    test: bool = typer.Option(False, "--test", "-t", help="Submit to test environment"),
    update: bool = typer.Option(False, "--update", "-u", help="Update existing dataset"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit", help="Preview without submitting"),
    service: str = typer.Option("staging", "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL for local backend"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """Publish dataset to MDF Connect.

    By default, performs a dry run showing the payload.
    Use --submit to actually send to MDF Connect.

    Examples:
        mdf publish                            # Dry run - show payload
        mdf publish --submit                   # Submit to production
        mdf publish --submit --service local   # Submit to local backend
    """
    import json
    from rich.syntax import Syntax

    agent = MDFAgent.from_repo(".")
    payload = agent.build_submission(test=test, update=update)

    if dry_run:
        console.print("\n[bold cyan]Dry run - would submit:[/bold cyan]")
        target = api_url or _api_url_for_service(service)
        console.print(f"[dim]Target: {target} ({service})[/dim]")
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
            service_instance=service,
            api_url=api_url,
            dev_user_id=dev_user,
            progress_callback=progress_callback,
        )
    finally:
        if progress_ctx is not None:
            progress_ctx.stop()

    if result.get("success"):
        console.print("\n[bold green]Published successfully![/bold green]")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{result.get('source_id')}[/cyan]")
        if result.get("version"):
            console.print(f"  [dim]Version:[/dim] [cyan]{result.get('version')}[/cyan]")
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
    service: str = typer.Option("staging", "--service", "-s", help="Service instance (staging/prod/dev/local)"),
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
    resolved_token = token or os.environ.get("MDF_CONNECT_TOKEN")
    resolved_dev_user = dev_user or os.environ.get("MDF_DEV_USER_ID")
    if resolved_token or resolved_dev_user:
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=service,
            dev_user_id=dev_user,
        )
    else:
        client = BackendClient(base_url=api_url or _api_url_for_service(service))
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


if __name__ == "__main__":
    app()
