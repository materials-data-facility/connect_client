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
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from mdf_agent.core.agent import MDFAgent
from mdf_agent.core.exceptions import NotARepositoryError

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
    service: str = typer.Option("prod", "--service", "-s", help="Service instance (prod/dev)"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
):
    """Publish dataset to MDF Connect.

    By default, performs a dry run showing the payload.
    Use --submit to actually send to MDF Connect.
    """
    import json
    from rich.syntax import Syntax
    from mdf_agent.auth.globus import get_authorizer

    agent = MDFAgent.from_repo(".")

    if dry_run:
        payload = agent.build_submission(test=test, update=update)
        console.print("\n[bold cyan]Dry run - would submit:[/bold cyan]")
        syntax = Syntax(json.dumps(payload, indent=2), "json", theme="monokai")
        console.print(syntax)
        return

    authorizer = get_authorizer(token=token, service_instance=service)
    result = agent.publish(
        test=test,
        update=update,
        dry_run=False,
        authorizer=authorizer,
        service_instance=service,
    )

    if result.get("success"):
        console.print("\n[bold green]Published successfully![/bold green]")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{result.get('source_id')}[/cyan]")
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


if __name__ == "__main__":
    app()
