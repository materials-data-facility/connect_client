"""MDF manifest subcommands.

Provides `mdf manifest init` and `mdf manifest discover` for creating
and enriching mdf.yaml manifest files without git.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console

from mdf_agent.core.agent import MDFAgent

console = Console()

app = typer.Typer(
    help="Manage mdf.yaml manifest files",
    no_args_is_help=True,
)


@app.command("init")
def manifest_init(
    path: str = typer.Argument(".", help="Directory to create mdf.yaml in"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Dataset title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    publisher: Optional[str] = typer.Option(None, "--publisher", help="Dataset publisher"),
    publication_year: Optional[int] = typer.Option(None, "--year", "-y", help="Publication year"),
):
    """Create an mdf.yaml manifest for a dataset directory.

    The manifest stores metadata (title, authors, etc.) so you don't
    have to pass it every time you publish.

    Examples:
        mdf manifest init --title "My Dataset" --author "Jane Doe"
        mdf manifest init ./my_data --title "Test" --author "Jane"
    """
    resolved_title = title
    resolved_authors = list(author) if author else []

    if not resolved_title or not resolved_authors:
        if sys.stdin.isatty():
            console.print("[bold]Create MDF manifest[/bold]\n")
            if not resolved_title:
                resolved_title = typer.prompt("Dataset title")
            if not resolved_authors:
                console.print("[dim]Enter author names one per line. Empty line to finish.[/dim]")
                while True:
                    name = typer.prompt("Author", default="", show_default=False)
                    if not name:
                        break
                    resolved_authors.append(name)
            if not description:
                description = typer.prompt("Description (optional)", default="", show_default=False) or None
        else:
            if not resolved_title:
                console.print("[red]--title is required[/red]")
                raise typer.Exit(code=1)
            if not resolved_authors:
                console.print("[red]--author is required[/red]")
                raise typer.Exit(code=1)

    if not resolved_title:
        console.print("[red]Title is required[/red]")
        raise typer.Exit(code=1)
    if not resolved_authors:
        console.print("[red]At least one author is required[/red]")
        raise typer.Exit(code=1)

    resolved_path = Path(path).resolve()
    if (resolved_path / "mdf.yaml").exists():
        console.print(f"[yellow]mdf.yaml already exists in {path}[/yellow]")
        raise typer.Exit(code=1)

    MDFAgent.init_manifest(
        path=path,
        title=resolved_title,
        authors=resolved_authors,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )
    console.print(f"[green]Created mdf.yaml in[/green] [bold]{path}[/bold]")
    console.print(f"  [dim]Title:[/dim] {resolved_title}")
    console.print(f"  [dim]Authors:[/dim] {', '.join(resolved_authors)}")


@app.command("discover")
def manifest_discover(
    paths: List[str] = typer.Argument(..., help="Files or globs to extract metadata from"),
):
    """Extract metadata from data files and save to mdf.yaml.

    Scans the provided files for metadata (column names, schemas, etc.)
    and stores the results in mdf.yaml's auto_metadata section.

    Requires an existing mdf.yaml in the current directory.

    Examples:
        mdf manifest discover *.csv
        mdf manifest discover data/*.json data/*.yaml
    """
    from mdf_agent.core.exceptions import NoManifestError

    try:
        agent = MDFAgent.from_manifest(".")
    except NoManifestError:
        console.print("[red]No mdf.yaml found in current directory[/red]")
        console.print("[dim]Run [/dim][cyan]mdf manifest init[/cyan][dim] first.[/dim]")
        raise typer.Exit(code=1)

    extracted = agent.discover(*paths)
    if extracted:
        console.print("[green]Extracted metadata saved to mdf.yaml[/green]")
        for key in extracted:
            console.print(f"  [dim]{key}[/dim]")
    else:
        console.print("[yellow]No metadata extracted from the provided files[/yellow]")
