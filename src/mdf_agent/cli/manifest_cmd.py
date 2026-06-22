"""MDF manifest subcommands.

Provides `mdf manifest init` and `mdf manifest discover` for creating
and enriching mdf.yaml manifest files without git.
"""

from __future__ import annotations

import sys
import json
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from mdf_agent.core.agent import MDFAgent
from mdf_agent.core.config import resolve_service
from mdf_agent.cli.preflight import suggest_data_sources

console = Console()


def _summarize_extracted(extracted: dict) -> None:
    """Print a human-readable summary of discovered metadata."""
    mdf = extracted.get("mdf", extracted) if isinstance(extracted, dict) else {}
    schema = mdf.get("table_schema") if isinstance(mdf, dict) else None
    printed = False
    if isinstance(schema, dict):
        cols = schema.get("columns") or []
        fname = schema.get("file", "table")
        rows = schema.get("row_count")
        detail = f"{len(cols)} column(s)"
        if rows is not None:
            detail += f", {rows} row(s)"
        console.print(f"  [dim]{fname}:[/dim] {detail}")
        if cols:
            names = ", ".join(str(c.get("name", c)) for c in cols[:8])
            more = "" if len(cols) <= 8 else f" (+{len(cols) - 8} more)"
            console.print(f"    [dim]columns:[/dim] {names}{more}")
        printed = True
    if not printed:
        for key in extracted:
            console.print(f"  [dim]{key}[/dim]")


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
    organization: Optional[str] = typer.Option(None, "--organization", "-o", help="Owning organization"),
    keyword: Optional[List[str]] = typer.Option(None, "--keyword", "-k", help="Keyword (repeatable)"),
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
    resolved_keywords = list(keyword) if keyword else []

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
            if not organization:
                organization = typer.prompt("Organization (optional)", default="", show_default=False) or None
            if not resolved_keywords:
                raw_keywords = typer.prompt(
                    "Keywords (comma-separated, optional)",
                    default="",
                    show_default=False,
                )
                if raw_keywords:
                    resolved_keywords = [part.strip() for part in raw_keywords.split(",") if part.strip()]
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

    agent = MDFAgent.init_manifest(
        path=path,
        title=resolved_title,
        authors=resolved_authors,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )
    suggested_sources = suggest_data_sources(resolved_path)
    if suggested_sources and sys.stdin.isatty():
        joined = ", ".join(suggested_sources)
        if typer.confirm(f"Use detected data source candidates ({joined})?", default=True):
            agent.manifest.data_sources = suggested_sources
    if organization:
        agent.manifest.organization = organization
    if resolved_keywords:
        agent.manifest.subjects = resolved_keywords
    if agent.manifest.data_sources or organization or resolved_keywords:
        agent.save_manifest()
    console.print(f"[green]Created mdf.yaml in[/green] [bold]{path}[/bold]")
    console.print(f"  [dim]Title:[/dim] {resolved_title}")
    console.print(f"  [dim]Authors:[/dim] {', '.join(resolved_authors)}")
    if agent.manifest.data_sources:
        console.print(f"  [dim]Data sources:[/dim] {', '.join(str(s) for s in agent.manifest.data_sources)}")
    if organization:
        console.print(f"  [dim]Organization:[/dim] {organization}")
    if resolved_keywords:
        console.print(f"  [dim]Keywords:[/dim] {', '.join(resolved_keywords)}")


@app.command("discover")
def manifest_discover(
    paths: List[str] = typer.Argument(..., help="Files or globs to extract metadata from"),
    preview: bool = typer.Option(False, "--preview", help="Show extracted metadata without writing to mdf.yaml"),
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

    if preview:
        resolved_files: List[str] = []
        for pattern in paths:
            matches = list(agent.root.glob(pattern)) if agent.root else []
            if not matches and agent.root:
                candidate = agent.root / pattern
                if candidate.exists():
                    matches = [candidate]
            for match in matches:
                if match.is_file():
                    resolved_files.append(str(match.resolve()))
        from mdf_agent.extractors.registry import discover_metadata

        extracted = discover_metadata(resolved_files)
    else:
        extracted = agent.discover(*paths)
    if extracted:
        if preview:
            console.print("[bold cyan]Preview only - extracted metadata:[/bold cyan]")
            console.print(Syntax(json.dumps(extracted, indent=2), "json", theme="monokai"))
        else:
            console.print("[green]Extracted metadata saved to mdf.yaml[/green]")
            _summarize_extracted(extracted)
    else:
        console.print("[yellow]No metadata extracted from the provided files[/yellow]")


@app.command("inspect")
def manifest_inspect(
    path: str = typer.Argument(".", help="Directory containing mdf.yaml"),
):
    """Show a readable summary of the current manifest."""
    from mdf_agent.cli.preflight import run_preflight

    root = Path(path).resolve()
    manifest_path = root / "mdf.yaml"
    if not manifest_path.exists():
        console.print(f"[red]No mdf.yaml found in {root}[/red]")
        raise typer.Exit(code=1)

    agent = MDFAgent.from_manifest(str(root))
    manifest = agent.manifest
    preflight = run_preflight(manifest, root=root, service=resolve_service(None), submit=False)

    title = manifest.title[0] if isinstance(manifest.title, list) else manifest.title or "Untitled"
    authors = []
    for author in manifest.authors or []:
        if isinstance(author, str):
            authors.append(author)
        else:
            authors.append(author.name)

    console.print()
    console.print(Panel(
        f"[bold]{title}[/bold]\n"
        f"[dim]{manifest.description or 'No description'}[/dim]",
        title="mdf.yaml",
        border_style="blue",
    ))

    table = Table(show_header=False, padding=(0, 2))
    table.add_column("Field", style="dim", width=14)
    table.add_column("Value")
    table.add_row("Authors", ", ".join(authors) or "None")
    table.add_row("Publisher", manifest.publisher or "None")
    table.add_row("Organization", manifest.organization or "None")
    table.add_row("Keywords", ", ".join(manifest.subjects or []) or "None")
    table.add_row("Data sources", str(len(manifest.data_sources or [])) if manifest.data_sources else "Auto-scan current directory")
    console.print(table)

    if preflight["sources"]:
        console.print("\n[bold]Resolved sources[/bold]\n")
        source_table = Table(show_header=True, header_style="bold")
        source_table.add_column("Source")
        source_table.add_column("Kind", style="dim")
        source_table.add_column("Files", justify="right")
        source_table.add_column("Bytes", justify="right")
        for source in preflight["sources"]:
            files = source.get("file_count")
            bytes_value = source.get("total_bytes")
            source_table.add_row(
                source.get("source", ""),
                source.get("kind", ""),
                "-" if files is None else str(files),
                "-" if bytes_value is None else str(bytes_value),
            )
        console.print(source_table)

    if preflight["issues"]:
        console.print("\n[bold]Preflight notes[/bold]")
        for issue in preflight["issues"]:
            console.print(f"  [dim]{issue['severity']}[/dim] {issue['message']}")
