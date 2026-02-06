from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from mdf_agent.core.backend_client import BackendClient

app = typer.Typer(help="Interact with MDF backend (v2) API")
console = Console()
_auth_opts: Dict[str, Optional[str]] = {"service": "prod", "token": None, "dev_user": None}


@app.callback()
def backend_callback(
    service: str = typer.Option("prod", "--service", "-s", help="Service instance (prod/dev/local)"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """Interact with MDF backend (v2) API."""
    _auth_opts.update(service=service, token=token, dev_user=dev_user)


def _client(api_url: Optional[str]) -> BackendClient:
    return BackendClient.authenticated(
        base_url=api_url,
        token=_auth_opts.get("token"),
        service_instance=_auth_opts.get("service") or "prod",
        dev_user_id=_auth_opts.get("dev_user"),
    )


def _print(result):
    typer.echo(json.dumps(result, indent=2))


@app.command("submit")
def submit(
    payload: Path = typer.Option(..., "--payload", help="Path to submission JSON"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    data = json.loads(payload.read_text(encoding="utf-8"))
    result = client.submit(data)
    client.close()
    _print(result)


@app.command("status")
def status(
    source_id: str = typer.Option(..., "--source-id"),
    version: Optional[str] = typer.Option(None, "--version"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.status(source_id, version=version)
    client.close()
    _print(result)


@app.command("submissions")
def submissions(
    organization: Optional[str] = typer.Option(None, "--organization"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.submissions(organization=organization)
    client.close()
    _print(result)


@app.command("update-status")
def update_status(
    source_id: str = typer.Option(..., "--source-id"),
    version: str = typer.Option(..., "--version"),
    status: str = typer.Option(..., "--status"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.update_status(source_id, version, status)
    client.close()
    _print(result)


@app.command("stream-create")
def stream_create(
    title: str = typer.Option(..., "--title"),
    lab_id: Optional[str] = typer.Option(None, "--lab-id"),
    organization: Optional[str] = typer.Option(None, "--organization"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.stream_create(title, lab_id=lab_id, organization=organization)
    client.close()
    _print(result)


@app.command("stream-append")
def stream_append(
    stream_id: str = typer.Option(..., "--stream-id"),
    files: Optional[Path] = typer.Option(None, "--files", help="JSON list of files or dict with files"),
    file_count: Optional[int] = typer.Option(None, "--file-count"),
    total_bytes: Optional[int] = typer.Option(None, "--total-bytes"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    files_payload = None
    if files:
        files_payload = json.loads(files.read_text(encoding="utf-8"))
        if isinstance(files_payload, dict) and "files" in files_payload:
            files_payload = files_payload["files"]
    result = client.stream_append(
        stream_id,
        files=files_payload,
        file_count=file_count,
        total_bytes=total_bytes,
    )
    client.close()
    _print(result)


@app.command("stream-status")
def stream_status(
    stream_id: str = typer.Option(..., "--stream-id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.stream_status(stream_id)
    client.close()
    _print(result)


@app.command("stream-close")
def stream_close(
    stream_id: str = typer.Option(..., "--stream-id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.stream_close(stream_id)
    client.close()
    _print(result)


@app.command("stream-snapshot")
def stream_snapshot(
    stream_id: str = typer.Option(..., "--stream-id"),
    title: Optional[str] = typer.Option(None, "--title"),
    update: bool = typer.Option(False, "--update", help="Update existing dataset if present"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    client = _client(api_url)
    result = client.stream_snapshot(stream_id, title=title, update=update)
    client.close()
    _print(result)


@app.command("search")
def search(
    query: str = typer.Argument(..., help="Search query"),
    search_type: str = typer.Option("all", "--type", "-t", help="all, datasets, or streams"),
    limit: int = typer.Option(20, "--limit", "-n", help="Max results"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Search datasets and streams."""
    client = _client(api_url)
    result = client.search(query, search_type=search_type, limit=limit)
    client.close()

    # Pretty-print search results
    if result.get("results"):
        typer.echo(f"\nFound {result.get('total', 0)} results for '{result.get('query')}':\n")
        for i, item in enumerate(result["results"], 1):
            if item.get("type") == "dataset":
                typer.echo(f"  {i}. [DATASET] {item.get('title', 'Untitled')}")
                typer.echo(f"     source_id: {item.get('source_id')} v{item.get('version')}")
                typer.echo(f"     status: {item.get('status')} | authors: {', '.join(item.get('authors', []))}")
            else:
                typer.echo(f"  {i}. [STREAM] {item.get('title', 'Untitled')}")
                typer.echo(f"     stream_id: {item.get('stream_id')}")
                typer.echo(f"     lab_id: {item.get('lab_id')} | files: {item.get('file_count', 0)}")
            typer.echo()
    else:
        typer.echo(f"No results found for '{query}'")


@app.command("card")
def card(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Show a dataset preview card."""
    client = _client(api_url)
    result = client.get_card(source_id, version=version)
    client.close()

    if not result.get("success"):
        console.print(f"[red]Error:[/red] {result.get('error', 'Unknown error')}")
        raise typer.Exit(1)

    c = result.get("card", {})

    # Title and basic info
    console.print()
    console.print(Panel(
        f"[bold]{c.get('title', 'Untitled')}[/bold]\n"
        f"[dim]{c.get('description', 'No description')}[/dim]",
        title=f"[cyan]{c.get('source_id')}[/cyan] v{c.get('version', '1.0')}",
        border_style="blue",
    ))

    # Metadata table
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
    if c.get("lab_id"):
        table.add_row("Lab ID", c["lab_id"])
    if c.get("keywords"):
        table.add_row("Keywords", ", ".join(c["keywords"]))
    if c.get("methods"):
        table.add_row("Methods", ", ".join(c["methods"]))
    if c.get("doi"):
        table.add_row("DOI", f"https://doi.org/{c['doi']}")
    table.add_row("Status", f"[green]{c.get('status', 'unknown')}[/green]")

    console.print(table)

    # Stats
    stats = c.get("stats", {})
    if stats:
        console.print(f"\n[dim]Files:[/dim] {stats.get('data_sources_count', 0)} sources | Types: {', '.join(stats.get('file_types', []))}")

    console.print()


@app.command("cite")
def cite(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    format: str = typer.Option("apa", "--format", "-f", help="Citation format: bibtex, ris, apa, datacite"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Get citation for a dataset.

    Examples:
        mdf backend cite my_dataset                    # APA format
        mdf backend cite my_dataset -f bibtex         # BibTeX for papers
        mdf backend cite my_dataset -f ris -o ref.ris # Export to file
    """
    client = _client(api_url)
    result = client.get_citation(source_id, format=format, version=version)
    client.close()

    if not result.get("success"):
        console.print(f"[red]Error:[/red] {result.get('error', 'Unknown error')}")
        raise typer.Exit(1)

    # Get the citation text
    citation_text = result.get(format) or result.get("apa", "")

    if output:
        output.write_text(citation_text)
        console.print(f"[green]Citation saved to:[/green] {output}")
    else:
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
