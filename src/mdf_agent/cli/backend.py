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
from mdf_agent.core.config import resolve_service
from mdf_agent.cli.formatting import format_result_or_json, format_status_badge, handle_api_result

app = typer.Typer(help="Interact with MDF backend (v2) API")
console = Console()
_auth_opts: Dict[str, Optional[str]] = {"service": None, "token": None, "dev_user": None}


@app.callback()
def backend_callback(
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """Interact with MDF backend (v2) API."""
    import sys as _sys

    _auth_opts.update(service=service, token=token, dev_user=dev_user)
    print(
        "Hint: try top-level commands like 'mdf cite', 'mdf preview', 'mdf doctor' instead of 'mdf backend ...'",
        file=_sys.stderr,
    )


def _client(api_url: Optional[str]) -> BackendClient:
    return BackendClient.authenticated(
        base_url=api_url,
        token=_auth_opts.get("token"),
        service_instance=resolve_service(_auth_opts.get("service")),
        dev_user_id=_auth_opts.get("dev_user"),
    )


def _print(result):
    typer.echo(json.dumps(result, indent=2))


@app.command("health")
def health(
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.health()
    client.close()
    format_result_or_json(result, json_output, success_msg="Backend is healthy")


@app.command("submit")
def submit(
    payload: Path = typer.Option(..., "--payload", help="Path to submission JSON"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    data = json.loads(payload.read_text(encoding="utf-8"))
    result = client.submit(data)
    client.close()
    if json_output:
        _print(result)
    else:
        if result.get("success"):
            console.print(f"[green]Submitted:[/green] {result.get('source_id')} v{result.get('version')}")
        else:
            handle_api_result(result, error_prefix="Submit failed")


@app.command("status")
def status(
    source_id: str = typer.Option(..., "--source-id"),
    version: Optional[str] = typer.Option(None, "--version"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.status(source_id, version=version)
    client.close()
    if json_output:
        _print(result)
    else:
        sub = result.get("submission") or result
        if sub.get("source_id"):
            console.print(f"[cyan]{sub.get('source_id')}[/cyan] v{sub.get('version', '?')}")
            console.print(f"  Status: {format_status_badge(sub.get('status', 'unknown'))}")
        else:
            handle_api_result(result, error_prefix="Status lookup")


@app.command("submissions")
def submissions(
    organization: Optional[str] = typer.Option(None, "--organization"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.submissions(organization=organization)
    client.close()
    if json_output:
        _print(result)
    else:
        subs = result.get("submissions", [])
        if subs:
            console.print(f"\n[bold]{len(subs)} submission(s):[/bold]")
            for s in subs:
                console.print(f"  {s.get('source_id', '')} v{s.get('version', '?')} — {format_status_badge(s.get('status', ''))}")
        else:
            handle_api_result(result, error_prefix="Submissions")


@app.command("curation-pending")
def curation_pending(
    limit: int = typer.Option(50, "--limit"),
    offset: int = typer.Option(0, "--offset"),
    organization: Optional[str] = typer.Option(None, "--organization"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.curation_pending(limit=limit, offset=offset, organization=organization)
    client.close()
    if json_output:
        _print(result)
    else:
        subs = result.get("submissions", [])
        if subs:
            console.print(f"\n[bold]Pending curation ({len(subs)}):[/bold]")
            for s in subs:
                console.print(f"  {s.get('source_id', '')} — {s.get('title', 'Untitled')}")
        else:
            console.print("[dim]No datasets pending curation.[/dim]")


@app.command("curation-detail")
def curation_detail(
    source_id: str = typer.Option(..., "--source-id"),
    version: Optional[str] = typer.Option(None, "--version"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.curation_detail(source_id, version=version)
    client.close()
    format_result_or_json(result, json_output, error_prefix="Curation detail")


@app.command("curation-approve")
def curation_approve(
    source_id: str = typer.Option(..., "--source-id"),
    mint_doi: bool = typer.Option(True, "--mint-doi/--no-mint-doi"),
    notes: Optional[str] = typer.Option(None, "--notes"),
    metadata_updates: Optional[Path] = typer.Option(
        None,
        "--metadata-updates",
        help="Path to JSON metadata updates",
    ),
    version: Optional[str] = typer.Option(None, "--version"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    metadata_payload = None
    if metadata_updates:
        metadata_payload = json.loads(metadata_updates.read_text(encoding="utf-8"))

    client = _client(api_url)
    result = client.curation_approve(
        source_id=source_id,
        mint_doi=mint_doi,
        notes=notes,
        metadata_updates=metadata_payload,
        version=version,
    )
    client.close()
    format_result_or_json(result, json_output, success_msg=f"Approved: {source_id}", error_prefix="Approve failed")


@app.command("curation-reject")
def curation_reject(
    source_id: str = typer.Option(..., "--source-id"),
    reason: str = typer.Option(..., "--reason"),
    suggestions: Optional[str] = typer.Option(None, "--suggestions"),
    version: Optional[str] = typer.Option(None, "--version"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.curation_reject(
        source_id=source_id,
        reason=reason,
        suggestions=suggestions,
        version=version,
    )
    client.close()
    format_result_or_json(result, json_output, success_msg=f"Rejected: {source_id}", error_prefix="Reject failed")


@app.command("update-status")
def update_status(
    source_id: str = typer.Option(..., "--source-id"),
    version: str = typer.Option(..., "--version"),
    status: str = typer.Option(..., "--status"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.update_status(source_id, version, status)
    client.close()
    format_result_or_json(result, json_output, success_msg=f"Status updated: {source_id} → {status}", error_prefix="Update failed")


@app.command("stream-create")
def stream_create(
    title: str = typer.Option(..., "--title"),
    lab_id: Optional[str] = typer.Option(None, "--lab-id"),
    organization: Optional[str] = typer.Option(None, "--organization"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.stream_create(title, lab_id=lab_id, organization=organization)
    client.close()
    if json_output:
        _print(result)
    else:
        if result.get("success"):
            console.print(f"[green]Stream created:[/green] {result.get('stream_id')}")
        else:
            handle_api_result(result, error_prefix="Stream create failed")


@app.command("stream-append")
def stream_append(
    stream_id: str = typer.Option(..., "--stream-id"),
    files: Optional[Path] = typer.Option(None, "--files", help="JSON list of files or dict with files"),
    file_count: Optional[int] = typer.Option(None, "--file-count"),
    total_bytes: Optional[int] = typer.Option(None, "--total-bytes"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
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
    format_result_or_json(result, json_output, success_msg="Files appended", error_prefix="Append failed")


@app.command("stream-status")
def stream_status(
    stream_id: str = typer.Option(..., "--stream-id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.stream_status(stream_id)
    client.close()
    if json_output:
        _print(result)
    else:
        if result.get("success"):
            s = result
            console.print(f"[cyan]{s.get('stream_id', stream_id)}[/cyan]")
            console.print(f"  Status: {format_status_badge(s.get('status', 'unknown'))}")
            console.print(f"  Files: {s.get('file_count', 0)}")
        else:
            handle_api_result(result, error_prefix="Stream status")


@app.command("stream-close")
def stream_close(
    stream_id: str = typer.Option(..., "--stream-id"),
    mint_doi: Optional[bool] = typer.Option(None, "--mint-doi/--no-mint-doi"),
    title: Optional[str] = typer.Option(None, "--title"),
    description: Optional[str] = typer.Option(None, "--description"),
    authors: Optional[Path] = typer.Option(None, "--authors", help="Path to JSON author list"),
    keywords: Optional[Path] = typer.Option(None, "--keywords", help="Path to JSON keywords list"),
    license: Optional[str] = typer.Option(None, "--license"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    authors_payload = None
    if authors:
        authors_payload = json.loads(authors.read_text(encoding="utf-8"))

    keywords_payload = None
    if keywords:
        keywords_payload = json.loads(keywords.read_text(encoding="utf-8"))

    client = _client(api_url)
    result = client.stream_close(
        stream_id=stream_id,
        mint_doi=mint_doi,
        title=title,
        description=description,
        authors=authors_payload,
        keywords=keywords_payload,
        license=license,
    )
    client.close()
    format_result_or_json(result, json_output, success_msg=f"Stream closed: {stream_id}", error_prefix="Stream close failed")


@app.command("stream-snapshot")
def stream_snapshot(
    stream_id: str = typer.Option(..., "--stream-id"),
    title: Optional[str] = typer.Option(None, "--title"),
    update: bool = typer.Option(False, "--update", help="Update existing dataset if present"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.stream_snapshot(stream_id, title=title, update=update)
    client.close()
    format_result_or_json(result, json_output, success_msg="Snapshot created", error_prefix="Snapshot failed")


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


@app.command("preview")
def preview(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    files: bool = typer.Option(False, "--files", help="List files in profile"),
    sample: bool = typer.Option(False, "--sample", help="Show dataset sample"),
    file_path: Optional[str] = typer.Option(None, "--file-path", help="Show details for a specific profile file"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    if file_path and (files or sample):
        raise typer.BadParameter("--file-path cannot be combined with --files or --sample")
    if files and sample:
        raise typer.BadParameter("--files and --sample cannot be combined")

    client = _client(api_url)
    if file_path:
        result = client.dataset_file_detail(source_id, file_path)
    elif files:
        result = client.dataset_files(source_id)
    elif sample:
        result = client.dataset_sample(source_id)
    else:
        result = client.dataset_preview(source_id)
    client.close()
    _print(result)
