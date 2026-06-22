"""Dataset utility subcommands: cite, open, preview, versions, diff, edit, withdraw, resubmit."""

from __future__ import annotations

import json
import webbrowser
from typing import List, Optional

import typer

from mdf_agent.core.backend_client import BackendClient
from mdf_agent.core.config import resolve_service
from mdf_agent.cli.formatting import (
    api_spinner,
    console,
    format_status_badge,
    read_client,
    require_success,
)
from mdf_agent.cli.preflight import resolve_dataset_identifier

app = typer.Typer(
    help="Dataset utilities (cite, open, preview, versions, ...)",
    no_args_is_help=True,
)


def _resolve_identifier_with_notice(client: BackendClient, raw_id: str) -> str:
    resolved_id = resolve_dataset_identifier(raw_id, client)
    if resolved_id != raw_id:
        console.print(f"[dim]Resolved DOI to source ID:[/dim] {resolved_id}")
    return resolved_id


def _human_size(nbytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def _copy_to_clipboard(text: str) -> None:
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
        mdf dataset cite my_dataset                       # APA format
        mdf dataset cite my_dataset -f bibtex             # BibTeX for papers
        mdf dataset cite my_dataset --copy                # Copy to clipboard
    """
    from rich.panel import Panel

    resolved = resolve_service(service)
    client = read_client(api_url=api_url, token=token, service=resolved, dev_user=dev_user)
    source_id = _resolve_identifier_with_notice(client, source_id)
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

    Examples:
        mdf dataset open my_dataset_v1
        mdf dataset open my_dataset_v1 --url
    """
    resolved = resolve_service(service)
    client = read_client(api_url=api_url, token=token, service=resolved, dev_user=dev_user)
    source_id = _resolve_identifier_with_notice(client, source_id)
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

    Examples:
        mdf dataset preview my_dataset_v1
        mdf dataset preview my_dataset_v1 --sample
    """
    from rich.table import Table

    resolved = resolve_service(service)
    client = read_client(api_url=api_url, token=token, service=resolved, dev_user=dev_user)
    source_id = _resolve_identifier_with_notice(client, source_id)

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
        rows = result.get("rows") or result.get("data", [])
        columns = result.get("columns", [])
        if not rows:
            console.print("[dim]No sample data available.[/dim]")
            return
        if not columns and rows:
            first = rows[0]
            if isinstance(first, dict):
                columns = list(first.keys())
        console.print(f"\n[bold]Sample ({len(rows)} rows):[/bold]\n")
        table = Table(show_header=True, header_style="bold")
        for col in columns[:10]:
            table.add_column(str(col), max_width=30)
        for row in rows[:20]:
            if isinstance(row, dict):
                table.add_row(*[str(row.get(c, ""))[:30] for c in columns[:10]])
            elif isinstance(row, (list, tuple)):
                table.add_row(*[str(v)[:30] for v in row[:10]])
        console.print(table)
        if len(rows) > 20:
            console.print(f"[dim]... and {len(rows) - 20} more rows[/dim]")
    else:
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
        mdf dataset versions my_dataset_v1
    """
    from rich.table import Table

    resolved = resolve_service(service)
    client = read_client(api_url=api_url, token=token, service=resolved, dev_user=dev_user)
    source_id = _resolve_identifier_with_notice(client, source_id)
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
    console.print(f"[dim]Next:[/dim] mdf show {source_id} | mdf dataset diff {source_id} --from 1.0 --to {version_list[-1].get('version', '1.0')}")
    console.print()


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
        mdf dataset diff my_dataset_v1 --from 1.0 --to 2.0
    """
    from rich.table import Table

    resolved = resolve_service(service)
    client = read_client(api_url=api_url, token=token, service=resolved, dev_user=dev_user)
    source_id = _resolve_identifier_with_notice(client, source_id)
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

    Examples:
        mdf dataset edit my_dataset_v1 --title "New Title"
        mdf dataset edit my_dataset_v1 --keyword "XRD" --keyword "diffraction"
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
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Withdraw a pending_curation submission.

    Examples:
        mdf dataset withdraw my_dataset_v1
        mdf dataset withdraw my_dataset_v1 --reason "Duplicate submission"
    """
    import sys
    resolved = resolve_service(service)
    if not yes and sys.stdin.isatty():
        if not typer.confirm(f"Withdraw {source_id}? This removes it from the curation queue.", default=False):
            raise typer.Exit(code=1)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    source_id = _resolve_identifier_with_notice(client, source_id)
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
        mdf dataset resubmit my_dataset_v1 --notes "Fixed title and added authors"
    """
    resolved = resolve_service(service)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    source_id = _resolve_identifier_with_notice(client, source_id)
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
