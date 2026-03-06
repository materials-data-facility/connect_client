"""Admin and curation subcommands: pending, approve, reject, delete, stats."""

from __future__ import annotations

import json
import sys
from typing import Optional

import typer

from mdf_agent.core.backend_client import BackendClient
from mdf_agent.core.config import resolve_service
from mdf_agent.cli.formatting import (
    api_spinner,
    console,
    format_status_badge,
    require_success,
)
from mdf_agent.cli.preflight import resolve_dataset_identifier

app = typer.Typer(
    help="Curation and admin commands",
    no_args_is_help=True,
)


def _resolve_identifier_with_notice(client: BackendClient, raw_id: str) -> str:
    resolved_id = resolve_dataset_identifier(raw_id, client)
    if resolved_id != raw_id:
        console.print(f"[dim]Resolved DOI to source ID:[/dim] {resolved_id}")
    return resolved_id


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
        mdf admin pending
        mdf admin pending --organization argonne
    """
    from rich.table import Table

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
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Approve a dataset for publication.

    Examples:
        mdf admin approve my_dataset_v1
        mdf admin approve my_dataset_v1 --notes "LGTM" --no-mint-doi
    """
    resolved = resolve_service(service)
    if not yes and sys.stdin.isatty():
        if not typer.confirm(f"Approve {source_id}? This will start publication.", default=False):
            raise typer.Exit(code=1)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    source_id = _resolve_identifier_with_notice(client, source_id)
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
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Reject a dataset and return to submitter.

    Examples:
        mdf admin reject my_dataset_v1 --reason "Missing methods section"
    """
    resolved = resolve_service(service)
    if not yes and sys.stdin.isatty():
        if not typer.confirm(f"Reject {source_id}? This will return the submission to the author.", default=False):
            raise typer.Exit(code=1)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    source_id = _resolve_identifier_with_notice(client, source_id)
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


@app.command("delete")
def delete_dataset(
    source_id: str = typer.Argument(..., help="Source ID of dataset to delete"),
    reason: str = typer.Option(..., "--reason", "-r", help="Deletion reason (required)"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Target version"),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Soft-delete a submission (curator-only).

    Examples:
        mdf admin delete my_dataset_v1 --reason "spam"
    """
    resolved = resolve_service(service)
    if not yes and sys.stdin.isatty():
        if not typer.confirm(f"Delete {source_id}? This will soft-delete the submission.", default=False):
            raise typer.Exit(code=1)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    source_id = _resolve_identifier_with_notice(client, source_id)
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

    Examples:
        mdf admin stats
        mdf admin stats --json
    """
    from rich.table import Table

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
