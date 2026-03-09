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
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
import typer.rich_utils as typer_rich_utils
from rich import box as rich_box
from rich.panel import Panel as RichPanel
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
from mdf_agent.cli.dataset_cmd import app as dataset_app
from mdf_agent.cli.admin_cmd import app as admin_app
from mdf_agent.cli.preflight import (
    auth_ready,
    resolve_dataset_identifier,
    run_preflight,
    suggest_data_sources,
)


app = typer.Typer(
    help="MDF Agent — Materials Data Facility",
    add_completion=True,
    invoke_without_command=True,
)

#                            11 visible chars prefix then letters
#                            "···········"
_LOGO = (
    "\n"
    "  [dim].[/dim]    [green]·[/green]        [blue].[/blue]    [dim]·[/dim]    [green]*[/green]          [blue].[/blue]       [dim]·[/dim]        [green].[/green]    [blue]*[/blue]    [dim].[/dim]\n"
    "       [blue]*[/blue]       [dim].[/dim]          [green]·[/green]       [blue].[/blue]            [dim]*[/dim]        [green].[/green]\n"
    "  [green]·[/green]         [bold cyan]__  __  ____   _____[/bold cyan]         [blue]·[/blue]          [dim].[/dim]       [green]*[/green]\n"
    "      [blue].[/blue]    [bold cyan]|  \\/  ||  _ \\ |  ___|[/bold cyan]   [green]*[/green]       [dim].[/dim]              [blue]·[/blue]\n"
    "  [dim]*[/dim]        [bold cyan]| |\\/| || | | || |_[/bold cyan]             [blue].[/blue]     [green]·[/green]        [dim].[/dim]\n"
    "       [green]·[/green]   [bold cyan]| |  | || |_| ||  _|[/bold cyan]    [dim].[/dim]          [blue]*[/blue]          [green].[/green]\n"
    "  [blue].[/blue]        [bold cyan]|_|  |_||____/ |_|[/bold cyan]            [green].[/green]       [dim]·[/dim]         [blue].[/blue]\n"
    "      [dim]*[/dim]          [green]·[/green]      [blue].[/blue]       [dim]·[/dim]     [green].[/green]        [blue]*[/blue]     [dim].[/dim]      [green]·[/green]\n"
    "  [green].[/green]        [bold blue]Materials[/bold blue] [bold green]Data[/bold green] [bold cyan]Facility[/bold cyan]        [dim]·[/dim]          [blue].[/blue]\n"
    "      [blue]·[/blue]    [dim].[/dim]       [green]*[/green]        [blue].[/blue]    [dim]·[/dim]    [green].[/green]       [blue]*[/blue]    [dim].[/dim]       [green]·[/green]\n"
)

PANEL_PUBLISH = "Publish & Download"
PANEL_EXPLORE = "Explore"
PANEL_AUTH = "Auth & Setup"
PANEL_MORE = "More"


def _help_panel(*args, **kwargs):
    kwargs.setdefault("box", rich_box.SIMPLE)
    return RichPanel(*args, **kwargs)


typer_rich_utils.Panel = _help_panel

# Visible sub-apps
app.add_typer(dataset_app, name="dataset", rich_help_panel=PANEL_MORE)
app.add_typer(admin_app, name="admin", rich_help_panel=PANEL_MORE)
app.add_typer(config_app, name="config", rich_help_panel=PANEL_MORE)

# Hidden sub-apps
app.add_typer(backend_app, name="backend", hidden=True)
app.add_typer(stream_app, name="stream", hidden=True)


# ---------------------------------------------------------------------------
# Main callback — first-run welcome or help
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main_callback(ctx: typer.Context):
    """MDF Agent — Materials Data Facility"""
    if ctx.invoked_subcommand is not None:
        return

    # First-run: no config file yet
    cfg_path = Path.home() / ".config" / "mdf_agent" / "config.json"
    if not cfg_path.exists():
        console.print(_LOGO)
        console.print(Panel(
            f"[bold]MDF Agent[/bold] v{__version__}\n"
            "\n"
            "Get started:\n"
            "  [cyan]mdf setup[/cyan]                 Configure defaults and create a manifest\n"
            "  [cyan]mdf publish --submit[/cyan]      Publish to MDF Connect\n"
            '  [cyan]mdf search "perovskite"[/cyan]   Find datasets\n'
            "  [cyan]mdf login[/cyan]                 Authenticate with Globus\n"
            "\n"
            "Run [bold]mdf --help[/bold] for all commands.",
            border_style="blue",
        ))
    else:
        # Show help
        console.print(_LOGO)
        console.print(ctx.get_help())


# ---------------------------------------------------------------------------
# Shared CLI helpers
# ---------------------------------------------------------------------------


def _render_preflight(preflight: Dict[str, Any], title: str = "Preflight") -> None:
    summary = preflight.get("source_summary", {})
    console.print()
    console.print(Panel(
        f"[bold]{title}[/bold]\n"
        f"[dim]Service:[/dim] {preflight.get('service')}\n"
        f"[dim]Files:[/dim] {summary.get('files', 0)}\n"
        f"[dim]Bytes:[/dim] {_human_size(summary.get('bytes', 0))}\n"
        f"[dim]Target:[/dim] {preflight.get('target_url') or 'default service URL'}",
        border_style="blue" if preflight.get("success") else "yellow",
    ))

    sources = preflight.get("sources", [])
    if sources:
        table = Table(show_header=True, header_style="bold")
        table.add_column("Source")
        table.add_column("Kind", style="dim")
        table.add_column("Files", justify="right")
        table.add_column("Bytes", justify="right")
        for source in sources:
            files = source.get("file_count")
            bytes_value = source.get("total_bytes")
            table.add_row(
                source.get("source", ""),
                source.get("kind", ""),
                "-" if files is None else str(files),
                "-" if bytes_value is None else _human_size(bytes_value),
            )
        console.print(table)

    issues = preflight.get("issues", [])
    if issues:
        console.print()
        for issue in issues:
            severity = issue.get("severity", "info")
            if severity == "blocking":
                prefix = "[red]x[/red]"
            elif severity == "warning":
                prefix = "[yellow]![/yellow]"
            else:
                prefix = "[cyan]-[/cyan]"
            console.print(f"{prefix} {issue.get('message')}")
            if issue.get("hint"):
                console.print(f"    [dim]{issue['hint']}[/dim]")
    else:
        console.print("[green]No preflight issues detected.[/green]")


def _resolve_identifier_with_notice(client: BackendClient, raw_id: str) -> str:
    resolved_id = resolve_dataset_identifier(raw_id, client)
    if resolved_id != raw_id:
        console.print(f"[dim]Resolved DOI to source ID:[/dim] {resolved_id}")
    return resolved_id


def _extract_feedback_fields(record: Dict[str, Any]) -> List[tuple[str, str]]:
    feedback: List[tuple[str, str]] = []
    direct_candidates = [
        ("Reason", record.get("rejection_reason") or record.get("reason")),
        ("Suggestions", record.get("suggestions")),
        ("Curator notes", record.get("notes")),
    ]
    for label, value in direct_candidates:
        if value:
            feedback.append((label, str(value)))

    history = record.get("curation_history")
    if isinstance(history, str):
        try:
            history = json.loads(history)
        except Exception:
            history = []
    if isinstance(history, list):
        for item in reversed(history):
            if not isinstance(item, dict):
                continue
            for label, key in (("Reason", "reason"), ("Suggestions", "suggestions"), ("Curator notes", "notes")):
                value = item.get(key)
                if value and (label, str(value)) not in feedback:
                    feedback.append((label, str(value)))
            if feedback:
                break
    return feedback


def _render_status_details(result: Dict[str, Any], lookup_id: str) -> None:
    sub = result.get("submission") or result
    sid = sub.get("source_id")
    if sid:
        ver = sub.get("version", "?")
        st = sub.get("status", "unknown")
        console.print(f"\n[bold]Backend status:[/bold] [cyan]{sid}[/cyan] v{ver}")
        console.print(f"  [dim]Status:[/dim] {format_status_badge(st)}")
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

        for label, value in _extract_feedback_fields(sub):
            console.print(f"  [dim]{label}:[/dim] {value}")

        if st == "pending_curation":
            console.print("  [dim]Next:[/dim] Waiting for curation review")
            console.print(f"  [dim]Try:[/dim] mdf status --watch {sid}")
        elif st == "approved":
            console.print("  [dim]Next:[/dim] Publishing in progress...")
            console.print(f"  [dim]Try:[/dim] mdf status --watch {sid}")
        elif st == "published":
            console.print("  [dim]Next:[/dim] Dataset is live!")
            console.print(f"  [dim]Try:[/dim] mdf show {sid} | mdf dataset cite {sid} | mdf dataset open {sid}")
        elif st == "rejected":
            console.print("  [dim]Next:[/dim] Review feedback, edit metadata if needed, then resubmit")
            console.print(f"  [dim]Try:[/dim] mdf dataset edit {sid} --title \"Updated title\" | mdf dataset resubmit {sid}")
        elif st == "withdrawn":
            console.print("  [dim]Next:[/dim] Update the submission, then resubmit if appropriate")
        elif st == "deleted":
            console.print("  [dim]Next:[/dim] This record is no longer active")
    elif result.get("error"):
        console.print(f"\n[yellow]Backend:[/yellow] {result.get('error')}")
    else:
        console.print(f"\n[dim]No backend record for {lookup_id}[/dim]")


def _watch_submission(
    lookup_id: str,
    interval: int,
    timeout: int,
    service: str,
    api_url: Optional[str],
    token: Optional[str],
    dev_user: Optional[str],
    json_output: bool = False,
    announce: bool = True,
    stop_on_statuses: Optional[set[str]] = None,
) -> int:
    terminal_states = {"published", "rejected", "failed", "deleted", "withdrawn"}
    start = time.monotonic()

    if announce and not json_output:
        console.print(f"\n[bold]Watching[/bold] [cyan]{lookup_id}[/cyan] (poll every {interval}s, timeout {timeout}s)\n")

    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            if json_output:
                print(json.dumps({"source_id": lookup_id, "status": "timeout", "elapsed": round(elapsed)}))
            else:
                console.print(f"\n[yellow]Timeout after {timeout}s[/yellow]")
                console.print(f"[dim]Next:[/dim] mdf status {lookup_id}")
            return 1

        try:
            client = BackendClient.authenticated(
                base_url=api_url,
                token=token,
                service_instance=service,
                dev_user_id=dev_user,
            )
            result = client.status(lookup_id)
            client.close()
        except Exception as exc:
            if json_output:
                print(json.dumps({"source_id": lookup_id, "error": str(exc), "elapsed": round(elapsed)}))
            else:
                console.print(f"  [yellow]Error:[/yellow] {exc}")
            time.sleep(interval)
            continue

        sub = result.get("submission") or result
        st = sub.get("status", "unknown")

        if json_output:
            print(json.dumps({"source_id": lookup_id, "status": st, "elapsed": round(elapsed)}))
        else:
            console.print(f"  {format_status_badge(st)}  [dim]({round(elapsed)}s)[/dim]")

        if stop_on_statuses and st in stop_on_statuses:
            if not json_output:
                console.print()
                _render_status_details(result, lookup_id)
            return 0

        if st in terminal_states:
            if json_output:
                return 0 if st == "published" else 1
            console.print()
            _render_status_details(result, lookup_id)
            if st == "published":
                console.print("\n[bold green]Done![/bold green]")
                return 0
            console.print(f"\n[red]Terminal state: {st}[/red]")
            return 1

        time.sleep(interval)


def _maybe_confirm(action: str, target: str, yes: bool = False, detail: Optional[str] = None) -> None:
    if yes or not sys.stdin.isatty():
        return
    message = f"{action} {target}?"
    if detail:
        message = f"{message} {detail}"
    if not typer.confirm(message, default=False):
        raise typer.Exit(code=1)


def _clone_method_label(method: str) -> str:
    return {
        "zip": "Archive download",
        "https": "Direct file download",
        "transfer": "Globus Transfer",
    }.get(method, method)


def _render_clone_plan(plan: Dict[str, Any]) -> None:
    lines = [
        f"[dim]Dataset:[/dim] {plan.get('resolved_source_id')}",
        f"[dim]Title:[/dim] {plan.get('title') or 'Untitled'}",
        f"[dim]Destination:[/dim] {plan.get('output_path')}",
        f"[dim]Method:[/dim] {_clone_method_label(plan.get('selected_method', ''))}",
    ]
    if plan.get("version"):
        lines.append(f"[dim]Version:[/dim] {plan['version']}")
    if plan.get("requested_identifier") != plan.get("resolved_source_id"):
        lines.append(f"[dim]Requested:[/dim] {plan.get('requested_identifier')}")
    if plan.get("archive_available") and plan.get("archive_size"):
        lines.append(f"[dim]Archive:[/dim] {_human_size(plan['archive_size'])}")
    elif plan.get("file_count"):
        lines.append(f"[dim]Files:[/dim] {plan['file_count']}")
    console.print()
    console.print(Panel("\n".join(lines), title="Clone plan", border_style="blue"))


def _render_clone_result(result: Dict[str, Any], derive: bool = False) -> None:
    if result.get("success"):
        queued_transfer = result.get("method") == "transfer" and result.get("task_id")
        heading = "Transfer queued!" if queued_transfer else "Clone complete!"
        console.print(f"\n[bold green]{heading}[/bold green]")
        if result.get("title"):
            console.print(f"  [dim]Title:[/dim] {result['title']}")
        console.print(f"  [dim]Method:[/dim] {_clone_method_label(result.get('method', ''))}")
        if result.get("files_count") is not None:
            console.print(f"  [dim]Files:[/dim] {result['files_count']}")
        console.print(f"  [dim]Path:[/dim] {result['path']}")
        if queued_transfer:
            console.print("  [dim]Status:[/dim] Waiting on Globus Transfer to finish")
            console.print(f"  [dim]Transfer task:[/dim] {result['task_id']}")
            console.print(f"  [dim]Monitor:[/dim] {result['monitor_url']}")
        else:
            source_id = result.get("resolved_source_id") or result.get("source_id")
            if source_id:
                console.print(f"  [dim]Next:[/dim] cd {result['path']} | mdf show {source_id}")

        if derive:
            console.print("  [green]Created mdf.yaml with derived-from lineage[/green]")
        console.print()
        return

    downloaded = result.get("files_count", 0)
    failed = result.get("failed_count")
    if result.get("partial") and result.get("errors"):
        console.print("\n[bold yellow]Clone partially completed[/bold yellow]")
        console.print(f"  [dim]Downloaded:[/dim] {downloaded}")
        if failed is not None:
            console.print(f"  [dim]Failed:[/dim] {failed}")
        console.print(f"  [dim]Path:[/dim] {result.get('path')}")
        console.print("  [dim]Retry:[/dim] Re-run `mdf clone ...` into the same destination or a clean directory.")
        console.print("\n[bold]Failed files[/bold]")
        for err in result.get("errors", []):
            console.print(f"  [red]x[/red] {err}")
        console.print()
        raise typer.Exit(code=1)

    if result.get("error"):
        console.print(f"\n[red]Clone failed:[/red] {result['error']}")
    elif result.get("errors"):
        console.print("\n[red]Clone failed[/red]")
        for err in result["errors"]:
            console.print(f"  [red]x[/red] {err}")
    else:
        console.print("\n[red]Clone failed[/red]")
    raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# mdf setup — guided onboarding
# ---------------------------------------------------------------------------


@app.command(rich_help_panel=PANEL_AUTH)
def setup(
    path: str = typer.Argument(".", help="Dataset directory to initialize"),
):
    """Configure defaults and create a manifest."""
    from mdf_agent.cli.manifest_cmd import manifest_init

    root = Path(path).resolve()
    root.mkdir(parents=True, exist_ok=True)

    console.print(f"\n[bold]MDF Agent setup[/bold] v{__version__}\n")

    cfg = GlobalConfig()
    service_default = cfg.service or "staging"
    service = typer.prompt(
        "Default service",
        default=service_default,
        show_default=True,
    ).strip().lower()
    if service:
        cfg.set("defaults.service", service)

    organization = typer.prompt(
        "Organization (optional)",
        default=cfg.organization or "",
        show_default=bool(cfg.organization),
    ).strip()
    if organization:
        cfg.set("user.organization", organization)

    publisher = typer.prompt(
        "Publisher (optional)",
        default=cfg.publisher or "",
        show_default=bool(cfg.publisher),
    ).strip()
    if publisher:
        cfg.set("user.publisher", publisher)

    email_default = cfg.get("user.email") or ""
    email = typer.prompt(
        "Contact email (optional)",
        default=email_default,
        show_default=bool(email_default),
    ).strip()
    if email:
        cfg.set("user.email", email)

    authed = auth_ready(service)
    if authed:
        console.print(f"[green]Auth ready for {service}[/green]")
    else:
        console.print(f"[yellow]Not authenticated for {service}[/yellow]")
        console.print(f"[dim]Next:[/dim] mdf login --service {service}")

    manifest_path = root / "mdf.yaml"
    if manifest_path.exists():
        console.print(f"[green]Found existing manifest:[/green] {manifest_path}")
    else:
        suggestions = suggest_data_sources(root)
        console.print(f"[dim]Dataset directory:[/dim] {root}")
        if suggestions:
            console.print(f"[dim]Detected data candidates:[/dim] {', '.join(suggestions)}")
        if typer.confirm("Create mdf.yaml in this directory?", default=True):
            manifest_init(
                path=str(root),
                title=None,
                author=None,
                description=None,
                publisher=publisher or None,
                publication_year=None,
                organization=organization or None,
                keyword=None,
            )

    console.print("\n[bold]Next steps[/bold]")
    if not authed:
        console.print(f"  [cyan]mdf login --service {service}[/cyan]")
    if manifest_path.exists() or (root / "mdf.yaml").exists():
        console.print("  [cyan]mdf config manifest inspect[/cyan]")
        console.print("  [cyan]mdf publish --preflight-only[/cyan]")
        console.print("  [cyan]mdf publish --submit[/cyan]")
    else:
        console.print("  [cyan]mdf setup[/cyan]")


# ---------------------------------------------------------------------------
# Auth commands
# ---------------------------------------------------------------------------

@app.command(rich_help_panel=PANEL_AUTH)
def login(
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev)"),
    token: Optional[str] = typer.Option(None, "--token", help="Use an explicit access token"),
):
    """Authenticate with Globus."""
    from mdf_agent.auth.globus import (
        DEFAULT_TOKEN_PATH,
        DATA_MDF_SCOPE,
        GROUPS_SCOPE,
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
        get_authorizer_for_scopes([scope, DATA_MDF_SCOPE, TRANSFER_SCOPE, GROUPS_SCOPE])
    console.print("[green]Authentication ready[/green]")
    console.print(f"[dim]Token store:[/dim] {DEFAULT_TOKEN_PATH}")
    if token:
        console.print("[dim]Using token from --token for this invocation.[/dim]")


@app.command(rich_help_panel=PANEL_AUTH)
def logout():
    """Clear cached credentials."""
    from mdf_agent.auth.globus import DEFAULT_TOKEN_PATH, logout as clear_cached_tokens

    removed = clear_cached_tokens()
    if removed:
        console.print("[green]Logged out[/green]")
    else:
        console.print(f"[yellow]No cached token file found[/yellow] ({DEFAULT_TOKEN_PATH})")


# ---------------------------------------------------------------------------
# Status — now absorbs whoami (--auth) and watch (--watch)
# ---------------------------------------------------------------------------

@app.command(rich_help_panel=PANEL_AUTH)
def status(
    source_id: Optional[str] = typer.Argument(None, help="Source ID to check (default: last published)"),
    auth: bool = typer.Option(False, "--auth", help="Show authentication/identity info"),
    watch: bool = typer.Option(False, "--watch", "-w", help="Poll until terminal state"),
    interval: int = typer.Option(10, "--interval", "-i", help="Poll interval in seconds (with --watch)"),
    timeout: int = typer.Option(1800, "--timeout", help="Timeout in seconds (with --watch)"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Dataset version"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Check submission or auth status.

    With source_id: shows backend status for that dataset.
    Without: shows backend status of last published dataset.
    With --auth: shows authentication/identity info.
    With --watch: polls until terminal state.
    """
    resolved = resolve_service(service)

    # --auth mode: show identity info (absorbs old whoami)
    if auth:
        from mdf_agent.auth.globus import DEFAULT_TOKEN_PATH, is_logged_in

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
        return

    lookup_id = source_id
    if not lookup_id:
        cfg = GlobalConfig()
        lookup_id = cfg.last_source_id
        if not lookup_id:
            console.print("\n[dim]No source_id provided and no last published dataset.[/dim]")
            console.print("[dim]Usage: mdf status <source_id>[/dim]")
            return

    # --watch mode: poll until terminal state (absorbs old watch)
    if watch:
        client = BackendClient.authenticated(
            base_url=api_url,
            token=token,
            service_instance=resolved,
            dev_user_id=dev_user,
        )
        lookup_id = _resolve_identifier_with_notice(client, lookup_id)
        client.close()

        raise typer.Exit(
            code=_watch_submission(
                lookup_id,
                interval=interval,
                timeout=timeout,
                service=resolved,
                api_url=api_url,
                token=token,
                dev_user=dev_user,
                json_output=json_output,
                announce=True,
            )
        )

    # Default: show status
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    lookup_id = _resolve_identifier_with_notice(client, lookup_id)
    with api_spinner("Checking status..."):
        result = client.status(lookup_id, version=version)
    client.close()

    if json_output:
        print(json.dumps(result, indent=2))
        return

    _render_status_details(result, lookup_id)


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

@app.command(rich_help_panel=PANEL_PUBLISH)
def publish(
    data: Optional[List[str]] = typer.Argument(None, help="Data paths/URIs to publish"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Dataset title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit", help="Preview without submitting"),
    test: bool = typer.Option(False, "--test", help="Submit to test environment"),
    update: bool = typer.Option(False, "--update", "-u", help="Update existing dataset"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    preflight_only: bool = typer.Option(False, "--preflight-only", help="Run submit-time checks without uploading"),
    no_watch: bool = typer.Option(False, "--no-watch", help="Do not watch after a successful submit"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL for local backend"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """Publish a dataset to MDF Connect.

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
        console.print("[dim]Manifest mode:[/dim]  mdf setup")
        console.print("[dim]                 mdf publish --submit[/dim]")
        raise typer.Exit(code=1)

    preflight = run_preflight(
        agent.manifest,
        root=agent.root or Path.cwd(),
        service=resolved,
        api_url=api_url or _api_url_for_service(resolved),
        token=token,
        dev_user=dev_user,
        submit=preflight_only or not dry_run,
    )

    if preflight_only:
        if json_output:
            print(json.dumps(preflight, indent=2))
        else:
            _render_preflight(preflight)
        if not preflight.get("success"):
            raise typer.Exit(code=1)
        return

    if not preflight.get("success"):
        if json_output:
            print(json.dumps({"success": False, "preflight": preflight}, indent=2))
        else:
            _render_preflight(preflight)
            console.print("\n[red]Publish blocked by preflight issues[/red]")
        raise typer.Exit(code=1)

    payload = agent.build_submission(test=test, update=update)

    if dry_run:
        if json_output:
            print(json.dumps({"success": True, "dry_run": True, "payload": payload, "preflight": preflight}, indent=2))
            return
        _render_preflight(preflight, title="Publish preflight")
        console.print("\n[bold cyan]Dry run - would submit:[/bold cyan]")
        target = api_url or _api_url_for_service(resolved)
        console.print(f"[dim]Target: {target} ({resolved})[/dim]")
        syntax = Syntax(json.dumps(payload, indent=2), "json", theme="monokai")
        console.print(syntax)
        console.print("\n[bold]Ready to publish. Run:[/bold]")
        console.print("  [cyan]mdf publish --submit[/cyan]")
        return

    # Build rich progress display matching the clone UI:
    # two-tier view with overall file count + per-file byte-level progress
    import threading as _threading
    from rich.live import Live
    from rich.console import Group
    from rich.progress import (
        BarColumn, DownloadColumn, MofNCompleteColumn,
        Progress, SpinnerColumn, TextColumn, TransferSpeedColumn,
    )

    progress_callback = None
    on_files_resolved_cb = None
    on_file_done_cb = None
    live_ctx = None

    if sys.stderr.isatty():
        overall_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            console=console,
        )
        file_progress = Progress(
            SpinnerColumn("dots2"),
            TextColumn("[dim]{task.description}[/dim]"),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        )

        overall_task_id = None
        file_tasks: dict = {}
        file_lock = _threading.Lock()

        def on_files_resolved_cb(count: int) -> None:
            nonlocal overall_task_id
            overall_task_id = overall_progress.add_task(
                "[cyan]Uploading[/cyan]", total=count
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
            filename = Path(rel_path).name
            with file_lock:
                tid = file_tasks.pop(rel_path, None)
                if tid is not None:
                    file_progress.update(
                        tid, description=f"[dim green]✓ {filename}[/dim green]"
                    )
            if overall_task_id is not None:
                overall_progress.advance(overall_task_id, 1)
                task = overall_progress.tasks[overall_task_id]
                if task.completed >= task.total:
                    overall_progress.update(
                        overall_task_id,
                        description="[bold green]Upload complete[/bold green]",
                    )
            if tid is not None:
                def _remove(task_id=tid):
                    import time as _time
                    _time.sleep(0.4)
                    try:
                        file_progress.remove_task(task_id)
                    except Exception:
                        pass
                _threading.Thread(target=_remove, daemon=True).start()

        live_ctx = Live(
            Group(overall_progress, file_progress),
            console=console,
            refresh_per_second=15,
        )
        live_ctx.start()

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
            on_files_resolved=on_files_resolved_cb,
            on_file_done=on_file_done_cb,
        )
    except RuntimeError as exc:
        if live_ctx is not None:
            live_ctx.stop()
        console.print(f"\n[red]Upload failed:[/red] {exc}")
        raise typer.Exit(code=1)
    finally:
        if live_ctx is not None:
            live_ctx.stop()

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
        console.print(f"  [dim]Service:[/dim] {resolved}")
        console.print("  [dim]Status:[/dim] pending_curation")
        if version_val:
            console.print(f"  [dim]Version:[/dim] [cyan]{version_val}[/cyan]")

        if source_id_val:
            cfg = GlobalConfig()
            cfg.record_publish(source_id_val, version_val, resolved)
            console.print(f"  [dim]Try:[/dim] mdf status {source_id_val} | mdf show {source_id_val}")
            if no_watch:
                console.print(f"  [dim]Watch:[/dim] mdf status --watch {source_id_val}")
            else:
                raise typer.Exit(
                    code=_watch_submission(
                        source_id_val,
                        interval=5,
                        timeout=1800,
                        service=resolved,
                        api_url=api_url,
                        token=token,
                        dev_user=dev_user,
                        announce=True,
                        stop_on_statuses={"pending_curation"},
                    )
                )
    else:
        require_success(result, error_prefix="Publish failed")


# ---------------------------------------------------------------------------
# Clone
# ---------------------------------------------------------------------------

@app.command(rich_help_panel=PANEL_PUBLISH)
def clone(
    source_id: str = typer.Argument(..., help="Source dataset ID or DOI to download"),
    output_dir: Optional[str] = typer.Argument(None, help="Output directory (default: ./{source_id})"),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific version to clone"),
    transfer: bool = typer.Option(False, "--transfer", help="Use Globus Transfer (requires GCP)"),
    derive: bool = typer.Option(False, "--derive", help="Create mdf.yaml with derived-from lineage"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Download a dataset from MDF.

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

    # Default output dir: ./{source_id} — use the last path component of a DOI
    if output_dir is None:
        dir_name = source_id.rstrip("/").split("/")[-1]
        output_dir = dir_name

    plan = agent.plan_clone(
        source_id=source_id,
        output_dir=output_dir,
        version=version,
        method=method,
        api_url=api_url,
        token=resolved_token,
        service_instance=resolved,
        dev_user_id=resolved_dev_user,
    )

    if json_output:
        if not plan.get("success"):
            print(json.dumps(plan, indent=2))
            raise typer.Exit(code=1)
    else:
        if not plan.get("success"):
            _render_clone_result(plan)
        console.print(f"\n[bold]Cloning[/bold] [cyan]{plan.get('resolved_source_id', source_id)}[/cyan]", end="")
        if version:
            console.print(f" [dim]v{version}[/dim]", end="")
        console.print()
        _render_clone_plan(plan)

    progress_callback = None
    on_files_resolved_cb = None
    on_file_done_cb = None
    stage_callback = None
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
            if overall_task_id is None:
                overall_task_id = overall_progress.add_task(
                    f"[cyan]{plan.get('resolved_source_id', source_id)}[/cyan]", total=max(count, 1)
                )
            else:
                overall_progress.update(overall_task_id, total=max(count, 1), completed=0)

        def progress_callback(rel_path: str, bytes_sent: int, total_bytes: int) -> None:
            filename = "dataset archive" if rel_path.endswith(".zip") else Path(rel_path).name
            with file_lock:
                if rel_path not in file_tasks:
                    file_tasks[rel_path] = file_progress.add_task(
                        filename, total=max(total_bytes, 1)
                    )
                file_progress.update(file_tasks[rel_path], completed=bytes_sent)

        def stage_callback(stage: str, info: Dict[str, Any]) -> None:
            nonlocal overall_task_id
            labels = {
                "resolving_dataset": "Resolving dataset",
                "downloading_archive": "Downloading archive",
                "extracting_archive": "Extracting archive",
                "downloading_files": "Downloading files",
                "queueing_transfer": "Queueing Globus Transfer",
                "transfer_queued": "Transfer queued",
                "falling_back_to_https": "Archive unavailable, switching to direct file download",
            }
            label = labels.get(stage, stage.replace("_", " "))
            if overall_task_id is None:
                overall_task_id = overall_progress.add_task(label, total=max(info.get("files_count", 1), 1))
            else:
                total = max(info.get("files_count", total_file_count or 1), 1)
                overall_progress.update(overall_task_id, description=label, total=total, completed=0)

        def on_file_done_cb(rel_path: str, _size: int) -> None:
            filename = "dataset archive" if rel_path.endswith(".zip") else Path(rel_path).name
            with file_lock:
                tid = file_tasks.pop(rel_path, None)
                if tid is not None:
                    file_progress.update(
                        tid, description=f"[dim green]✓ {filename}[/dim green]"
                    )
            if overall_task_id is not None:
                overall_progress.advance(overall_task_id, 1)
                task = overall_progress.tasks[overall_task_id]
                if task.completed >= task.total:
                    overall_progress.update(
                        overall_task_id,
                        description=f"[bold green]{plan.get('resolved_source_id', source_id)}[/bold green]",
                    )
            # Remove the ✓ row after a short delay so it's visible for at least a few frames
            if tid is not None:
                def _remove(task_id=tid):
                    import time as _time
                    _time.sleep(0.4)
                    try:
                        file_progress.remove_task(task_id)
                    except Exception:
                        pass
                threading.Thread(target=_remove, daemon=True).start()

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
                stage_callback=stage_callback,
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
        payload = {"plan": plan, "result": result}
        print(json.dumps(payload, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if derive:
        from mdf_agent.models.config import DerivedFrom

        derive_root = Path(output_dir).resolve()
        derive_agent = MDFAgent.init_manifest(
            path=str(derive_root),
            title=f"Derived from {result.get('resolved_source_id') or source_id}",
            authors=["Unknown"],
        )
        derive_agent.manifest.derived_from = [
            DerivedFrom(source_id=result.get("resolved_source_id") or source_id, relationship="derived")
        ]
        derive_agent.save_manifest()
    _render_clone_result(result, derive=derive)


# ---------------------------------------------------------------------------
# Explore commands
# ---------------------------------------------------------------------------

@app.command("search", rich_help_panel=PANEL_EXPLORE)
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
    """Search public datasets and streams.

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
        table.add_column("DOI", style="dim")

        for i, item in enumerate(result["results"], 1):
            if item.get("type") == "dataset":
                table.add_row(
                    str(i),
                    "[blue]dataset[/blue]",
                    item.get("title", "Untitled"),
                    f"{item.get('source_id')} v{item.get('version')}",
                    format_status_badge(item.get("status", "")),
                    item.get("doi", ""),
                )
            else:
                table.add_row(
                    str(i),
                    "[green]stream[/green]",
                    item.get("title", "Untitled"),
                    item.get("stream_id", ""),
                    f"{item.get('file_count', 0)} files",
                    "",
                )

        console.print(table)
        if result["results"]:
            first = result["results"][0]
            if first.get("source_id"):
                console.print(f"\n[dim]Next:[/dim] mdf show {first['source_id']}")
    else:
        console.print(f"\n[dim]No results found for '{query}'[/dim]")


@app.command("list", rich_help_panel=PANEL_EXPLORE)
def list_datasets(
    limit: int = typer.Option(50, "--limit", "-n", help="Max results"),
    status_filter: Optional[str] = typer.Option(None, "--status", help="Filter by status"),
    search: Optional[str] = typer.Option(None, "--search", help="Filter by title, source ID, or DOI"),
    latest_only: bool = typer.Option(False, "--latest-only", help="Only show latest versions"),
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
    filtered_submissions = []
    query = (search or "").strip().lower()
    for sub in submissions:
        mdata = sub.get("dataset_mdata", {})
        if isinstance(mdata, str):
            try:
                mdata = json.loads(mdata)
            except Exception:
                mdata = {}
        status_value = sub.get("status", "")
        if status_filter and status_value != status_filter:
            continue
        if latest_only and isinstance(mdata, dict) and mdata.get("latest") is False:
            continue
        if query:
            haystack = " ".join(
                str(value or "")
                for value in (
                    sub.get("source_id"),
                    sub.get("doi"),
                    sub.get("dataset_doi"),
                    sub.get("title"),
                    mdata.get("title") if isinstance(mdata, dict) else "",
                )
            ).lower()
            if query not in haystack:
                continue
        filtered_submissions.append((sub, mdata))

    if not filtered_submissions:
        console.print("\n[dim]No datasets found.[/dim]")
        return

    console.print(f"\n[bold]Your datasets ({len(filtered_submissions)}):[/bold]\n")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Source ID", no_wrap=True)
    table.add_column("Title", max_width=40)
    table.add_column("Version", style="dim", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Updated", style="dim", no_wrap=True)

    for sub, mdata in filtered_submissions[:limit]:
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


@app.command(rich_help_panel=PANEL_EXPLORE)
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
    """Show dataset details.

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
    source_id = _resolve_identifier_with_notice(client, source_id)
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

    console.print(f"\n[dim]Next:[/dim] mdf dataset open {source_id} | mdf dataset cite {source_id} | mdf dataset preview {source_id}")
    console.print()


# ---------------------------------------------------------------------------
# Hidden aliases — old top-level commands that now live in sub-apps
# ---------------------------------------------------------------------------

@app.command(hidden=True)
def init(
    path: str = typer.Argument(".", help="Directory to create mdf.yaml in"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Dataset title"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a", help="Author name (repeatable)"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Dataset description"),
    publisher: Optional[str] = typer.Option(None, "--publisher", help="Dataset publisher"),
    publication_year: Optional[int] = typer.Option(None, "--year", "-y", help="Publication year"),
):
    """Create an mdf.yaml manifest for a dataset directory."""
    from mdf_agent.cli.manifest_cmd import manifest_init
    manifest_init(
        path=path,
        title=title,
        author=author,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )


@app.command(hidden=True)
def validate(
    data: Optional[List[str]] = typer.Argument(None, help="Data paths to validate (optional)"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance for context"),
):
    """Validate manifest before publishing."""
    manifest_path = Path.cwd() / "mdf.yaml"
    if manifest_path.exists():
        agent = MDFAgent.from_manifest(".")
    elif data:
        from mdf_agent.models.config import ManifestConfig
        agent = MDFAgent(manifest=ManifestConfig(data_sources=list(data)))
        console.print("[yellow]No mdf.yaml found — validating data paths only[/yellow]")
    else:
        console.print("[red]No mdf.yaml found and no data paths provided[/red]")
        console.print("[dim]Run [/dim][cyan]mdf setup[/cyan][dim] to create one, or pass data paths.[/dim]")
        raise typer.Exit(code=1)

    root = agent.root or Path.cwd()
    preflight = run_preflight(
        agent.manifest,
        root=root,
        service=resolve_service(service),
        submit=False,
    )
    issues = preflight.get("issues", [])
    errors = [issue["message"] for issue in issues if issue["severity"] == "blocking"]
    warnings = [issue["message"] for issue in issues if issue["severity"] == "warning"]

    if json_output:
        print(json.dumps({"success": not errors, "errors": errors, "warnings": warnings, "preflight": preflight}, indent=2))
        if errors:
            raise typer.Exit(code=1)
        return

    _render_preflight(preflight, title="Validation")

    if errors:
        console.print("\n[red]Validation failed[/red]")
        raise typer.Exit(code=1)

    console.print("\n[bold green]Validation passed[/bold green]")


@app.command(hidden=True)
def whoami(
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
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


@app.command("watch", hidden=True)
def watch_cmd(
    source_id: Optional[str] = typer.Argument(None, help="Source ID (default: last published)"),
    interval: int = typer.Option(10, "--interval", "-i", help="Poll interval in seconds"),
    timeout: int = typer.Option(1800, "--timeout", help="Timeout in seconds"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Watch a dataset until it reaches a terminal state."""
    resolved = resolve_service(service)

    lookup_id = source_id
    if not lookup_id:
        cfg = GlobalConfig()
        lookup_id = cfg.last_source_id
        if not lookup_id:
            console.print("[red]No source_id provided and no last published dataset.[/red]")
            console.print("[dim]Usage: mdf status --watch <source_id>[/dim]")
            raise typer.Exit(code=1)
    client = BackendClient.authenticated(
        base_url=api_url,
        token=token,
        service_instance=resolved,
        dev_user_id=dev_user,
    )
    lookup_id = _resolve_identifier_with_notice(client, lookup_id)
    client.close()

    raise typer.Exit(
        code=_watch_submission(
            lookup_id,
            interval=interval,
            timeout=timeout,
            service=resolved,
            api_url=api_url,
            token=token,
            dev_user=dev_user,
            json_output=json_output,
            announce=True,
        )
    )


@app.command(hidden=True)
def doctor(
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
):
    """Run diagnostics on your MDF Agent setup."""
    from mdf_agent.cli.config_cmd import doctor as config_doctor
    config_doctor(json_output=json_output, service=service, api_url=api_url)


# Hidden aliases for commands moved to dataset sub-app
@app.command(hidden=True, name="cite")
def cite_alias(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    format: str = typer.Option("apa", "--format", "-f", help="Citation format"),
    copy: bool = typer.Option(False, "--copy", help="Copy to clipboard"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Get citation for a dataset."""
    from mdf_agent.cli.dataset_cmd import cite
    cite(source_id=source_id, format=format, copy=copy, version=version,
         json_output=json_output, service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command("open", hidden=True)
def open_alias(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    url: bool = typer.Option(False, "--url"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Open a dataset in the browser."""
    from mdf_agent.cli.dataset_cmd import open_dataset
    open_dataset(source_id=source_id, url=url, version=version,
                 service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command(hidden=True, name="preview")
def preview_alias(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    sample: bool = typer.Option(False, "--sample"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Preview a dataset's files and data."""
    from mdf_agent.cli.dataset_cmd import preview
    preview(source_id=source_id, sample=sample, json_output=json_output,
            service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command(hidden=True, name="versions")
def versions_alias(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    limit: int = typer.Option(50, "--limit", "-n"),
    offset: int = typer.Option(0, "--offset"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Show version history for a dataset."""
    from mdf_agent.cli.dataset_cmd import versions
    versions(source_id=source_id, limit=limit, offset=offset, json_output=json_output,
             service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command(hidden=True, name="diff")
def diff_alias(
    source_id: str = typer.Argument(..., help="Dataset source ID"),
    from_version: str = typer.Option(..., "--from"),
    to_version: str = typer.Option(..., "--to"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Show metadata diff between two versions."""
    from mdf_agent.cli.dataset_cmd import diff
    diff(source_id=source_id, from_version=from_version, to_version=to_version,
         json_output=json_output, service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command(hidden=True, name="edit")
def edit_alias(
    source_id: str = typer.Argument(..., help="Source ID"),
    title: Optional[str] = typer.Option(None, "--title", "-t"),
    description: Optional[str] = typer.Option(None, "--description", "-d"),
    keywords: Optional[List[str]] = typer.Option(None, "--keyword", "-k"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Edit metadata on a submission."""
    from mdf_agent.cli.dataset_cmd import edit
    edit(source_id=source_id, title=title, description=description, keywords=keywords,
         version=version, json_output=json_output, service=service, api_url=api_url,
         token=token, dev_user=dev_user)


@app.command(hidden=True, name="withdraw")
def withdraw_alias(
    source_id: str = typer.Argument(..., help="Source ID"),
    reason: Optional[str] = typer.Option(None, "--reason", "-r"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    yes: bool = typer.Option(False, "--yes"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Withdraw a pending_curation submission."""
    from mdf_agent.cli.dataset_cmd import withdraw
    withdraw(source_id=source_id, reason=reason, version=version, yes=yes,
             json_output=json_output, service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command(hidden=True, name="resubmit")
def resubmit_alias(
    source_id: str = typer.Argument(..., help="Source ID"),
    notes: Optional[str] = typer.Option(None, "--notes", "-n"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Resubmit a rejected dataset."""
    from mdf_agent.cli.dataset_cmd import resubmit
    resubmit(source_id=source_id, notes=notes, version=version,
             json_output=json_output, service=service, api_url=api_url, token=token, dev_user=dev_user)


# Hidden aliases for commands moved to admin sub-app
@app.command(hidden=True, name="pending")
def pending_alias(
    limit: int = typer.Option(20, "--limit", "-n"),
    organization: Optional[str] = typer.Option(None, "--organization", "-o"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """List datasets awaiting curation."""
    from mdf_agent.cli.admin_cmd import pending
    pending(limit=limit, organization=organization, json_output=json_output,
            service=service, api_url=api_url, token=token, dev_user=dev_user)


@app.command(hidden=True, name="approve")
def approve_alias(
    source_id: str = typer.Argument(..., help="Source ID"),
    mint_doi: bool = typer.Option(True, "--mint-doi/--no-mint-doi"),
    notes: Optional[str] = typer.Option(None, "--notes", "-n"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    yes: bool = typer.Option(False, "--yes"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Approve a dataset for publication."""
    from mdf_agent.cli.admin_cmd import approve
    approve(source_id=source_id, mint_doi=mint_doi, notes=notes, version=version,
            yes=yes, json_output=json_output, service=service, api_url=api_url,
            token=token, dev_user=dev_user)


@app.command(hidden=True, name="reject")
def reject_alias(
    source_id: str = typer.Argument(..., help="Source ID"),
    reason: str = typer.Option(..., "--reason", "-r"),
    suggestions: Optional[str] = typer.Option(None, "--suggestions"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    yes: bool = typer.Option(False, "--yes"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Reject a dataset and return to submitter."""
    from mdf_agent.cli.admin_cmd import reject
    reject(source_id=source_id, reason=reason, suggestions=suggestions, version=version,
           yes=yes, json_output=json_output, service=service, api_url=api_url,
           token=token, dev_user=dev_user)


@app.command("delete", hidden=True)
def delete_alias(
    source_id: str = typer.Argument(..., help="Source ID"),
    reason: str = typer.Option(..., "--reason", "-r"),
    version: Optional[str] = typer.Option(None, "--version", "-v"),
    yes: bool = typer.Option(False, "--yes"),
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Soft-delete a submission."""
    from mdf_agent.cli.admin_cmd import delete_dataset
    delete_dataset(source_id=source_id, reason=reason, version=version,
                   yes=yes, json_output=json_output, service=service, api_url=api_url,
                   token=token, dev_user=dev_user)


@app.command(hidden=True, name="stats")
def stats_alias(
    json_output: bool = typer.Option(False, "--json"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Show admin statistics."""
    from mdf_agent.cli.admin_cmd import stats
    stats(json_output=json_output, service=service, api_url=api_url, token=token, dev_user=dev_user)


# Hidden alias for update (publish --update covers this)
@app.command(hidden=True, name="update")
def update_alias(
    source_id: Optional[str] = typer.Argument(None, help="Source ID of dataset to update"),
    data: Optional[List[str]] = typer.Option(None, "--data", "-D", help="Data paths/URIs"),
    title: Optional[str] = typer.Option(None, "--title", "-t"),
    author: Optional[List[str]] = typer.Option(None, "--author", "-a"),
    description: Optional[str] = typer.Option(None, "--description", "-d"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit"),
    json_output: bool = typer.Option(False, "--json"),
    preflight_only: bool = typer.Option(False, "--preflight-only"),
    no_watch: bool = typer.Option(False, "--no-watch"),
    service: Optional[str] = typer.Option(None, "--service", "-s"),
    api_url: Optional[str] = typer.Option(None, "--api-url"),
    token: Optional[str] = typer.Option(None, "--token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user"),
):
    """Update an existing published dataset."""
    from rich.syntax import Syntax
    from mdf_agent.models.config import ManifestConfig

    resolved = resolve_service(service)

    resolved_source_id = source_id
    if not resolved_source_id:
        cfg = GlobalConfig()
        resolved_source_id = cfg.last_source_id
        if not resolved_source_id:
            console.print("[red]No source_id provided and no last published dataset.[/red]")
            console.print("[dim]Usage: mdf publish --update --data ./new_data/ --submit[/dim]")
            raise typer.Exit(code=1)
        if not json_output:
            console.print(f"[dim]Using last published dataset:[/dim] {resolved_source_id}")

    cfg = GlobalConfig()
    existing_title = resolved_source_id
    existing_authors: List[str] = []
    try:
        _base = api_url or _api_url_for_service(resolved)
        _client = BackendClient.authenticated(
            base_url=_base, token=token, service_instance=resolved, dev_user_id=dev_user,
        )
        try:
            resolved_source_id = _resolve_identifier_with_notice(_client, resolved_source_id)
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

    preflight = run_preflight(
        manifest,
        root=Path.cwd(),
        service=resolved,
        api_url=api_url or _api_url_for_service(resolved),
        token=token,
        dev_user=dev_user,
        submit=preflight_only or not dry_run,
    )

    if preflight_only:
        if json_output:
            print(json.dumps(preflight, indent=2))
        else:
            _render_preflight(preflight, title="Update preflight")
        if not preflight.get("success"):
            raise typer.Exit(code=1)
        return

    if not preflight.get("success"):
        if json_output:
            print(json.dumps({"success": False, "preflight": preflight}, indent=2))
        else:
            _render_preflight(preflight, title="Update preflight")
            console.print("\n[red]Update blocked by preflight issues[/red]")
        raise typer.Exit(code=1)

    agent = MDFAgent(root=None, manifest=manifest)
    payload = agent.build_submission(test=False, update=True)

    if dry_run:
        if json_output:
            print(json.dumps({"success": True, "dry_run": True, "payload": payload, "preflight": preflight}, indent=2))
            return
        _render_preflight(preflight, title="Update preflight")
        console.print("\n[bold cyan]Dry run — would update:[/bold cyan]")
        target = api_url or _api_url_for_service(resolved)
        console.print(f"[dim]Target: {target} ({resolved})[/dim]")
        console.print(f"[dim]Source ID: {resolved_source_id}[/dim]")
        syntax = Syntax(json.dumps(payload, indent=2), "json", theme="monokai")
        console.print(syntax)
        return

    import threading as _threading
    from rich.live import Live
    from rich.console import Group
    from rich.progress import (
        BarColumn, DownloadColumn, MofNCompleteColumn,
        Progress, SpinnerColumn, TextColumn, TransferSpeedColumn,
    )

    progress_callback = None
    on_files_resolved_cb = None
    on_file_done_cb = None
    live_ctx = None

    if sys.stderr.isatty():
        overall_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            console=console,
        )
        file_progress = Progress(
            SpinnerColumn("dots2"),
            TextColumn("[dim]{task.description}[/dim]"),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        )

        overall_task_id = None
        file_tasks: dict = {}
        file_lock = _threading.Lock()

        def on_files_resolved_cb(count: int) -> None:
            nonlocal overall_task_id
            overall_task_id = overall_progress.add_task(
                "[cyan]Uploading[/cyan]", total=count
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
            filename = Path(rel_path).name
            with file_lock:
                tid = file_tasks.pop(rel_path, None)
                if tid is not None:
                    file_progress.update(
                        tid, description=f"[dim green]✓ {filename}[/dim green]"
                    )
            if overall_task_id is not None:
                overall_progress.advance(overall_task_id, 1)
                task = overall_progress.tasks[overall_task_id]
                if task.completed >= task.total:
                    overall_progress.update(
                        overall_task_id,
                        description="[bold green]Upload complete[/bold green]",
                    )
            if tid is not None:
                def _remove(task_id=tid):
                    import time as _time
                    _time.sleep(0.4)
                    try:
                        file_progress.remove_task(task_id)
                    except Exception:
                        pass
                _threading.Thread(target=_remove, daemon=True).start()

        live_ctx = Live(
            Group(overall_progress, file_progress),
            console=console,
            refresh_per_second=15,
        )
        live_ctx.start()

    try:
        result = agent.publish(
            test=False,
            update=True,
            dry_run=False,
            token=token,
            service_instance=resolved,
            api_url=api_url,
            dev_user_id=dev_user,
            progress_callback=progress_callback,
            on_files_resolved=on_files_resolved_cb,
            on_file_done=on_file_done_cb,
        )
    except RuntimeError as exc:
        if live_ctx is not None:
            live_ctx.stop()
        console.print(f"\n[red]Upload failed:[/red] {exc}")
        raise typer.Exit(code=1)
    finally:
        if live_ctx is not None:
            live_ctx.stop()

    if json_output:
        print(json.dumps(result, indent=2))
        if not result.get("success"):
            raise typer.Exit(code=1)
        return

    if result.get("success"):
        new_version = result.get("version")
        console.print(f"\n[bold green]Updated successfully![/bold green]")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{result.get('source_id')}[/cyan]")
        console.print(f"  [dim]Service:[/dim] {resolved}")
        console.print("  [dim]Status:[/dim] pending_curation")
        if new_version:
            console.print(f"  [dim]Version:[/dim] [cyan]{new_version}[/cyan]")

        if result.get("source_id"):
            cfg = GlobalConfig()
            cfg.record_publish(result.get("source_id"), new_version, resolved)
            console.print(f"  [dim]Try:[/dim] mdf status {result.get('source_id')} | mdf show {result.get('source_id')}")
            if no_watch:
                console.print(f"  [dim]Watch:[/dim] mdf status --watch {result.get('source_id')}")
            else:
                raise typer.Exit(
                    code=_watch_submission(
                        result.get("source_id"),
                        interval=5,
                        timeout=1800,
                        service=resolved,
                        api_url=api_url,
                        token=token,
                        dev_user=dev_user,
                        announce=True,
                        stop_on_statuses={"pending_curation"},
                    )
                )
    else:
        require_success(result, error_prefix="Update failed")


# ---------------------------------------------------------------------------
# Import (cross-publish from external repos)
# ---------------------------------------------------------------------------

@app.command("import", rich_help_panel=PANEL_PUBLISH)
def import_external(
    identifier: str = typer.Argument(..., help="External identifier (e.g. zenodo:12345, https://zenodo.org/records/12345)"),
    output_dir: Optional[str] = typer.Option(None, "--output", "-o", help="Output directory for downloaded files"),
    dry_run: bool = typer.Option(True, "--dry-run/--submit", help="Preview metadata without downloading"),
    test: bool = typer.Option(False, "--test", help="Submit to test environment"),
    json_output: bool = typer.Option(False, "--json", help="JSON output"),
    no_watch: bool = typer.Option(False, "--no-watch", help="Do not watch after submit"),
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API URL"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id"),
):
    """Import a dataset from an external repository (Zenodo, etc.).

    Fetches metadata and data files, then publishes to MDF with
    provenance linking back to the original source.

    Examples:
        mdf import zenodo:12345                    # preview metadata
        mdf import zenodo:12345 --submit           # download + submit
        mdf import zenodo:12345 -o ./data --submit # custom output dir
    """
    from mdf_agent.importers.registry import resolve_adapter

    adapter = resolve_adapter(identifier)
    if not adapter:
        console.print(f"[red]No adapter found for:[/red] {identifier}")
        console.print("[dim]Supported formats: zenodo:ID, https://zenodo.org/records/ID[/dim]")
        raise typer.Exit(code=1)

    # Fetch metadata
    with api_spinner(f"Fetching metadata from {adapter.name()}..."):
        try:
            metadata = adapter.fetch_metadata(identifier)
        except Exception as exc:
            console.print(f"[red]Failed to fetch metadata:[/red] {exc}")
            raise typer.Exit(code=1)

    # Show metadata panel
    if not json_output:
        table = Table(show_header=False, box=rich_box.SIMPLE, padding=(0, 2))
        table.add_column("Field", style="bold")
        table.add_column("Value")
        table.add_row("Source", f"[cyan]{adapter.name()}[/cyan]")
        table.add_row("Title", metadata.get("title", ""))
        authors_str = ", ".join(a.get("name", "") for a in metadata.get("authors", []))
        table.add_row("Authors", authors_str)
        desc = metadata.get("description", "")
        if len(desc) > 200:
            desc = desc[:200] + "..."
        table.add_row("Description", desc)
        if metadata.get("keywords"):
            table.add_row("Keywords", ", ".join(metadata["keywords"]))
        if metadata.get("license"):
            lic = metadata["license"]
            table.add_row("License", lic.get("name", "") if isinstance(lic, dict) else str(lic))
        if metadata.get("doi"):
            table.add_row("DOI", f"[link=https://doi.org/{metadata['doi']}]{metadata['doi']}[/link]")
        table.add_row("URL", metadata.get("url", ""))
        files = metadata.get("file_urls", [])
        total_size = sum(f.get("size", 0) for f in files)
        table.add_row("Files", f"{len(files)} files ({_human_size(total_size)})")

        console.print(Panel(table, title=f"[bold]{adapter.name()} Import[/bold]", border_style="cyan"))

    if dry_run:
        if json_output:
            print(json.dumps({"success": True, "dry_run": True, "metadata": metadata}, indent=2))
        else:
            if metadata.get("file_urls"):
                console.print("\n[dim]Files:[/dim]")
                for f in metadata["file_urls"][:10]:
                    size_str = f" ({_human_size(f['size'])})" if f.get("size") else ""
                    console.print(f"  [dim]•[/dim] {f.get('filename', '?')}{size_str}")
                if len(metadata["file_urls"]) > 10:
                    console.print(f"  [dim]... and {len(metadata['file_urls']) - 10} more[/dim]")
            console.print(f"\n[bold]Ready to import. Run:[/bold]")
            console.print(f"  [cyan]mdf import {identifier} --submit[/cyan]")
        return

    # Download + submit
    import threading as _threading
    from rich.live import Live
    from rich.console import Group
    from rich.progress import (
        BarColumn, DownloadColumn, MofNCompleteColumn,
        Progress, SpinnerColumn, TextColumn, TransferSpeedColumn,
    )

    resolved = resolve_service(service)
    agent = MDFAgent()

    progress_callback = None
    on_files_resolved_cb = None
    on_file_done_cb = None
    live_ctx = None

    if sys.stderr.isatty():
        overall_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            console=console,
        )
        file_progress = Progress(
            SpinnerColumn("dots2"),
            TextColumn("[dim]{task.description}[/dim]"),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        )

        overall_task_id = None
        file_tasks: dict = {}
        file_lock = _threading.Lock()

        def on_files_resolved_cb(count: int) -> None:
            nonlocal overall_task_id
            overall_task_id = overall_progress.add_task(
                f"[cyan]Downloading from {adapter.name()}[/cyan]", total=count
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
            filename = Path(rel_path).name
            with file_lock:
                tid = file_tasks.pop(rel_path, None)
                if tid is not None:
                    file_progress.update(
                        tid, description=f"[dim green]✓ {filename}[/dim green]"
                    )
            if overall_task_id is not None:
                overall_progress.advance(overall_task_id, 1)
                task = overall_progress.tasks[overall_task_id]
                if task.completed >= task.total:
                    overall_progress.update(
                        overall_task_id,
                        description=f"[bold green]Download complete[/bold green]",
                    )
            if tid is not None:
                def _remove(task_id=tid):
                    import time as _time
                    _time.sleep(0.4)
                    try:
                        file_progress.remove_task(task_id)
                    except Exception:
                        pass
                _threading.Thread(target=_remove, daemon=True).start()

        live_ctx = Live(
            Group(overall_progress, file_progress),
            console=console,
            refresh_per_second=15,
        )
        live_ctx.start()

    try:
        result = agent.import_external(
            identifier=identifier,
            output_dir=output_dir,
            metadata=metadata,
            progress_callback=progress_callback,
            on_files_resolved=on_files_resolved_cb,
            on_file_done=on_file_done_cb,
        )
    except Exception as exc:
        if live_ctx is not None:
            live_ctx.stop()
        console.print(f"\n[red]Import failed:[/red] {exc}")
        raise typer.Exit(code=1)
    finally:
        if live_ctx is not None:
            live_ctx.stop()

    if not result.get("success"):
        if json_output:
            print(json.dumps(result, indent=2))
        else:
            console.print(f"\n[red]Import failed:[/red] {result.get('error', 'Unknown error')}")
        raise typer.Exit(code=1)

    # Now publish using the generated manifest (includes upload to MDF storage)
    manifest = result["manifest"]
    agent_pub = MDFAgent(root=Path(result["output_dir"]), manifest=manifest)

    if not json_output:
        console.print(f"\n[bold green]Downloaded {result.get('files_downloaded', 0)} files[/bold green] to [cyan]{result['output_dir']}[/cyan]")
        console.print("[dim]Uploading to MDF and submitting...[/dim]")

    # Build upload progress UI (reuse same pattern for the publish/upload phase)
    upload_progress_cb = None
    upload_files_resolved_cb = None
    upload_file_done_cb = None
    upload_live_ctx = None

    if sys.stderr.isatty():
        upload_overall = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            console=console,
        )
        upload_file_progress = Progress(
            SpinnerColumn("dots2"),
            TextColumn("[dim]{task.description}[/dim]"),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        )

        upload_overall_task_id = None
        upload_file_tasks: dict = {}
        upload_file_lock = _threading.Lock()

        def upload_files_resolved_cb(count: int) -> None:
            nonlocal upload_overall_task_id
            upload_overall_task_id = upload_overall.add_task(
                "[cyan]Uploading to MDF[/cyan]", total=count
            )

        def upload_progress_cb(rel_path: str, bytes_sent: int, total_bytes: int) -> None:
            filename = Path(rel_path).name
            with upload_file_lock:
                if rel_path not in upload_file_tasks:
                    upload_file_tasks[rel_path] = upload_file_progress.add_task(
                        filename, total=max(total_bytes, 1)
                    )
                upload_file_progress.update(upload_file_tasks[rel_path], completed=bytes_sent)

        def upload_file_done_cb(rel_path: str, _size: int) -> None:
            filename = Path(rel_path).name
            with upload_file_lock:
                tid = upload_file_tasks.pop(rel_path, None)
                if tid is not None:
                    upload_file_progress.update(
                        tid, description=f"[dim green]✓ {filename}[/dim green]"
                    )
            if upload_overall_task_id is not None:
                upload_overall.advance(upload_overall_task_id, 1)
                task = upload_overall.tasks[upload_overall_task_id]
                if task.completed >= task.total:
                    upload_overall.update(
                        upload_overall_task_id,
                        description="[bold green]Upload complete[/bold green]",
                    )
            if tid is not None:
                def _remove(task_id=tid):
                    import time as _time
                    _time.sleep(0.4)
                    try:
                        upload_file_progress.remove_task(task_id)
                    except Exception:
                        pass
                _threading.Thread(target=_remove, daemon=True).start()

        upload_live_ctx = Live(
            Group(upload_overall, upload_file_progress),
            console=console,
            refresh_per_second=15,
        )
        upload_live_ctx.start()

    try:
        pub_result = agent_pub.publish(
            test=test,
            dry_run=False,
            token=token,
            service_instance=resolved,
            api_url=api_url,
            dev_user_id=dev_user,
            progress_callback=upload_progress_cb,
            on_files_resolved=upload_files_resolved_cb,
            on_file_done=upload_file_done_cb,
        )
    except RuntimeError as exc:
        if upload_live_ctx is not None:
            upload_live_ctx.stop()
        console.print(f"\n[red]Upload failed:[/red] {exc}")
        raise typer.Exit(code=1)
    finally:
        if upload_live_ctx is not None:
            upload_live_ctx.stop()

    if json_output:
        print(json.dumps({"import": result, "publish": pub_result}, indent=2))
        if not pub_result.get("success"):
            raise typer.Exit(code=1)
        return

    if pub_result.get("success"):
        console.print("\n[bold green]Published successfully![/bold green]")
        source_id_val = pub_result.get("source_id")
        console.print(f"  [dim]Source ID:[/dim] [cyan]{source_id_val}[/cyan]")
        console.print(f"  [dim]External DOI:[/dim] {metadata.get('doi', 'N/A')}")
        console.print(f"  [dim]Service:[/dim] {resolved}")
        if source_id_val:
            cfg = GlobalConfig()
            cfg.record_publish(source_id_val, pub_result.get("version"), resolved)
            if no_watch:
                console.print(f"  [dim]Watch:[/dim] mdf status --watch {source_id_val}")
            else:
                raise typer.Exit(
                    code=_watch_submission(
                        source_id_val,
                        interval=5,
                        timeout=1800,
                        service=resolved,
                        api_url=api_url,
                        token=token,
                        dev_user=dev_user,
                        announce=True,
                        stop_on_statuses={"pending_curation"},
                    )
                )
    else:
        require_success(pub_result, error_prefix="Publish failed")


# Hidden alias: manifest top-level (now under config)
from mdf_agent.cli.manifest_cmd import app as manifest_app
app.add_typer(manifest_app, name="manifest", hidden=True)


def _human_size(nbytes: float) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


if __name__ == "__main__":
    app()
