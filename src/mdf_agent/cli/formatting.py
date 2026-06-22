"""Shared output formatting for MDF CLI commands.

Provides consistent Rich-formatted output for API results, status badges,
and error interpretation across all CLI commands.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

from rich.console import Console

console = Console(no_color=bool(os.environ.get("NO_COLOR")))

_STATUS_STYLES = {
    "published":        "[green]* published[/green]",
    "pending_curation": "[yellow]~ pending_curation[/yellow]",
    "pending":          "[yellow]~ pending[/yellow]",
    "approved":         "[blue]> approved[/blue]",
    "publish_failed":   "[red]! publish_failed[/red]",
    "rejected":         "[red]x rejected[/red]",
    "failed":           "[red]! failed[/red]",
    "withdrawn":        "[dim]- withdrawn[/dim]",
    "deleted":          "[dim]- deleted[/dim]",
    "active":           "[cyan]> active[/cyan]",
    "closed":           "[dim]- closed[/dim]",
}


def format_status_badge(status: str) -> str:
    """Return Rich-markup colored string for a submission status."""
    return _STATUS_STYLES.get(status, f"[dim]{status}[/dim]")


def read_client(
    api_url: Optional[str] = None,
    token: Optional[str] = None,
    service: str = "staging",
    dev_user: Optional[str] = None,
):
    """Build a BackendClient for PUBLIC read endpoints.

    Public GETs (search, card, citation, related, preview, health) are served
    unauthenticated, so — unlike ``BackendClient.authenticated`` — this never
    triggers an interactive browser login. It still uses explicit credentials
    when they are available (so private/pending records remain reachable), but a
    missing login does not block reading public data.

    Also turns an unknown ``--service`` into a clean error instead of a raw
    traceback.
    """
    import typer

    from mdf_agent.core.backend_client import BackendClient, _api_url_for_service

    try:
        base = api_url or _api_url_for_service(service)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    has_creds = bool(
        token
        or os.environ.get("MDF_CONNECT_TOKEN")
        or (os.environ.get("MDF_CLIENT_ID") and os.environ.get("MDF_CLIENT_SECRET"))
        or dev_user
        or os.environ.get("MDF_DEV_USER_ID")
    )
    # A cached interactive login also counts: route logged-in users through the
    # authenticated client (uses the cached token, no new browser prompt) so they
    # can read ACL-restricted datasets they own and reach auth-gated endpoints
    # (e.g. /search/semantic). Anonymous users still get a public-only client and
    # are never forced into a login prompt.
    if not has_creds:
        try:
            from mdf_agent.auth.globus import is_logged_in
            has_creds = is_logged_in(service_instance=service)
        except Exception:
            has_creds = False
    if has_creds:
        return BackendClient.authenticated(
            base_url=base, token=token, service_instance=service, dev_user_id=dev_user
        )
    return BackendClient(base_url=base)


@contextmanager
def api_spinner(message: str = "Loading..."):
    """Show a Rich spinner while waiting for an API call. Auto-suppresses in pipes/non-TTY."""
    if sys.stderr.isatty() and not os.environ.get("NO_COLOR"):
        with console.status(f"[dim]{message}[/dim]", spinner="dots"):
            yield
    else:
        yield


def json_or_rich(result: dict, json_mode: bool, render_fn: Callable) -> None:
    """If json_mode, dump raw JSON to stdout. Otherwise call render_fn."""
    if json_mode:
        print(json.dumps(result, indent=2))
    else:
        render_fn(result)


def handle_api_result(
    result: Dict[str, Any],
    success_msg: Optional[str] = None,
    error_prefix: str = "Error",
) -> bool:
    """Interpret a standard API result envelope and print formatted output.

    Returns True on success, False on error.
    """
    if result.get("success"):
        if success_msg:
            console.print(f"[green]{success_msg}[/green]")
        return True

    error = result.get("error") or result.get("detail") or "Unknown error"

    # Map common HTTP-level errors to actionable messages
    error_str = str(error).lower()
    if "401" in error_str or "unauthorized" in error_str or "not authenticated" in error_str:
        console.print(f"[red]{error_prefix}: Authentication required[/red]")
        console.print("[dim]Run:[/dim] [cyan]mdf login[/cyan]")
        return False

    if "403" in error_str or "forbidden" in error_str:
        console.print(f"[red]{error_prefix}: Permission denied[/red]")
        return False

    if "429" in error_str or "rate limit" in error_str:
        console.print(f"[yellow]{error_prefix}: Rate limited — try again shortly[/yellow]")
        return False

    if "404" in error_str or "not found" in error_str:
        console.print(f"[yellow]{error_prefix}: Not found[/yellow]")
        return False

    if "validation" in error_str:
        console.print(f"[red]{error_prefix}: Validation error[/red]")
        console.print(f"  [dim]{error}[/dim]")
        return False

    console.print(f"[red]{error_prefix}:[/red] {error}")
    return False


def require_success(result: dict, error_prefix: str = "Error") -> None:
    """Check API result and raise typer.Exit(1) on failure."""
    import typer

    if not handle_api_result(result, error_prefix=error_prefix):
        raise typer.Exit(code=1)


def format_result_or_json(
    result: Dict[str, Any],
    json_mode: bool,
    success_msg: Optional[str] = None,
    error_prefix: str = "Error",
) -> bool:
    """If json_mode, dump raw JSON. Otherwise use handle_api_result."""
    if json_mode:
        print(json.dumps(result, indent=2))
        return result.get("success", False)
    return handle_api_result(result, success_msg=success_msg, error_prefix=error_prefix)
