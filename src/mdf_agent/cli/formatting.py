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
    "rejected":         "[red]x rejected[/red]",
    "failed":           "[red]! failed[/red]",
    "active":           "[cyan]> active[/cyan]",
    "closed":           "[dim]- closed[/dim]",
}


def format_status_badge(status: str) -> str:
    """Return Rich-markup colored string for a submission status."""
    return _STATUS_STYLES.get(status, f"[dim]{status}[/dim]")


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
