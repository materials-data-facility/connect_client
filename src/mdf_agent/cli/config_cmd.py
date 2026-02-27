"""CLI commands for mdf config show/set/get/path."""

from __future__ import annotations

import json

import typer
from rich.console import Console
from rich.syntax import Syntax

from mdf_agent.core.config import GlobalConfig

app = typer.Typer(help="Manage MDF Agent configuration")
console = Console()


@app.command("show")
def show():
    """Show current configuration."""
    cfg = GlobalConfig()
    if cfg.data:
        syntax = Syntax(json.dumps(cfg.data, indent=2), "json", theme="monokai")
        console.print(syntax)
    else:
        console.print("[dim]No configuration set yet.[/dim]")
        console.print(f"[dim]Config file:[/dim] {cfg.path}")


@app.command("set")
def set_value(
    key: str = typer.Argument(..., help="Dotted key (e.g. defaults.service)"),
    value: str = typer.Argument(..., help="Value to set"),
):
    """Set a configuration value."""
    cfg = GlobalConfig()
    cfg.set(key, value)
    console.print(f"[green]Set[/green] {key} = {value}")


@app.command("get")
def get_value(
    key: str = typer.Argument(..., help="Dotted key (e.g. defaults.service)"),
):
    """Get a configuration value."""
    cfg = GlobalConfig()
    val = cfg.get(key)
    if val is None:
        console.print(f"[dim]{key} is not set[/dim]")
        raise typer.Exit(code=1)
    if isinstance(val, (dict, list)):
        console.print(json.dumps(val, indent=2))
    else:
        console.print(str(val))


@app.command("path")
def path():
    """Show configuration file path."""
    cfg = GlobalConfig()
    console.print(str(cfg.path))
