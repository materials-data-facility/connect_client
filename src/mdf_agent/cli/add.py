from __future__ import annotations

from typing import List, Optional

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Stage files for MDF submission")


@app.command()
def add(
    paths: List[str] = typer.Argument(..., help="Files or globs to add"),
    discover: Optional[bool] = typer.Option(
        None, "--discover/--no-discover", help="Auto-discover metadata"
    ),
):
    agent = MDFAgent.from_repo(".")
    staged = agent.add(*paths, discover=discover)
    typer.echo("Staged:")
    for path in staged:
        typer.echo(f"  {path}")
