from __future__ import annotations

import json

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Show MDF repository status")


@app.command()
def status():
    agent = MDFAgent.from_repo(".")
    status_info = agent.status()
    typer.echo(json.dumps(status_info, indent=2))
