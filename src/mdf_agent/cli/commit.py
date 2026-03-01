from __future__ import annotations

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Record staged files")


@app.command()
def commit(
    message: str = typer.Option(..., "-m", "--message", help="Commit message")
):
    agent = MDFAgent.from_repo(".")
    commit_info = agent.commit(message)
    typer.echo(f"Committed {len(commit_info['staged_files'])} files")
