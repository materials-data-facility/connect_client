from __future__ import annotations

from typing import List

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Create a new dataset derived from an MDF source")


@app.command()
def clone(
    source_id: str = typer.Argument(..., help="Source dataset ID"),
    path: str = typer.Argument(".", help="Destination path"),
    title: str = typer.Option(None, "--title", help="Title for derived dataset"),
    author: List[str] = typer.Option([], "--author", help="Author name (repeatable)"),
):
    derived_title = title or f"Derived from {source_id}"
    authors = author if author else ["Unknown"]
    agent = MDFAgent.init(path=path, title=derived_title, authors=authors)
    agent.manifest.derived_from = [{"source_id": source_id, "relationship": "derived"}]
    agent.save_manifest()
    typer.echo(f"Initialized derived dataset in {path}")
