from __future__ import annotations

from typing import List

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Initialize an MDF dataset repository")


@app.command()
def init(
    path: str = typer.Argument(".", help="Repository path"),
    title: str = typer.Option(..., "--title", help="Dataset title"),
    author: List[str] = typer.Option(..., "--author", help="Author name (repeatable)"),
    description: str | None = typer.Option(None, "--description", help="Dataset description"),
    publisher: str | None = typer.Option(None, "--publisher", help="Dataset publisher"),
    publication_year: int | None = typer.Option(None, "--year", help="Publication year"),
):
    MDFAgent.init(
        path=path,
        title=title,
        authors=author,
        description=description,
        publisher=publisher,
        publication_year=publication_year,
    )
    typer.echo(f"Initialized MDF repository at {path}")
