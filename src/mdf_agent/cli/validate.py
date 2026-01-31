from __future__ import annotations

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Validate manifest and data before publishing")


@app.command()
def validate():
    agent = MDFAgent.from_repo(".")
    results = agent.validate()
    errors = results.get("errors", [])
    warnings = results.get("warnings", [])

    if errors:
        typer.echo("Errors:")
        for error in errors:
            typer.echo(f"  - {error}")
    if warnings:
        typer.echo("Warnings:")
        for warning in warnings:
            typer.echo(f"  - {warning}")

    if errors:
        raise typer.Exit(code=1)
    typer.echo("Validation passed")
