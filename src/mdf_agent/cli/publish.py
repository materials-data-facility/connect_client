from __future__ import annotations

import json

import typer

from mdf_agent.auth.globus import get_authorizer
from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Build or submit an MDF dataset")


@app.command()
def publish(
    test: bool = typer.Option(False, "--test", help="Submit as test dataset"),
    update: bool = typer.Option(False, "--update", help="Submit as update"),
    submit: bool = typer.Option(False, "--submit", help="Submit to MDF Connect"),
    service: str = typer.Option("prod", "--service", help="prod or dev"),
    token: str | None = typer.Option(None, "--token", help="Access token"),
    client_id: str | None = typer.Option(None, "--client-id", help="Globus client ID"),
    scope: str | None = typer.Option(None, "--scope", help="Globus scope"),
):
    agent = MDFAgent.from_repo(".")
    authorizer = None
    if submit:
        authorizer = get_authorizer(token=token, client_id=client_id, scope=scope)
    result = agent.publish(
        test=test,
        update=update,
        dry_run=not submit,
        authorizer=authorizer,
        service_instance=service,
    )

    if not submit:
        payload = result["payload"]
        typer.echo(json.dumps(payload, indent=2))
        return

    if result.get("success"):
        typer.echo(f"Submitted dataset: {result.get('source_id')}")
    else:
        typer.echo(f"Submission failed: {result.get('error')}")
        raise typer.Exit(code=1)
