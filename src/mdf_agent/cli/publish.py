from __future__ import annotations

import json
from typing import Optional

import typer

from mdf_agent.core.agent import MDFAgent

app = typer.Typer(help="Build or submit an MDF dataset")


@app.command()
def publish(
    test: bool = typer.Option(False, "--test", help="Submit as test dataset"),
    update: bool = typer.Option(False, "--update", help="Submit as update"),
    submit: bool = typer.Option(False, "--submit", help="Submit to MDF Connect"),
    service: str = typer.Option("prod", "--service", help="prod, dev, or local"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    token: str | None = typer.Option(None, "--token", help="Access token"),
    dev_user: str | None = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    agent = MDFAgent.from_repo(".")
    result = agent.publish(
        test=test,
        update=update,
        dry_run=not submit,
        token=token,
        service_instance=service,
        api_url=api_url,
        dev_user_id=dev_user,
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
