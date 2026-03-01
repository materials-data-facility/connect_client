from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

import typer
from rich.console import Console
from rich.table import Table

from mdf_agent.core.backend_client import BackendClient
from mdf_agent.core.config import resolve_service
from mdf_agent.cli.formatting import format_result_or_json, handle_api_result

console = Console()

app = typer.Typer(help="MDF Streaming commands")
_auth_opts: Dict[str, Optional[str]] = {"service": None, "token": None, "dev_user": None}


@app.callback()
def stream_callback(
    service: Optional[str] = typer.Option(None, "--service", "-s", help="Service instance (staging/prod/dev/local)"),
    token: Optional[str] = typer.Option(None, "--token", help="Globus access token"),
    dev_user: Optional[str] = typer.Option(None, "--dev-user", help="Dev-mode user id (X-User-Id)"),
):
    """MDF Streaming commands."""
    _auth_opts.update(service=service, token=token, dev_user=dev_user)


def _client(api_url: Optional[str]) -> BackendClient:
    return BackendClient.authenticated(
        base_url=api_url,
        token=_auth_opts.get("token"),
        service_instance=resolve_service(_auth_opts.get("service")),
        dev_user_id=_auth_opts.get("dev_user"),
    )


def _print(result):
    typer.echo(json.dumps(result, indent=2))


@app.command("create")
def create(
    title: str = typer.Option(..., "--title"),
    lab_id: Optional[str] = typer.Option(None, "--lab-id"),
    organization: Optional[str] = typer.Option(None, "--organization"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.stream_create(title, lab_id=lab_id, organization=organization)
    client.close()
    if json_output:
        _print(result)
    else:
        if result.get("success"):
            console.print(f"[green]Stream created:[/green] {result.get('stream_id')}")
        else:
            handle_api_result(result, error_prefix="Stream create failed")


@app.command("append")
def append(
    stream_id: str = typer.Option(..., "--stream-id"),
    files: Optional[Path] = typer.Option(None, "--files", help="JSON list of files or dict with files"),
    file_count: Optional[int] = typer.Option(None, "--file-count"),
    total_bytes: Optional[int] = typer.Option(None, "--total-bytes"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    files_payload = None
    if files:
        files_payload = json.loads(files.read_text(encoding="utf-8"))
        if isinstance(files_payload, dict) and "files" in files_payload:
            files_payload = files_payload["files"]
    result = client.stream_append(
        stream_id,
        files=files_payload,
        file_count=file_count,
        total_bytes=total_bytes,
    )
    client.close()
    format_result_or_json(result, json_output, success_msg="Files appended", error_prefix="Append failed")


@app.command("status")
def status(
    stream_id: str = typer.Option(..., "--stream-id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.stream_status(stream_id)
    client.close()
    format_result_or_json(result, json_output, error_prefix="Stream status")


@app.command("close")
def close(
    stream_id: str = typer.Option(..., "--stream-id"),
    mint_doi: Optional[bool] = typer.Option(None, "--mint-doi/--no-mint-doi"),
    title: Optional[str] = typer.Option(None, "--title"),
    description: Optional[str] = typer.Option(None, "--description"),
    authors: Optional[Path] = typer.Option(None, "--authors", help="Path to JSON author list"),
    keywords: Optional[Path] = typer.Option(None, "--keywords", help="Path to JSON keywords list"),
    license: Optional[str] = typer.Option(None, "--license"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    authors_payload = None
    if authors:
        authors_payload = json.loads(authors.read_text(encoding="utf-8"))

    keywords_payload = None
    if keywords:
        keywords_payload = json.loads(keywords.read_text(encoding="utf-8"))

    client = _client(api_url)
    result = client.stream_close(
        stream_id=stream_id,
        mint_doi=mint_doi,
        title=title,
        description=description,
        authors=authors_payload,
        keywords=keywords_payload,
        license=license,
    )
    client.close()
    format_result_or_json(result, json_output, success_msg=f"Stream closed: {stream_id}", error_prefix="Stream close failed")


@app.command("snapshot")
def snapshot(
    stream_id: str = typer.Option(..., "--stream-id"),
    title: Optional[str] = typer.Option(None, "--title"),
    update: bool = typer.Option(False, "--update"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
    json_output: bool = typer.Option(False, "--json", help="Raw JSON output"),
):
    client = _client(api_url)
    result = client.stream_snapshot(stream_id, title=title, update=update)
    client.close()
    format_result_or_json(result, json_output, success_msg="Snapshot created", error_prefix="Snapshot failed")


_PRESIGNED_THRESHOLD = 6 * 1024 * 1024  # 6 MB


@app.command("upload")
def upload(
    stream_id: str = typer.Option(..., "--stream-id"),
    files: List[Path] = typer.Argument(..., help="Files to upload"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """Upload files to a stream.

    Files > 6 MB are uploaded via pre-signed URL with a progress bar.

    Examples:
        mdf stream upload --stream-id abc123 data.csv
        mdf stream upload --stream-id abc123 *.csv
    """
    import hashlib
    import httpx
    from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn

    client = _client(api_url)

    uploaded = []
    errors = []

    use_progress = sys.stderr.isatty()
    progress_ctx = None
    if use_progress:
        progress_ctx = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
            transient=True,
        )
        progress_ctx.start()

    try:
        for file_path in files:
            if not file_path.exists():
                errors.append({"file": str(file_path), "error": "File not found"})
                continue

            file_size = file_path.stat().st_size

            try:
                if file_size > _PRESIGNED_THRESHOLD:
                    # Use presigned URL path for large files
                    url_result = client.stream_get_upload_url(stream_id, file_path.name)
                    if not url_result.get("success"):
                        errors.append({"file": str(file_path), "error": url_result.get("error", "Failed to get upload URL")})
                        continue

                    upload_url = url_result.get("url", "")
                    upload_headers = url_result.get("headers", {})
                    storage_path = url_result.get("path", "")

                    task_id = None
                    if progress_ctx:
                        task_id = progress_ctx.add_task(file_path.name, total=file_size)

                    # Stream the file via PUT
                    bytes_sent = 0
                    md5 = hashlib.md5()

                    def _file_stream():
                        nonlocal bytes_sent
                        with open(file_path, "rb") as f:
                            while True:
                                chunk = f.read(8 * 1024 * 1024)
                                if not chunk:
                                    break
                                md5.update(chunk)
                                bytes_sent += len(chunk)
                                if progress_ctx and task_id is not None:
                                    progress_ctx.update(task_id, completed=bytes_sent)
                                yield chunk

                    timeout = httpx.Timeout(connect=30, read=300, write=300, pool=30)
                    with httpx.Client(timeout=timeout) as http:
                        resp = http.put(upload_url, content=_file_stream(), headers=upload_headers)
                        resp.raise_for_status()

                    # Confirm the upload
                    confirm = client.stream_confirm_upload(
                        stream_id,
                        path=storage_path,
                        size_bytes=file_size,
                        checksum_md5=md5.hexdigest(),
                    )
                    if confirm.get("success"):
                        uploaded.append({
                            "filename": file_path.name,
                            "size_bytes": file_size,
                            "checksum_md5": md5.hexdigest(),
                        })
                    else:
                        errors.append({"file": str(file_path), "error": confirm.get("error", "Confirm failed")})
                else:
                    # Small file: base64 upload
                    content = file_path.read_bytes()
                    result = client.stream_upload(stream_id, file_path.name, content)
                    if result.get("success"):
                        uploaded.extend(result.get("files", []))
                    else:
                        errors.append({"file": str(file_path), "error": result.get("error", "Unknown error")})
            except Exception as e:
                errors.append({"file": str(file_path), "error": str(e)})
    finally:
        if progress_ctx:
            progress_ctx.stop()

    client.close()

    if uploaded:
        console.print(f"\n[green]Uploaded {len(uploaded)} file(s)[/green]")
        table = Table(show_header=True, header_style="bold")
        table.add_column("Filename")
        table.add_column("Size", justify="right")
        table.add_column("Checksum", style="dim")

        for f in uploaded:
            size_kb = f.get("size_bytes", 0) / 1024
            table.add_row(
                f.get("filename", ""),
                f"{size_kb:.1f} KB",
                (f.get("checksum_md5", "") or "")[:12] + "...",
            )
        console.print(table)

    if errors:
        console.print(f"\n[red]Errors ({len(errors)}):[/red]")
        for err in errors:
            console.print(f"  [red]x[/red] {err['file']}: {err['error']}")


@app.command("files")
def list_files(
    stream_id: str = typer.Option(..., "--stream-id"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Override API base URL"),
):
    """List files in a stream."""
    client = _client(api_url)
    result = client.stream_list_files(stream_id)
    client.close()

    if result.get("success"):
        files = result.get("files", [])
        console.print(f"\n[bold]Stream {stream_id}[/bold] - {len(files)} file(s)\n")

        if files:
            table = Table(show_header=True, header_style="bold")
            table.add_column("Filename")
            table.add_column("Size", justify="right")
            table.add_column("Uploaded", style="dim")
            table.add_column("Checksum", style="dim")

            for f in files:
                size_kb = f.get("size_bytes", 0) / 1024
                stored_at = f.get("stored_at", "")[:19] if f.get("stored_at") else ""
                table.add_row(
                    f.get("filename", ""),
                    f"{size_kb:.1f} KB",
                    stored_at,
                    f.get("checksum_md5", "")[:12] + "...",
                )
            console.print(table)
        else:
            console.print("[dim]No files uploaded yet[/dim]")
    else:
        console.print(f"[red]Error:[/red] {result.get('error', 'Unknown error')}")
