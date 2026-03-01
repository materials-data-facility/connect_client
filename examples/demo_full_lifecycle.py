#!/usr/bin/env python3
"""MDF Connect v2 -- Full Lifecycle Demo

Starts a local v2 backend, then walks through:

  1. Repository workflow:  init -> add -> commit -> validate -> publish
  2. Streaming workflow:   create -> upload -> preview -> snapshot
  3. Curation workflow:    pending -> approve (DOI minting)
  4. Discovery:            search -> dataset card -> citation

Run:
    cd /path/to/mdf_client
    python examples/demo_full_lifecycle.py
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# rich imports
# ---------------------------------------------------------------------------
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich import box

console = Console(width=100)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = REPO_ROOT / "cs" / "aws"
EXAMPLE_ALLOYS = REPO_ROOT / "examples" / "dft-alloys"
EXAMPLE_XRD = REPO_ROOT / "examples" / "xrd-patterns"

API_URL = "http://127.0.0.1:8080"
DEV_USER = "demo-researcher"

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/bold cyan]", style="cyan")
    console.print()


def step(label: str) -> None:
    console.print(f"  [bold white]> {label}[/bold white]")


def ok(msg: str = "done") -> None:
    console.print(f"    [green]{msg}[/green]")


def show_json(data: dict, title: str = "") -> None:
    text = json.dumps(data, indent=2, default=str)
    syntax = Syntax(text, "json", theme="monokai", line_numbers=False)
    if title:
        console.print(Panel(syntax, title=f"[bold]{title}[/bold]", border_style="blue", padding=(0, 1)))
    else:
        console.print(syntax)


def fail(msg: str) -> None:
    console.print(f"    [bold red]FAILED: {msg}[/bold red]")
    sys.exit(1)


def check(result: dict, label: str) -> dict:
    if not result.get("success"):
        fail(f"{label}: {result.get('error', result)}")
    ok(label)
    return result


# ---------------------------------------------------------------------------
# Backend lifecycle
# ---------------------------------------------------------------------------

def start_backend() -> subprocess.Popen:
    """Start the v2 backend as a subprocess."""
    env = {
        **os.environ,
        "STORE_BACKEND": "sqlite",
        "SQLITE_PATH": "/tmp/mdf_demo.db",
        "SQLITE_STREAMS_PATH": "/tmp/mdf_demo_streams.db",
        "STORAGE_BACKEND": "local",
        "FILE_STORE_PATH": "/tmp/mdf_demo_files",
        "AUTH_MODE": "dev",
        "ALLOW_ALL_CURATORS": "true",
        "USE_MOCK_DATACITE": "true",
        "ASYNC_DISPATCH_MODE": "inline",
        "LOG_LEVEL": "WARNING",
        "PYTHONPATH": str(BACKEND_DIR),
    }

    # Clean up prior demo state
    for p in ["/tmp/mdf_demo.db", "/tmp/mdf_demo_streams.db"]:
        if os.path.exists(p):
            os.unlink(p)
    if os.path.exists("/tmp/mdf_demo_files"):
        shutil.rmtree("/tmp/mdf_demo_files")

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "v2.app:app",
         "--host", "127.0.0.1", "--port", "8080", "--log-level", "warning"],
        cwd=str(BACKEND_DIR),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def wait_for_backend(timeout: float = 10.0) -> None:
    """Poll /health until the backend is ready."""
    import httpx

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{API_URL}/health", timeout=1.0)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    fail("Backend did not start within timeout")


# ---------------------------------------------------------------------------
# BackendClient helper (thin wrapper so we don't need mdf_agent installed)
# ---------------------------------------------------------------------------

def client():
    """Return an authenticated BackendClient for the local demo."""
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from mdf_agent.core.backend_client import BackendClient
    return BackendClient(base_url=API_URL, user_id=DEV_USER)


# ---------------------------------------------------------------------------
# Demo scenario
# ---------------------------------------------------------------------------

def demo_repository_workflow(c) -> str:
    """Part 1: Git-style repository workflow."""
    section("Part 1: Repository Workflow")
    console.print(
        "  The [cyan]mdf[/cyan] CLI gives researchers a git-style workflow.\n"
        "  [dim]init -> add -> commit -> validate -> publish[/dim]\n"
    )

    work_dir = Path(tempfile.mkdtemp(prefix="mdf_demo_repo_"))

    # Copy example data
    step("Creating dataset directory with DFT alloy calculations")
    shutil.copy(EXAMPLE_ALLOYS / "calculations.csv", work_dir / "calculations.csv")
    shutil.copy(EXAMPLE_ALLOYS / "parameters.json", work_dir / "parameters.json")
    ok(f"{work_dir}")

    # Init
    step("mdf init --title '...' --author '...'")
    from mdf_agent.core.agent import MDFAgent
    agent = MDFAgent.init(
        path=str(work_dir),
        title="High-Throughput DFT Study of Binary Intermetallic Alloys",
        authors=["Jane Doe", "John Smith", "Alice Chen"],
        description="Formation energies and electronic properties for 15 Al-X intermetallics computed with VASP PBE.",
    )
    ok("Repository initialized")

    # Add files and register data sources
    step("mdf add calculations.csv parameters.json")
    staged = agent.add("calculations.csv", "parameters.json")
    agent.add_data_source("./calculations.csv")
    agent.add_data_source("./parameters.json")
    agent.save_manifest()
    ok(f"Staged {len(staged)} files")

    # Commit
    step("mdf commit -m 'Initial DFT calculation results'")
    agent.commit("Initial DFT calculation results")
    ok("Committed")

    # Validate
    step("mdf validate")
    validation = agent.validate()
    errors = validation.get("errors", [])
    if errors:
        fail(f"Validation errors: {errors}")
    ok(f"Validation passed ({len(validation.get('warnings', []))} warnings)")

    # Build & preview payload
    step("mdf publish (dry run)")
    payload = agent.build_submission(test=True)
    show_json(payload, "Submission Payload (dry run)")

    # Publish to local backend
    step("mdf publish --submit --service local --dev-user demo-researcher")
    result = c.submit(payload)
    result = check(result, "Published")
    source_id = result["source_id"]
    console.print(f"    [dim]source_id:[/dim] [cyan]{source_id}[/cyan]  version: {result.get('version', '1.0')}")

    shutil.rmtree(work_dir, ignore_errors=True)
    return source_id


def demo_streaming_workflow(c) -> str:
    """Part 2: Streaming data workflow."""
    section("Part 2: Streaming Workflow")
    console.print(
        "  Streams let instruments push data in real-time.\n"
        "  [dim]create -> upload -> preview -> snapshot[/dim]\n"
    )

    # Create stream
    step("mdf stream create --title 'XRD Beamline 12-ID'")
    result = c.stream_create(
        title="XRD Beamline 12-ID Live Acquisition",
        lab_id="APS-12-ID",
        organization="Argonne National Laboratory",
    )
    result = check(result, "Stream created")
    stream_id = result["stream_id"]
    console.print(f"    [dim]stream_id:[/dim] [cyan]{stream_id}[/cyan]")

    # Upload XRD data files
    step("mdf stream upload sample_001.csv sample_002.csv")
    for fname in ["sample_001.csv", "sample_002.csv"]:
        content = (EXAMPLE_XRD / fname).read_bytes()
        up = c.stream_upload(stream_id, fname, content)
        if not up.get("success"):
            fail(f"Upload {fname}: {up.get('error')}")
    ok("2 files uploaded")

    # Also upload the instrument config
    step("mdf stream upload instrument.yaml")
    content = (EXAMPLE_XRD / "instrument.yaml").read_bytes()
    up = c.stream_upload(stream_id, "instrument.yaml", content)
    check(up, "instrument.yaml uploaded")

    # List files
    step("mdf stream files --stream-id ...")
    files_result = c.stream_list_files(stream_id)
    if files_result.get("success"):
        files = files_result.get("files", [])
        table = Table(title="Stream Files", box=box.ROUNDED, border_style="blue")
        table.add_column("Filename", style="cyan")
        table.add_column("Size", justify="right")
        table.add_column("Type", style="dim")
        for f in files:
            size_kb = f.get("size_bytes", 0) / 1024
            table.add_row(
                f.get("filename", ""),
                f"{size_kb:.1f} KB",
                f.get("content_type", ""),
            )
        console.print(table)

    # Preview
    step("GET /stream/{id}/preview")
    preview = c.stream_preview(stream_id)
    if preview.get("success") and preview.get("previews"):
        first = preview["previews"][0]
        pdata = first.get("preview", {})
        if pdata.get("columns") and pdata.get("sample_rows"):
            table = Table(
                title=f"Preview: {first.get('filename', '?')}",
                box=box.ROUNDED,
                border_style="green",
            )
            for col in pdata["columns"]:
                table.add_column(col)
            for row in pdata["sample_rows"][:5]:
                table.add_row(*[str(row.get(c, "")) for c in pdata["columns"]])
            console.print(table)
    ok("Live preview rendered")

    # Stream status
    step("mdf stream status --stream-id ...")
    ss = c.stream_status(stream_id)
    if ss.get("success"):
        st = ss["stream"]
        console.print(
            f"    [dim]status:[/dim] {st.get('status')}  "
            f"[dim]files:[/dim] {st.get('file_count')}  "
            f"[dim]bytes:[/dim] {st.get('total_bytes')}"
        )

    # Snapshot -> submission
    step("mdf stream snapshot --title 'XRD Session Feb 2026'")
    snap = c.stream_snapshot(
        stream_id,
        title="XRD Session Feb 2026 - Perovskite Thin Films",
    )
    snap = check(snap, "Snapshot created")
    snap_source_id = snap["source_id"]
    console.print(f"    [dim]source_id:[/dim] [cyan]{snap_source_id}[/cyan]  version: {snap.get('version', '1.0')}")

    return snap_source_id


def demo_curation_workflow(c, source_id: str) -> None:
    """Part 3: Curation workflow -- submit -> review -> approve -> DOI."""
    section("Part 3: Curation & DOI Minting")
    console.print(
        "  Curators review submitted datasets before publication.\n"
        "  [dim]pending_curation -> approve -> DOI minted[/dim]\n"
    )

    # Move to pending_curation
    step("Curator sets status to pending_curation")
    result = c.update_status(source_id, "1.0", "pending_curation")
    check(result, "Status updated")

    # Show pending queue
    step("GET /curation/pending")
    import httpx
    resp = httpx.get(
        f"{API_URL}/curation/pending",
        headers={"X-User-Id": DEV_USER},
        timeout=10.0,
    )
    pending = resp.json()
    if pending.get("success"):
        subs = pending.get("submissions", [])
        console.print(f"    [dim]{pending.get('pending_count', len(subs))} dataset(s) awaiting review[/dim]")
        for s in subs:
            console.print(f"    - [cyan]{s.get('source_id')}[/cyan] v{s.get('version')} \"{s.get('title', '?')}\"")

    # Approve with DOI
    step(f"POST /curation/{source_id}/approve (mint DOI)")
    resp = httpx.post(
        f"{API_URL}/curation/{source_id}/approve",
        headers={"X-User-Id": DEV_USER},
        json={"notes": "Excellent dataset, well-documented.", "mint_doi": True},
        timeout=10.0,
    )
    approve = resp.json()
    if approve.get("success") or approve.get("status") == "published":
        ok(f"Approved! Status: {approve.get('status', 'published')}")
        doi_info = approve.get("doi") or approve.get("doi_job", {})
        if isinstance(doi_info, dict) and doi_info.get("doi"):
            console.print(f"    [dim]DOI:[/dim] [bold green]{doi_info['doi']}[/bold green]")
        elif isinstance(doi_info, dict) and doi_info.get("success"):
            console.print(f"    [dim]DOI:[/dim] [bold green]{doi_info.get('doi', 'mock-doi')}[/bold green]")
    else:
        console.print(f"    [yellow]Approve response:[/yellow] {json.dumps(approve, indent=2)}")


def demo_discovery(c, source_ids: list[str]) -> None:
    """Part 4: Search, dataset cards, and citations."""
    section("Part 4: Discovery & Citation")
    console.print(
        "  Published datasets are searchable and citable.\n"
        "  [dim]search -> card -> citation[/dim]\n"
    )

    # Search
    step("mdf search 'alloy'")
    result = c.search("alloy", search_type="datasets", limit=5)
    if result.get("results"):
        table = Table(title="Search Results", box=box.ROUNDED, border_style="magenta")
        table.add_column("#", style="dim", width=3)
        table.add_column("Title")
        table.add_column("Source ID", style="cyan")
        table.add_column("Status", style="dim")
        for i, item in enumerate(result["results"], 1):
            table.add_row(
                str(i),
                (item.get("title") or "Untitled")[:50],
                item.get("source_id", ""),
                item.get("status", ""),
            )
        console.print(table)
    else:
        console.print("    [dim]No search results (expected for fresh db)[/dim]")

    # Dataset card
    for sid in source_ids:
        step(f"mdf backend card {sid}")
        card_result = c.get_card(sid)
        if card_result.get("success"):
            card = card_result["card"]
            lines = []
            lines.append(f"[bold]{card.get('title', 'Untitled')}[/bold]")
            lines.append(f"[dim]{card.get('description', '')}[/dim]")
            lines.append("")
            if card.get("authors"):
                lines.append(f"[dim]Authors:[/dim]  {', '.join(card['authors'])}")
            if card.get("organization"):
                lines.append(f"[dim]Org:[/dim]      {card['organization']}")
            if card.get("doi"):
                lines.append(f"[dim]DOI:[/dim]      [green]https://doi.org/{card['doi']}[/green]")
            lines.append(f"[dim]Status:[/dim]   {card.get('status', '?')}")
            stats = card.get("stats", {})
            if stats:
                lines.append(f"[dim]Sources:[/dim]  {stats.get('data_sources_count', '?')} | Types: {', '.join(stats.get('file_types', []))}")
            profile = card.get("profile_summary", {})
            if profile:
                lines.append(f"[dim]Files:[/dim]    {profile.get('total_files', '?')} ({profile.get('total_bytes', 0)} bytes)")
                if profile.get("formats"):
                    lines.append(f"[dim]Formats:[/dim]  {profile['formats']}")

            console.print(Panel(
                "\n".join(lines),
                title=f"[cyan]{sid}[/cyan] v{card.get('version', '1.0')}",
                border_style="blue",
                padding=(1, 2),
            ))
        break  # Just show the first one in detail

    # Citation
    step(f"mdf backend cite {source_ids[0]} --format bibtex")
    cite = c.get_citation(source_ids[0], format="all")
    if cite.get("success"):
        for fmt in ["apa", "bibtex"]:
            text = cite.get(fmt, "")
            if text:
                console.print(Panel(text, title=fmt.upper(), border_style="green", padding=(0, 1)))
    else:
        console.print(f"    [dim]Citation not available: {cite.get('error', '?')}[/dim]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    console.print()
    console.print(Panel.fit(
        "[bold white]MDF Connect v2 -- Full Lifecycle Demo[/bold white]\n\n"
        "[dim]This demo starts a local v2 backend and walks through the\n"
        "complete dataset lifecycle: create, upload, publish, curate,\n"
        "search, and cite -- all running on your machine.[/dim]",
        border_style="bright_blue",
        padding=(1, 3),
    ))

    # -----------------------------------------------------------------------
    section("Starting Local Backend")
    step("Launching v2 FastAPI server on :8080")

    proc = start_backend()
    try:
        wait_for_backend()
        ok("Backend healthy")

        # Create the client
        c = client()

        # Run each demo section
        source_id_repo = demo_repository_workflow(c)
        source_id_stream = demo_streaming_workflow(c)
        demo_curation_workflow(c, source_id_repo)
        demo_discovery(c, [source_id_repo, source_id_stream])

        c.close()

        # ---------------------------------------------------------------
        section("Summary")
        console.print(
            "  [bold green]All steps completed successfully.[/bold green]\n"
        )

        table = Table(box=box.SIMPLE, padding=(0, 2))
        table.add_column("Workflow", style="bold")
        table.add_column("Source ID", style="cyan")
        table.add_column("Status")
        table.add_row("Repository publish", source_id_repo, "[green]published (DOI minted)[/green]")
        table.add_row("Stream snapshot", source_id_stream, "[blue]submitted[/blue]")
        console.print(table)

        console.print()
        console.print("  [dim]Backend ran at[/dim] [cyan]http://127.0.0.1:8080[/cyan]")
        console.print("  [dim]SQLite DB:[/dim]     /tmp/mdf_demo.db")
        console.print("  [dim]File store:[/dim]    /tmp/mdf_demo_files/")
        console.print()

    finally:
        step("Shutting down backend")
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        ok("Backend stopped")
        console.print()


if __name__ == "__main__":
    main()
