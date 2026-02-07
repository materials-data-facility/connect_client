#!/usr/bin/env python3
"""MDF Connect v2 -- Staging + Globus Auth Demo

Tests the deployed staging backend with real Globus authentication.
Walks through:

  1. Health check against the staging API
  2. Stream workflow: create -> upload files -> list -> verify on Globus
  3. Repository workflow: init -> add -> commit -> publish (dry-run + submit)
  4. Discovery: search -> dataset card -> citation

Prerequisites:
  1. Deploy the staging stack:
       cd cs/aws && sam build && ./deploy.sh staging
  2. Authenticate with Globus:
       mdf login --service prod
  3. Run this script with the staging API URL:
       python examples/demo_staging_globus.py https://<id>.execute-api.us-east-1.amazonaws.com/staging

The script creates a stream and uploads test CSV data to the Globus HTTPS
endpoint at data.materialsdatafacility.org under /mdf/staging/.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from mdf_agent.core.backend_client import BackendClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def heading(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def step(label: str) -> None:
    print(f"  > {label}")


def ok(msg: str = "done") -> None:
    print(f"    OK: {msg}")


def fail(msg: str) -> None:
    print(f"    FAILED: {msg}")
    sys.exit(1)


def show(data: dict, indent: int = 4) -> None:
    prefix = " " * indent
    for line in json.dumps(data, indent=2, default=str).splitlines():
        print(f"{prefix}{line}")


def check(result: dict, label: str) -> dict:
    if not result.get("success"):
        fail(f"{label}: {result.get('error', result)}")
    ok(label)
    return result


# ---------------------------------------------------------------------------
# Test CSV data (small but real enough to exercise the pipeline)
# ---------------------------------------------------------------------------

SAMPLE_CSV = """\
material,formation_energy_eV,bandgap_eV,lattice_a,lattice_c,space_group
Al3Ti,  -0.42,  0.00, 3.854, 8.584, I4/mmm
AlNi,   -0.67,  0.00, 2.881, 2.881, Pm-3m
Al3Ni,  -0.41,  0.00, 6.611, 7.366, Pnma
AlFe,   -0.30,  0.00, 2.909, 2.909, Pm-3m
Al2Cu,  -0.18,  0.00, 6.063, 4.872, I4/mcm
"""

PARAMS_JSON = json.dumps({
    "method": "DFT-PBE",
    "code": "VASP 6.4",
    "encut_eV": 520,
    "kpoints": "8x8x8 Monkhorst-Pack",
    "smearing": "Methfessel-Paxton, 0.2 eV",
}, indent=2)


# ---------------------------------------------------------------------------
# Demo functions
# ---------------------------------------------------------------------------

def demo_health(client: BackendClient, api_url: str) -> None:
    heading("Health Check")
    step(f"GET {api_url}/health")
    import httpx
    resp = httpx.get(f"{api_url}/health", timeout=15.0)
    if resp.status_code == 200:
        ok(f"status={resp.status_code}  body={resp.text.strip()}")
    else:
        fail(f"status={resp.status_code}  body={resp.text[:200]}")


def demo_stream(client: BackendClient) -> str:
    heading("Stream Workflow (data -> Globus endpoint)")

    # Create stream
    step("Creating stream: 'Staging Test - Alloy DFT Data'")
    result = client.stream_create(
        title="Staging Test - Alloy DFT Data",
        organization="MDF Staging Test",
    )
    result = check(result, "Stream created")
    stream_id = result["stream_id"]
    print(f"    stream_id: {stream_id}")

    # Upload CSV
    step("Uploading calculations.csv (intermetallic formation energies)")
    up = client.stream_upload(stream_id, "calculations.csv", SAMPLE_CSV.encode())
    check(up, "calculations.csv uploaded")
    if up.get("file"):
        print(f"    stored at: {up['file'].get('path', '?')}")
        print(f"    backend:   {up['file'].get('storage_backend', '?')}")

    # Upload params
    step("Uploading parameters.json (DFT settings)")
    up = client.stream_upload(stream_id, "parameters.json", PARAMS_JSON.encode())
    check(up, "parameters.json uploaded")

    # List files
    step("Listing stream files")
    files_result = client.stream_list_files(stream_id)
    if files_result.get("success"):
        files = files_result.get("files", [])
        print(f"    {len(files)} file(s):")
        for f in files:
            size = f.get("size_bytes", 0)
            print(f"      - {f.get('filename'):30s}  {size:>6d} bytes  ({f.get('storage_backend', '?')})")
            if f.get("download_url"):
                print(f"        url: {f['download_url'][:80]}...")
    else:
        print(f"    (list_files returned: {files_result})")

    # Stream status
    step("Checking stream status")
    ss = client.stream_status(stream_id)
    if ss.get("success"):
        st = ss["stream"]
        print(f"    status:      {st.get('status')}")
        print(f"    file_count:  {st.get('file_count')}")
        print(f"    total_bytes: {st.get('total_bytes')}")

    # Snapshot -> creates a submission from the stream
    step("Creating snapshot (stream -> dataset submission)")
    snap = client.stream_snapshot(stream_id, title="Alloy DFT Snapshot - Staging Test")
    snap = check(snap, "Snapshot created")
    snap_source_id = snap["source_id"]
    print(f"    source_id: {snap_source_id}")
    print(f"    version:   {snap.get('version', '1.0')}")

    return snap_source_id


def demo_publish(client: BackendClient) -> str:
    heading("Repository Workflow (init -> add -> commit -> publish)")

    work_dir = Path(tempfile.mkdtemp(prefix="mdf_staging_"))

    try:
        # Write test data
        (work_dir / "calculations.csv").write_text(SAMPLE_CSV)
        (work_dir / "parameters.json").write_text(PARAMS_JSON)

        from mdf_agent.core.agent import MDFAgent

        # Init
        step(f"mdf init (in {work_dir.name})")
        agent = MDFAgent.init(
            path=str(work_dir),
            title="Staging Test - Binary Intermetallic Alloys",
            authors=["Demo User"],
            description="Formation energies for Al-X intermetallics (staging test).",
        )
        ok("Repository initialized")

        # Add + commit
        step("mdf add calculations.csv parameters.json")
        staged = agent.add("calculations.csv", "parameters.json")
        ok(f"Staged {len(staged)} files")

        step("mdf commit -m 'Initial data'")
        agent.commit("Initial data")
        ok("Committed")

        # Validate
        step("mdf validate")
        val = agent.validate()
        errors = val.get("errors", [])
        warnings = val.get("warnings", [])
        if errors:
            fail(f"Validation errors: {errors}")
        ok(f"Passed ({len(warnings)} warning(s))")

        # Dry run
        step("mdf publish (dry run)")
        payload = agent.build_submission(test=True)
        ds = payload.get("data_sources", [])
        print(f"    title:        {payload.get('title')}")
        print(f"    authors:      {payload.get('authors')}")
        print(f"    data_sources: {len(ds)} file(s)")
        for src in ds:
            print(f"      - {src}")
        ok("Dry run payload built (data_sources auto-populated from commits)")

        # Submit to staging backend
        step("Submitting to staging backend")
        result = client.submit(payload)
        result = check(result, "Published")
        source_id = result["source_id"]
        print(f"    source_id: {source_id}")
        print(f"    version:   {result.get('version', '1.0')}")
        return source_id

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def demo_discovery(client: BackendClient, source_ids: list[str]) -> None:
    heading("Discovery (search -> card -> citation)")

    step("Searching for 'staging test'")
    result = client.search("staging test", limit=5)
    hits = result.get("results", [])
    print(f"    {len(hits)} result(s)")
    for h in hits:
        print(f"      - [{h.get('source_id')}] {h.get('title', '?')[:60]}")

    for sid in source_ids:
        step(f"Dataset card: {sid}")
        card_result = client.get_card(sid)
        if card_result.get("success"):
            card = card_result["card"]
            print(f"    title:   {card.get('title')}")
            print(f"    status:  {card.get('status')}")
            print(f"    authors: {card.get('authors')}")
            if card.get("doi"):
                print(f"    doi:     {card['doi']}")
        else:
            print(f"    (card not available: {card_result.get('error', '?')})")

        step(f"Citation: {sid}")
        cite = client.get_citation(sid, format="all")
        if cite.get("success"):
            if cite.get("bibtex"):
                print(f"    BibTeX:\n      {cite['bibtex'][:120]}...")
            if cite.get("apa"):
                print(f"    APA: {cite['apa'][:120]}...")
        else:
            print(f"    (citation not available: {cite.get('error', '?')})")
        break  # Just show first


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python examples/demo_staging_globus.py <staging-api-url>")
        print()
        print("Example:")
        print("  python examples/demo_staging_globus.py https://abc123.execute-api.us-east-1.amazonaws.com/staging")
        print()
        print("Get the URL after deploying:")
        print("  aws cloudformation describe-stacks \\")
        print("    --stack-name mdf-connect-v2-staging \\")
        print("    --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \\")
        print("    --output text")
        sys.exit(1)

    api_url = sys.argv[1].rstrip("/")

    print()
    print("MDF Connect v2 -- Staging + Globus Auth Demo")
    print(f"API: {api_url}")
    print()

    # Create authenticated client (requests MDF Connect + data scopes;
    # will open browser for login if needed)
    step("Authenticating with Globus (MDF Connect + data.materialsdatafacility.org scopes)")
    client = BackendClient.authenticated(
        base_url=api_url,
        service_instance="prod",  # staging uses prod Globus auth
    )
    ok(f"Client ready -> {api_url}")

    # Debug: show token state and validate directly against Globus
    step("Token diagnostics")
    import httpx as _httpx
    print(f"    Bearer token: {client._token[:20]}...{client._token[-10:]}" if client._token else "    Bearer token: NONE")
    print(f"    Data token:   {client._globus_data_token[:20]}...{client._globus_data_token[-10:]}" if client._globus_data_token else "    Data token:   NONE")

    step("Validating Bearer token directly against Globus Auth")
    _r = _httpx.get(
        "https://auth.globus.org/v2/oauth2/userinfo",
        headers={"Authorization": f"Bearer {client._token}"},
        timeout=10.0,
    )
    print(f"    Globus userinfo (Bearer): {_r.status_code}")
    if _r.status_code == 200:
        _ui = _r.json()
        print(f"    sub={_ui.get('sub')}  email={_ui.get('email')}")
    else:
        print(f"    Response: {_r.text[:300]}")

    step("Validating Data token directly against Globus Auth")
    _r2 = _httpx.get(
        "https://auth.globus.org/v2/oauth2/userinfo",
        headers={"Authorization": f"Bearer {client._globus_data_token}"},
        timeout=10.0,
    )
    print(f"    Globus userinfo (Data):   {_r2.status_code}")

    step("Testing auth against staging API")
    _r3 = _httpx.get(
        f"{api_url}/submissions",
        headers={"Authorization": f"Bearer {client._token}"},
        timeout=15.0,
    )
    print(f"    GET /submissions -> {_r3.status_code}")
    if _r3.status_code != 200:
        print(f"    Response: {_r3.text[:300]}")

    try:
        # 1. Health check
        demo_health(client, api_url)

        # 2. Stream workflow (uploads to Globus endpoint)
        stream_source_id = demo_stream(client)

        # 3. Repository publish workflow
        publish_source_id = demo_publish(client)

        # 4. Discovery
        demo_discovery(client, [publish_source_id, stream_source_id])

        # Summary
        heading("Summary")
        print("  All steps completed successfully.\n")
        print(f"  Stream snapshot:    {stream_source_id}")
        print(f"  Repo submission:    {publish_source_id}")
        print(f"  API:                {api_url}")
        print(f"  Globus storage:     https://data.materialsdatafacility.org/tmp/staging/")
        print()
        print("  Files were uploaded to the Globus HTTPS endpoint.")
        print("  View them at: https://app.globus.org/file-manager")
        print("    Collection: MDF (82f1b5c6-6e9b-11e5-ba47-22000b92c6ec)")
        print("    Path:       /tmp/staging/")
        print()

    finally:
        client.close()


if __name__ == "__main__":
    main()
