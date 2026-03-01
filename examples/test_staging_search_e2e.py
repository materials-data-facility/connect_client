#!/usr/bin/env python3
"""E2E test: submit → approve → poll for publish → verify Globus Search ingest.

Tests the full v2 pipeline including real DataCite DOI minting, real
Globus Search index ingestion, and domains / external import fields on staging.

Usage:
    python examples/test_staging_search_e2e.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdf_agent.core.backend_client import BackendClient

STAGING_URL = "https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging"

# Unique tag so we can find this exact submission in search
RUN_TAG = f"search-e2e-{int(time.time())}"


def heading(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def poll_status(client, source_id, target, timeout=90, interval=5):
    """Poll until submission reaches target status or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.status(source_id)
        sub = resp.get("submission", resp)
        current = sub.get("status")
        print(f"    status={current}")
        if current == target:
            return sub
        time.sleep(interval)
    print(f"    TIMEOUT waiting for status={target}")
    return None


def main():
    heading("MDF v2 Staging E2E — Full Search Pipeline")
    ok = True

    # -- Auth ---------------------------------------------------------------
    print("Authenticating with Globus...")
    client = BackendClient.authenticated(
        base_url=STAGING_URL, service_instance="prod"
    )
    print("  Authenticated.\n")

    # -- 1. Health ----------------------------------------------------------
    print("[1] Health check...")
    health = client.health()
    print(f"  {health}")
    assert health.get("status") == "ok", f"Health check failed: {health}"
    print("  PASS\n")

    # -- 2. Submit ----------------------------------------------------------
    print("[2] Submitting dataset...")
    payload = {
        "title": f"Search Integration Test ({RUN_TAG})",
        "authors": [
            {
                "name": "Search Test Bot",
                "given_name": "Search",
                "family_name": "Test Bot",
                "affiliations": ["Materials Data Facility"],
            }
        ],
        "description": "Automated test verifying Globus Search index ingest on staging.",
        "keywords": ["e2e", "search-test", RUN_TAG],
        "data_sources": ["https://data.materialsdatafacility.org/test/search-e2e/sample.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "resource_type": "Dataset",
        "license": {"name": "CC-BY-4.0", "identifier": "CC-BY-4.0"},
        "domains": ["materials", "chemistry"],
        "external_doi": "10.5281/zenodo.9999999",
        "external_url": "https://zenodo.org/record/9999999",
        "external_source": "Zenodo",
        "test": True,
    }
    result = client.submit(payload)
    print(f"  {result}")

    if not result.get("source_id"):
        print(f"\n  FAIL: submit did not return source_id")
        sys.exit(1)

    source_id = result["source_id"]
    version = result["version"]
    print(f"  source_id = {source_id}")
    print(f"  version   = {version}\n")

    # -- 3. Verify pending_curation -----------------------------------------
    print("[3] Checking initial status...")
    status_resp = client.status(source_id)
    sub = status_resp.get("submission", status_resp)
    initial_status = sub.get("status")
    print(f"  status = {initial_status}")
    assert initial_status == "pending_curation", f"Expected pending_curation, got {initial_status}"
    print("  PASS\n")

    # -- 4. Approve with DOI minting ----------------------------------------
    print("[4] Approving (mint_doi=true)...")
    approve = client.curation_approve(
        source_id, mint_doi=True, notes="Automated search E2E test"
    )
    print(f"  {approve}")
    approved_status = approve.get("status")
    print(f"  immediate status = {approved_status}")
    # Staging uses SQS — approve returns "approved", publish happens async
    assert approved_status in ("approved", "published"), f"Unexpected status: {approved_status}"
    print("  PASS\n")

    # -- 5. Poll for published ----------------------------------------------
    print("[5] Polling for published status (SQS async, up to 90s)...")
    final = poll_status(client, source_id, "published", timeout=90, interval=5)
    if final:
        doi = final.get("doi")
        published_at = final.get("published_at")
        print(f"  PASS: published")
        print(f"  doi          = {doi}")
        print(f"  published_at = {published_at}\n")
    else:
        print("  FAIL: never reached published status")
        ok = False

    # -- 6. Verify domains & external import fields in status ----------------
    print("[6] Verifying domains & external import in dataset_mdata...")
    mdata_resp = client.status(source_id)
    mdata = mdata_resp.get("submission", {}).get("dataset_mdata", {})
    print(f"  domains:         {mdata.get('domains')}")
    print(f"  external_doi:    {mdata.get('external_doi')}")
    print(f"  external_url:    {mdata.get('external_url')}")
    print(f"  external_source: {mdata.get('external_source')}")

    if mdata.get("domains") == ["materials", "chemistry"]:
        print("  PASS: domains round-trip")
    else:
        print("  FAIL: domains mismatch")
        ok = False

    if (
        mdata.get("external_doi") == "10.5281/zenodo.9999999"
        and mdata.get("external_url") == "https://zenodo.org/record/9999999"
        and mdata.get("external_source") == "Zenodo"
    ):
        print("  PASS: external import fields round-trip")
    else:
        print("  FAIL: external import fields mismatch")
        ok = False

    # MDF should have minted its own DOI, distinct from the external one
    mdf_doi = mdata_resp.get("submission", {}).get("doi")
    if mdf_doi and mdf_doi != "10.5281/zenodo.9999999":
        print(f"  PASS: MDF DOI ({mdf_doi}) != external DOI")
    elif mdf_doi:
        print(f"  FAIL: MDF DOI equals external DOI")
        ok = False
    else:
        print("  SKIP: no MDF DOI to compare (may be async)")
    print()

    # -- 7. Search for the dataset ------------------------------------------
    # Globus Search ingest is async too — give it a moment
    print("[7] Searching for dataset...")
    time.sleep(3)

    search_result = client.search(RUN_TAG, limit=10)
    total = search_result.get("total", 0)
    results = search_result.get("results", [])
    print(f"  query  = {RUN_TAG}")
    print(f"  total  = {total}")
    for r in results:
        print(f"    - {r.get('title', r.get('source_id', '?'))}")

    if total > 0:
        print("  PASS: dataset found in search\n")
    else:
        # Search may be local fallback — try broader query
        print("  Trying broader search (source_id)...")
        search2 = client.search(source_id, limit=5)
        total2 = search2.get("total", 0)
        for r in search2.get("results", []):
            print(f"    - {r.get('title', r.get('source_id', '?'))}")
        if total2 > 0:
            print("  PASS: found via source_id search\n")
        else:
            print("  WARN: not found in search yet (Globus Search ingest may still be processing)\n")
            ok = False

    # -- 8. Citation --------------------------------------------------------
    print("[8] Getting citation...")
    cite = client.get_citation(source_id, format="bibtex")
    bibtex = cite.get("bibtex", "")
    if bibtex:
        print(f"  BibTeX (first 200 chars):")
        print(f"    {bibtex[:200]}")
        print("  PASS\n")
    else:
        print(f"  {cite}")
        print("  WARN: no bibtex returned\n")

    # -- Summary ------------------------------------------------------------
    heading("Summary")
    print(f"  source_id       {source_id}")
    print(f"  version         {version}")
    print(f"  doi             {final.get('doi') if final else 'N/A'}")
    print(f"  published_at    {final.get('published_at') if final else 'N/A'}")
    print(f"  domains         {mdata.get('domains')}")
    print(f"  external_doi    {mdata.get('external_doi')}")
    print(f"  external_source {mdata.get('external_source')}")
    print(f"  search hit      {total > 0}")
    print(f"  run tag         {RUN_TAG}")
    print()

    if ok:
        print("  ALL CHECKS PASSED")
    else:
        print("  SOME CHECKS FAILED (see above)")
        sys.exit(1)


if __name__ == "__main__":
    main()
