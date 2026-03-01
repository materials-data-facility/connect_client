#!/usr/bin/env python3
"""E2E test of the v2 publication pipeline on staging.

Runs: submit → verify pending_curation → approve (mint_doi=true) → verify published + DOI

Usage:
    python examples/test_staging_e2e.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdf_agent.core.backend_client import BackendClient

STAGING_URL = "https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging"


def heading(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def main():
    heading("MDF v2 Staging E2E — Publication Pipeline")

    # Authenticate via Globus (will open browser if needed)
    print("Authenticating with Globus...")
    client = BackendClient.authenticated(base_url=STAGING_URL, service_instance="prod")
    print("  Authenticated.\n")

    # Step 1: Health check
    print("[1] Health check...")
    health = client._request("GET", "/health")
    print(f"  {health}\n")

    # Step 2: Submit
    print("[2] Submitting dataset...")
    result = client.submit({
        "title": "E2E Pipeline Test — Real DOI Minting",
        "authors": [
            {"name": "MDF Test Suite", "given_name": "MDF", "family_name": "Test Suite", "affiliation": "Materials Data Facility"},
        ],
        "description": "Automated E2E test of the v2 publication pipeline with real DataCite DOI minting on staging.",
        "keywords": ["test", "e2e", "publication-pipeline", "datacite"],
        "data_sources": ["https://data.materialsdatafacility.org/test/e2e/sample.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "license": {"name": "CC-BY-4.0", "identifier": "CC-BY-4.0"},
    })
    print(f"  Result: {result}")

    if not result.get("success"):
        print(f"\n  Submit failed: {result}")
        sys.exit(1)

    source_id = result["source_id"]
    version = result["version"]
    print(f"  Source ID: {source_id}")
    print(f"  Version:  {version}\n")

    # Step 3: Check status — should be pending_curation
    print("[3] Checking status...")
    status = client.status(source_id)
    current_status = status.get("submission", {}).get("status")
    print(f"  Status: {current_status}")
    assert current_status == "pending_curation", f"Expected pending_curation, got {current_status}"
    print("  OK: status is pending_curation\n")

    # Step 4: Approve with DOI minting
    print("[4] Approving submission (mint_doi=true)...")
    approve_result = client._request("POST", f"/curation/{source_id}/approve", json_data={
        "mint_doi": True,
        "notes": "Automated E2E test approval",
    })
    print(f"  Result: {approve_result}\n")

    # Step 5: Check final status — should be published with DOI
    print("[5] Checking final status...")
    final_status = client.status(source_id)
    submission = final_status.get("submission", {})
    print(f"  Status:       {submission.get('status')}")
    print(f"  DOI:          {submission.get('doi')}")
    print(f"  Published at: {submission.get('published_at')}")

    # Step 6: Get citation
    print("\n[6] Getting citation...")
    citation = client.get_citation(source_id, format="bibtex")
    if citation.get("success"):
        print(f"  BibTeX:\n    {citation.get('bibtex', 'N/A')[:200]}")
    else:
        print(f"  Citation: {citation}")

    # Step 7: Search
    print("\n[7] Searching for dataset...")
    search = client.search("E2E Pipeline Test")
    print(f"  Total results: {search.get('total', 0)}")
    for r in search.get("results", [])[:3]:
        print(f"    - {r.get('title')} (status: {r.get('status')})")

    heading("Summary")
    print(f"  Source ID:  {source_id}")
    print(f"  Status:     {submission.get('status')}")
    print(f"  DOI:        {submission.get('doi')}")
    print(f"  Published:  {submission.get('published_at')}")
    print()


if __name__ == "__main__":
    main()
