#!/usr/bin/env python3
"""E2E test of dataset versioning on staging.

Characterizes the versioning lifecycle:
  [1] Submit v1.0 → approve with mint_doi=True → record DOI, status
  [2] Submit v1.1 (update=True) → approve with mint_doi=False → inherits dataset DOI
  [3] Submit v1.2 (update=True) → approve with mint_doi=True → version-specific DOI
  [4] Query all 3 versions via status endpoint — compare doi fields
  [5] Search for dataset — check what version appears, what DOI is shown
  [6] Query DataCite test API for the DOI — check metadata
  [7] Print summary

Usage:
    python examples/test_versioning_e2e.py
"""

import json
import sys
from pathlib import Path
from urllib.parse import quote

import httpx

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdf_agent.core.backend_client import BackendClient

STAGING_URL = "https://3xicgt0g7l.execute-api.us-east-1.amazonaws.com/staging"
DATACITE_TEST_API = "https://api.test.datacite.org"


def heading(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def step(num, title):
    print(f"[{num}] {title}")


def check_datacite(doi):
    """Query DataCite test API for a DOI's metadata."""
    try:
        resp = httpx.get(
            f"{DATACITE_TEST_API}/dois/{quote(doi, safe='')}",
            timeout=15.0,
        )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            attrs = data.get("attributes", {})
            return {
                "found": True,
                "doi": data.get("id"),
                "state": attrs.get("state"),
                "title": (attrs.get("titles") or [{}])[0].get("title"),
                "creators": [c.get("name") for c in attrs.get("creators", [])],
                "relatedIdentifiers": attrs.get("relatedIdentifiers", []),
                "version": attrs.get("version"),
            }
        return {"found": False, "status_code": resp.status_code}
    except Exception as exc:
        return {"found": False, "error": str(exc)}


def main():
    heading("MDF v2 Staging E2E — Dataset Versioning")

    print("Authenticating with Globus...")
    client = BackendClient.authenticated(base_url=STAGING_URL, service_instance="prod")
    print("  Authenticated.\n")

    # ── Step 1: Submit v1.0 ──────────────────────────────────────────────
    step(1, "Submit v1.0 → approve with mint_doi=True")

    result = client.submit({
        "title": "Versioning E2E Test Dataset",
        "authors": [
            {"name": "Test Author v1.0", "given_name": "Test", "family_name": "Author v1.0"},
        ],
        "description": "v1.0 of the versioning E2E test dataset.",
        "keywords": ["test", "versioning", "e2e"],
        "data_sources": ["https://data.materialsdatafacility.org/test/versioning/v1.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "license": {"name": "CC-BY-4.0", "identifier": "CC-BY-4.0"},
    })
    if not result.get("success"):
        print(f"  FAIL: Submit v1.0 failed: {result}")
        sys.exit(1)

    source_id = result["source_id"]
    print(f"  Source ID: {source_id}")
    print(f"  Version:   {result['version']}")

    approve = client._request("POST", f"/curation/{source_id}/approve", json_data={
        "mint_doi": True,
        "notes": "E2E versioning test — v1.0 with DOI",
    })
    print(f"  Approve result: status={approve.get('status')}")

    v1_status = client.status(source_id, version="1.0")
    v1_sub = v1_status.get("submission", {})
    v1_doi = v1_sub.get("doi")
    v1_dataset_doi = v1_sub.get("dataset_doi")
    print(f"  v1.0 doi:         {v1_doi}")
    print(f"  v1.0 dataset_doi: {v1_dataset_doi}")
    print(f"  v1.0 status:      {v1_sub.get('status')}")
    print()

    # ── Step 2: Submit v1.1 (update, no new DOI) ────────────────────────
    step(2, "Submit v1.1 (update=True) → approve with mint_doi=False")

    result2 = client.submit({
        "title": "Versioning E2E Test Dataset — Updated",
        "authors": [
            {"name": "Test Author v1.1", "given_name": "Test", "family_name": "Author v1.1"},
        ],
        "description": "v1.1 of the versioning E2E test — updated metadata, inherits DOI.",
        "keywords": ["test", "versioning", "e2e", "updated"],
        "data_sources": ["https://data.materialsdatafacility.org/test/versioning/v1.1.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "update": True,
        "extensions": {"mdf_source_id": source_id},
    })
    if not result2.get("success"):
        print(f"  FAIL: Submit v1.1 failed: {result2}")
        sys.exit(1)
    print(f"  Version: {result2['version']}")

    approve2 = client._request("POST", f"/curation/{source_id}/approve", json_data={
        "mint_doi": False,
        "version": result2["version"],
        "notes": "E2E versioning test — v1.1, inherit DOI",
    })
    print(f"  Approve result: status={approve2.get('status')}")

    v11_status = client.status(source_id, version=result2["version"])
    v11_sub = v11_status.get("submission", {})
    v11_doi = v11_sub.get("doi")
    v11_dataset_doi = v11_sub.get("dataset_doi")
    print(f"  v1.1 doi:         {v11_doi}")
    print(f"  v1.1 dataset_doi: {v11_dataset_doi}")
    print(f"  v1.1 status:      {v11_sub.get('status')}")
    print()

    # ── Step 3: Submit v1.2 (update, mint version DOI) ──────────────────
    step(3, "Submit v1.2 (update=True) → approve with mint_doi=True")

    result3 = client.submit({
        "title": "Versioning E2E Test Dataset — v1.2 Release",
        "authors": [
            {"name": "Test Author v1.2", "given_name": "Test", "family_name": "Author v1.2"},
            {"name": "New Collaborator", "given_name": "New", "family_name": "Collaborator"},
        ],
        "description": "v1.2 of the versioning E2E test — new version DOI.",
        "keywords": ["test", "versioning", "e2e", "v1.2"],
        "data_sources": ["https://data.materialsdatafacility.org/test/versioning/v1.2.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "update": True,
        "extensions": {"mdf_source_id": source_id},
    })
    if not result3.get("success"):
        print(f"  FAIL: Submit v1.2 failed: {result3}")
        sys.exit(1)
    print(f"  Version: {result3['version']}")

    approve3 = client._request("POST", f"/curation/{source_id}/approve", json_data={
        "mint_doi": True,
        "version": result3["version"],
        "notes": "E2E versioning test — v1.2, version-specific DOI",
    })
    print(f"  Approve result: status={approve3.get('status')}")

    v12_status = client.status(source_id, version=result3["version"])
    v12_sub = v12_status.get("submission", {})
    v12_doi = v12_sub.get("doi")
    v12_dataset_doi = v12_sub.get("dataset_doi")
    print(f"  v1.2 doi:         {v12_doi}")
    print(f"  v1.2 dataset_doi: {v12_dataset_doi}")
    print(f"  v1.2 status:      {v12_sub.get('status')}")
    print()

    # ── Step 4: Query all versions ──────────────────────────────────────
    step(4, "Query all versions via status endpoint")

    for ver_label, ver in [("v1.0", "1.0"), ("v1.1", result2["version"]), ("v1.2", result3["version"])]:
        s = client.status(source_id, version=ver)
        sub = s.get("submission", {})
        print(f"  {ver_label}: status={sub.get('status')}, doi={sub.get('doi')}, dataset_doi={sub.get('dataset_doi')}")
    print()

    # ── Step 5: Search ──────────────────────────────────────────────────
    step(5, "Search for dataset")
    search = client.search("Versioning E2E Test Dataset")
    print(f"  Total results: {search.get('total', 0)}")
    for r in search.get("results", [])[:5]:
        print(f"    - {r.get('title')} (source_id={r.get('source_id')}, version={r.get('version')})")
    print()

    # ── Step 6: Check DataCite ──────────────────────────────────────────
    step(6, "Query DataCite test API")

    if v1_doi:
        dc_dataset = check_datacite(v1_doi)
        print(f"  Dataset DOI ({v1_doi}):")
        print(f"    found: {dc_dataset.get('found')}")
        print(f"    title: {dc_dataset.get('title')}")
        print(f"    creators: {dc_dataset.get('creators')}")
        print(f"    relatedIdentifiers: {dc_dataset.get('relatedIdentifiers')}")
    else:
        print("  No dataset DOI to check.")

    if v12_doi and v12_doi != v1_doi:
        dc_version = check_datacite(v12_doi)
        print(f"  Version DOI ({v12_doi}):")
        print(f"    found: {dc_version.get('found')}")
        print(f"    title: {dc_version.get('title')}")
        print(f"    relatedIdentifiers: {dc_version.get('relatedIdentifiers')}")
    else:
        print(f"  v1.2 DOI same as dataset DOI or not set ({v12_doi})")
    print()

    # ── Step 7: Summary ─────────────────────────────────────────────────
    heading("Summary")
    print(f"  Source ID:       {source_id}")
    print(f"  v1.0 doi:        {v1_doi}")
    print(f"  v1.0 dataset_doi:{v1_dataset_doi}")
    print(f"  v1.1 doi:        {v11_doi}")
    print(f"  v1.1 dataset_doi:{v11_dataset_doi}")
    print(f"  v1.2 doi:        {v12_doi}")
    print(f"  v1.2 dataset_doi:{v12_dataset_doi}")
    print()

    # Assertions for post-implementation verification
    heading("Assertions (post-implementation)")
    checks = []

    # v1.0: doi and dataset_doi should both be set and equal
    if v1_doi and v1_dataset_doi and v1_doi == v1_dataset_doi:
        checks.append(("v1.0: doi == dataset_doi", True))
    elif v1_doi and not v1_dataset_doi:
        checks.append(("v1.0: doi set but dataset_doi missing (pre-implementation)", False))
    else:
        checks.append(("v1.0: doi/dataset_doi mismatch", False))

    # v1.1: no version doi, but dataset_doi inherited
    if not v11_doi and v11_dataset_doi:
        checks.append(("v1.1: no doi, dataset_doi inherited", True))
    elif v11_doi:
        checks.append(("v1.1: unexpected doi set (pre-implementation may do this)", False))
    else:
        checks.append(("v1.1: no doi and no dataset_doi", False))

    # v1.2: version-specific doi different from dataset_doi
    if v12_doi and v12_dataset_doi and v12_doi != v12_dataset_doi:
        checks.append(("v1.2: version doi != dataset_doi", True))
    elif v12_doi and v12_doi == v12_dataset_doi:
        checks.append(("v1.2: version doi == dataset_doi (pre-implementation)", False))
    else:
        checks.append(("v1.2: missing doi or dataset_doi", False))

    for label, passed in checks:
        status_mark = "PASS" if passed else "FAIL"
        print(f"  [{status_mark}] {label}")
    print()

    passed_count = sum(1 for _, p in checks if p)
    print(f"  {passed_count}/{len(checks)} checks passed")
    print()


if __name__ == "__main__":
    main()
