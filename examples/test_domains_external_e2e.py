#!/usr/bin/env python3
"""E2E test of domains and external import fields on staging.

Tests:
  [1] Submit dataset with domains field → verify it appears in status
  [2] Submit externally-imported dataset → verify external_doi, external_url,
      external_source appear in status
  [3] Submit dataset with both domains and external import → verify both

Usage:
    python examples/test_domains_external_e2e.py
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


def step(num, title):
    print(f"[{num}] {title}")


def main():
    heading("MDF v2 Staging E2E — Domains & External Import")

    print("Authenticating with Globus...")
    client = BackendClient.authenticated(base_url=STAGING_URL, service_instance="prod")
    print("  Authenticated.\n")

    # ── Step 1: Submit with domains ─────────────────────────────────────
    step(1, "Submit dataset with domains=['materials', 'chemistry']")

    result1 = client.submit({
        "title": "Domains E2E Test Dataset",
        "authors": [
            {"name": "MDF Test Suite", "given_name": "MDF", "family_name": "Test Suite"},
        ],
        "description": "E2E test: domains field.",
        "keywords": ["test", "e2e", "domains"],
        "data_sources": ["https://data.materialsdatafacility.org/test/e2e/domains.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "domains": ["materials", "chemistry"],
    })
    if not result1.get("success"):
        print(f"  FAIL: {result1}")
        sys.exit(1)

    sid1 = result1["source_id"]
    print(f"  Source ID: {sid1}")
    print(f"  Version:   {result1['version']}")

    status1 = client.status(sid1)
    sub1 = status1.get("submission", {})
    print(f"  Status:  {sub1.get('status')}")
    print(f"  Domains: {sub1.get('domains')}")
    print()

    # ── Step 2: Submit with external import fields ──────────────────────
    step(2, "Submit externally-imported dataset")

    result2 = client.submit({
        "title": "External Import E2E Test Dataset",
        "authors": [
            {"name": "MDF Test Suite", "given_name": "MDF", "family_name": "Test Suite"},
        ],
        "description": "E2E test: external import from Zenodo.",
        "keywords": ["test", "e2e", "external-import"],
        "data_sources": ["https://data.materialsdatafacility.org/test/e2e/external.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "external_doi": "10.5281/zenodo.1234567",
        "external_url": "https://zenodo.org/record/1234567",
        "external_source": "Zenodo",
    })
    if not result2.get("success"):
        print(f"  FAIL: {result2}")
        sys.exit(1)

    sid2 = result2["source_id"]
    print(f"  Source ID: {sid2}")
    print(f"  Version:   {result2['version']}")

    status2 = client.status(sid2)
    sub2 = status2.get("submission", {})
    print(f"  Status:          {sub2.get('status')}")
    print(f"  external_doi:    {sub2.get('external_doi')}")
    print(f"  external_url:    {sub2.get('external_url')}")
    print(f"  external_source: {sub2.get('external_source')}")
    print()

    # ── Step 3: Submit with both domains and external import ────────────
    step(3, "Submit dataset with both domains and external import")

    result3 = client.submit({
        "title": "Combined Domains + External Import E2E Test",
        "authors": [
            {"name": "MDF Test Suite", "given_name": "MDF", "family_name": "Test Suite"},
        ],
        "description": "E2E test: both domains and external import fields.",
        "keywords": ["test", "e2e", "combined"],
        "data_sources": ["https://data.materialsdatafacility.org/test/e2e/combined.csv"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "domains": ["biology"],
        "external_doi": "10.5061/dryad.abc123",
        "external_url": "https://datadryad.org/stash/dataset/abc123",
        "external_source": "Dryad",
    })
    if not result3.get("success"):
        print(f"  FAIL: {result3}")
        sys.exit(1)

    sid3 = result3["source_id"]
    print(f"  Source ID: {sid3}")
    print(f"  Version:   {result3['version']}")

    status3 = client.status(sid3)
    sub3 = status3.get("submission", {})
    print(f"  Status:          {sub3.get('status')}")
    print(f"  Domains:         {sub3.get('domains')}")
    print(f"  external_doi:    {sub3.get('external_doi')}")
    print(f"  external_url:    {sub3.get('external_url')}")
    print(f"  external_source: {sub3.get('external_source')}")
    print()

    # ── Summary ─────────────────────────────────────────────────────────
    heading("Assertions")

    checks = [
        ("Test 1: domains round-trip",
         sub1.get("domains") == ["materials", "chemistry"]),
        ("Test 2: external_doi round-trip",
         sub2.get("external_doi") == "10.5281/zenodo.1234567"),
        ("Test 2: external_url round-trip",
         sub2.get("external_url") == "https://zenodo.org/record/1234567"),
        ("Test 2: external_source round-trip",
         sub2.get("external_source") == "Zenodo"),
        ("Test 3: combined domains",
         sub3.get("domains") == ["biology"]),
        ("Test 3: combined external_doi",
         sub3.get("external_doi") == "10.5061/dryad.abc123"),
        ("Test 3: combined external_source",
         sub3.get("external_source") == "Dryad"),
    ]

    for label, passed in checks:
        mark = "PASS" if passed else "FAIL"
        print(f"  [{mark}] {label}")

    passed_count = sum(1 for _, p in checks if p)
    print(f"\n  {passed_count}/{len(checks)} checks passed\n")


if __name__ == "__main__":
    main()
