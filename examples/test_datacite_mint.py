#!/usr/bin/env python3
"""Standalone DataCite test DOI minting script.

Tests the DataCiteClient directly against api.test.datacite.org.
No server, no Lambda, no DynamoDB -- just the DataCite REST API.

Usage:
    python examples/test_datacite_mint.py
"""

import sys
import os
import time

# Add cs/aws to path so we can import v2.datacite
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "cs", "aws"))

from v2.datacite import DataCiteClient


def main():
    # Test credentials for api.test.datacite.org
    client = DataCiteClient(
        username="globus",
        password="fhy77$g3",
        api_url="https://api.test.datacite.org",
        prefix="10.18126",
        test_mode=True,
    )

    timestamp = int(time.time())
    source_id = f"mdf-test-{timestamp}"

    print(f"{'=' * 60}")
    print(f"  DataCite Test DOI Minting")
    print(f"  Source ID: {source_id}")
    print(f"  API: https://api.test.datacite.org")
    print(f"{'=' * 60}")

    # --- Step 1: Mint a DOI (publish=True → findable) ---
    print("\n[1] Minting DOI (publish=True)...")
    metadata = {
        "title": f"MDF Test Dataset {timestamp}",
        "authors": [
            {"given_name": "Test", "family_name": "User", "affiliation": "MDF"},
            {"name": "Materials Data Facility"},
        ],
        "description": "Automated test of DataCite DOI minting from MDF v2 pipeline.",
        "keywords": ["test", "mdf", "materials-data"],
        "license": "CC-BY-4.0",
        "publication_year": 2026,
    }

    result = client.mint_doi(source_id=source_id, metadata=metadata, publish=True)
    print(f"  Success:  {result.get('success')}")
    print(f"  DOI:      {result.get('doi')}")
    print(f"  URL:      {result.get('url')}")
    print(f"  State:    {result.get('state')}")

    if not result.get("success"):
        print(f"  Error:    {result.get('error')}")
        print("\nAborting -- mint failed.")
        client.close()
        sys.exit(1)

    doi = result["doi"]

    # --- Step 2: Verify via GET ---
    print("\n[2] Verifying DOI via GET...")
    fetched = client.get_doi(doi)
    if fetched and fetched.get("data"):
        attrs = fetched["data"]["attributes"]
        print(f"  DOI:      {fetched['data']['id']}")
        print(f"  State:    {attrs.get('state')}")
        print(f"  Title:    {attrs.get('titles', [{}])[0].get('title', 'N/A')}")
    else:
        print("  Warning: could not fetch DOI (may take a moment to propagate)")

    # --- Step 3: Update the DOI (new title) ---
    print("\n[3] Updating DOI with new title...")
    metadata["title"] = f"MDF Test Dataset {timestamp} (Updated)"
    update_result = client.mint_doi(source_id=source_id, metadata=metadata, publish=True)
    print(f"  Success:  {update_result.get('success')}")
    print(f"  Updated:  {update_result.get('updated', False)}")
    print(f"  State:    {update_result.get('state')}")

    # --- Step 4: Test draft mode ---
    print("\n[4] Testing draft mode (publish=False)...")
    draft_source_id = f"mdf-test-draft-{timestamp}"
    draft_result = client.mint_doi(
        source_id=draft_source_id,
        metadata={
            "title": f"MDF Draft Test {timestamp}",
            "authors": [{"name": "Test User"}],
        },
        publish=False,
    )
    print(f"  Success:  {draft_result.get('success')}")
    print(f"  DOI:      {draft_result.get('doi')}")
    print(f"  State:    {draft_result.get('state')}")

    # --- Step 5: Publish the draft ---
    if draft_result.get("success"):
        print("\n[5] Publishing the draft DOI...")
        publish_result = client.mint_doi(
            source_id=draft_source_id,
            metadata={
                "title": f"MDF Draft Test {timestamp} (Now Published)",
                "authors": [{"name": "Test User"}],
            },
            publish=True,
        )
        print(f"  Success:  {publish_result.get('success')}")
        print(f"  State:    {publish_result.get('state')}")

    # --- Summary ---
    print(f"\n{'=' * 60}")
    print("  Summary")
    print(f"{'=' * 60}")
    print(f"  Findable DOI:  {doi}")
    print(f"  Draft DOI:     {draft_result.get('doi', 'N/A')}")
    print(f"  Verify at:     https://commons.datacite.org/doi.org/{doi}")
    print(f"{'=' * 60}")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
