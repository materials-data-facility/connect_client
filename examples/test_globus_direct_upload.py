#!/usr/bin/env python3
"""Direct Globus HTTPS upload test — no server in the middle.

Authenticates with Globus, gets a data token for data.materialsdatafacility.org,
and tries a simple HTTPS PUT to /tmp/staging/ on the NCSA endpoint.

Usage:
    python examples/test_globus_direct_upload.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import httpx
from mdf_agent.auth.globus import (
    DATA_MDF_SCOPE,
    MDF_CONNECT_SCOPE,
    NCSA_MDF_COLLECTION_UUID,
    get_authorizer_for_scopes,
)

HTTPS_SERVER = "data.materialsdatafacility.org"
BASE_PATH = "/tmp/staging"
TEST_FILENAME = "mdf_upload_test.txt"
TEST_CONTENT = b"Hello from MDF Agent direct upload test!\n"


def extract_token(authorizer) -> str:
    h = authorizer.get_authorization_header()
    return h.removeprefix("Bearer ")


def main():
    print("1. Authenticating with Globus...")
    authorizers = get_authorizer_for_scopes([MDF_CONNECT_SCOPE, DATA_MDF_SCOPE])

    print(f"   Authorizers obtained for: {list(authorizers.keys())}")

    data_auth = authorizers.get(NCSA_MDF_COLLECTION_UUID)
    if not data_auth:
        print("   ERROR: No authorizer for data.materialsdatafacility.org")
        print("   Try: rm ~/.config/mdf_agent/tokens.json && mdf login --service prod")
        sys.exit(1)

    data_token = extract_token(data_auth)
    print(f"   Data token: {data_token[:20]}...{data_token[-10:]}")

    # Also show auth.globus.org token for comparison
    auth_auth = authorizers.get("auth.globus.org")
    if auth_auth:
        auth_token = extract_token(auth_auth)
        print(f"   Auth token: {auth_token[:20]}...{auth_token[-10:]}")

    url = f"https://{HTTPS_SERVER}{BASE_PATH}/{TEST_FILENAME}"
    print(f"\n2. Uploading to: {url}")

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.put(
            url,
            content=TEST_CONTENT,
            headers={
                "Authorization": f"Bearer {data_token}",
                "Content-Type": "text/plain",
            },
        )
        print(f"   PUT response: {resp.status_code}")
        if resp.status_code >= 400:
            print(f"   Response body: {resp.text[:500]}")

        if resp.status_code < 300:
            print("\n3. Verifying with GET...")
            resp2 = client.get(
                url,
                headers={"Authorization": f"Bearer {data_token}"},
            )
            print(f"   GET response: {resp2.status_code}")
            if resp2.status_code == 200:
                print(f"   Content: {resp2.text}")
                print("\n   SUCCESS — upload and download both work!")
            else:
                print(f"   GET failed: {resp2.text[:300]}")

            print("\n4. Cleaning up...")
            resp3 = client.delete(
                url,
                headers={"Authorization": f"Bearer {data_token}"},
            )
            print(f"   DELETE response: {resp3.status_code}")


if __name__ == "__main__":
    main()
