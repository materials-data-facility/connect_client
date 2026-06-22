#!/usr/bin/env python3
"""Check whether a submission made it into search.

Uses the mdf_agent BackendClient (handles auth automatically).

Usage:
    python examples/check_search_index.py
    python examples/check_search_index.py --query "search-e2e"
    python examples/check_search_index.py --source-id mdf-ee1868394a384d048a5ded436fc306a9
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdf_agent.core.backend_client import BackendClient

STAGING_URL = "https://3xicgt0g7l.execute-api.us-east-1.amazonaws.com/staging"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default=None, help="Search query")
    parser.add_argument("--source-id", default=None, help="Look up specific source_id")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    if not args.query and not args.source_id:
        parser.print_help()
        sys.exit(1)

    client = BackendClient.authenticated(base_url=STAGING_URL, service_instance="prod")

    if args.source_id:
        print(f"Status for {args.source_id}:")
        status = client.status(args.source_id)
        print(json.dumps(status, indent=2, default=str))

    if args.query:
        print(f"\nSearch: {args.query!r}")
        results = client.search(args.query, limit=args.limit)
        print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
