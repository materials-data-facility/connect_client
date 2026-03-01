# MDF Development Notes

## Repository Structure

Two separate git repos living in one directory:

- **`mdf_client/`** — `connect_client` repo on GitHub (`materials-data-facility/connect_client`)
  - Branch: `mdf-agent`
  - Remote tracks `origin/master` (not `main`)
  - Client-side code: `src/mdf_agent/`
  - Tests: `tests/`

- **`mdf_client/cs/`** — `connect_server` repo on GitHub (`materials-data-facility/connect_server`)
  - Branch: `v2-backend-curation`
  - Separate `.git`, separate remotes
  - Backend code: `aws/v2/`
  - Tests: `aws/v2/test_v2_*.py` (colocated with source, not in a tests/ dir)
  - SAM template + deploy: `aws/template.yaml`, `aws/deploy.sh`, `aws/samconfig.toml`

**Gotcha:** `git status` in the root shows `cs/` as untracked — it's a nested repo, not a submodule. Commits must be done separately in each directory.

## Deployment

- **Dev:** Self-contained, no Globus needed. `AUTH_MODE=dev` uses `X-User-Id` headers.
- **Staging:** `AUTH_MODE=production`, Globus token introspection. Globus creds from SSM (`/mdf/globus-client-id`, `/mdf/globus-client-secret`). DataCite creds from `samconfig.toml` (not SSM yet). `AllowAllCurators=true`.
- **Prod:** Same as staging but with real credentials from SSM.

Deploy flow: `cd cs/aws && sam build && ./deploy.sh staging`

Quick code-only deploy (skips CloudFormation): `./deploy.sh quick staging`

Staging API URL: `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging`

## DataCite Credentials

**Working test credentials:**
- Username: `Globus.TEST`
- Password: `NTroFAzElE`
- API: `https://api.test.datacite.org`
- Prefix: **`10.23677`** (NOT `10.26311` — that prefix is not assigned to this repository)

**How we found the right prefix:** The plan had `globus` / `fhy77$g3` — that repo doesn't exist on the test API (404). The correct repo is `Globus.TEST`. We then got 403 because the plan's prefix `10.26311` wasn't assigned to it. Queried the DataCite API to discover the actual prefix:
```
GET https://api.test.datacite.org/prefixes?client-id=globus.test
→ prefix: 10.23677
```

**Lesson:** Always verify DataCite prefix assignment before assuming credentials work. The repository ID, password, AND prefix must all be correct together.

## Auth / Backend Auth Flow

The staging backend validates tokens by calling `globus_sdk.AuthClient.userinfo()` with the bearer token. It accepts **any valid Globus access token** — no scope restriction. So the native app's `auth.globus.org` openid token works fine.

The `BackendClient.authenticated()` method handles the full flow:
1. Checks for explicit token / env var
2. Falls back to interactive Globus OAuth login (opens browser)
3. Caches tokens at `~/.config/mdf_agent/tokens.json`

For testing against staging, use `BackendClient.authenticated(base_url=STAGING_URL, service_instance="prod")`. The `service_instance="prod"` is needed because staging uses the same Globus auth as prod.

## Metadata Model Gotchas

- `license` field in `DatasetMetadata` is a dict (`{"name": "CC-BY-4.0", "identifier": "CC-BY-4.0"}`), NOT a string. Pydantic will reject a plain string.
- `authors` expects list of dicts with `name` key, not plain strings.
- The metadata model is at `cs/aws/v2/metadata.py` — check `DatasetMetadata` for the canonical schema.

## Status Lifecycle (v2)

```
submit → pending_curation → approved → published
                          ↘ rejected
```

Submissions now land directly as `pending_curation` (changed from `submitted`).
`ALLOWED_STATUSES` for manual updates: `{pending_curation, approved, published, rejected}`.

## Async Job Dispatch

Three modes controlled by `ASYNC_DISPATCH_MODE`:
- `inline` — synchronous, runs immediately (dev default, also used in API Lambda)
- `sqs` — queues to SQS, processed by AsyncWorkerFunction Lambda
- `sqlite` — local SQLite queue, processed by `run_sqlite_worker_once()` (tests)

Staging uses `sqs` for the API Lambda. The async worker Lambda always uses `inline` (it processes jobs directly from SQS events).

**Timing note:** When dispatch is `sqs`, the publish job is queued and the approve endpoint returns `status: "approved"`. The status changes to `published` after the async worker processes it (~15-20 seconds on staging). Tests that need to verify the final state should either use `inline` dispatch or poll.

## Testing Patterns

### Client tests (`tests/`)
- `conftest.py` adds `src/` to sys.path
- Pure pytest, class-based organization (`TestFoo`)
- Uses `tmp_path` fixture for temp dirs
- No mocking framework needed for URL normalization tests

### Server tests (`cs/aws/v2/test_v2_*.py`)
- Files colocated with source code (not in a separate tests/ dir)
- `sys.path.insert(0, ...)` to parent dir at top of file
- `FastAPI TestClient` for API tests
- Fixtures set up SQLite store + env vars via `monkeypatch`
- Must call `reset_storage_backend()` and `reset_middleware_state()` in fixtures
- `USE_MOCK_DATACITE=true` and `USE_MOCK_SEARCH=true` for tests

### Running tests
```bash
# Client tests
cd mdf_client && python -m pytest tests/test_submission_normalize.py -v

# Server tests
cd mdf_client/cs/aws && python -m pytest v2/test_v2_publish_pipeline.py -v

# All server v2 tests
cd mdf_client/cs/aws && python -m pytest v2/test_v2_*.py -v
```

## Ben's Preferences

- Commit and push both repos separately (different commit messages per repo)
- Include all pre-existing changes when committing (don't cherry-pick just session changes)
- Exclude build artifacts (`.aws-sam/`, `.DS_Store`)
- Test with real deployed infrastructure when possible (staging E2E)
- Use the mdf_agent client for testing against staging (not raw curl with manually obtained tokens)
- Fix forward: when credentials are wrong, debug systematically (check repo exists, check prefix assignment) rather than guessing
- Practical over theoretical: validate things work against real APIs before wiring into infrastructure

## Globus Search Integration (completed 2026-02-07)

### Search Index
- Test index UUID: `ab19b80b-0887-4337-b9f8-b8cc7feb1fdc`
- Both `SearchIndexUUID` and `TestSearchIndexUUID` in samconfig.toml point to this index for staging
- Writer role granted to confidential app via `cs/aws/v2/scripts/grant_search_index_role.py`
- `USE_MOCK_SEARCH` is NOT a CloudFormation parameter — it's auto-derived from `AuthMode` via `!If [IsDevAuth, "true", "false"]`. For staging (`AuthMode=production`), it's already `"false"`. Do NOT add `UseMockSearch` to samconfig — it would break the deploy.

### Bugs Fixed in Search Ingest
1. **Missing `requested_scopes`:** `oauth2_client_credentials_tokens()` must explicitly request `urn:globus:auth:scope:search.api.globus.org:all`. Without it, `by_resource_server` has no `search.api.globus.org` entry and `access_token` is `None`.
2. **Pydantic `License` not JSON-serializable:** `meta.license` is a `License(BaseModel)`, not a string. The `simplejson` serializer in globus_sdk chokes on it. Extract `.identifier` or `.name` before putting it in the GMetaEntry.

### Search Fallback Behavior
- `search.py:search_datasets()` tries Globus Search first, silently falls back to DynamoDB scan on any exception. This means the `/search` endpoint can return results even when Globus Search ingest is broken — the results just come from DynamoDB instead.
- To verify entries are actually in the Globus Search index: use `cs/aws/v2/scripts/test_search_token.py` or query the index directly with `globus_sdk.SearchClient`.

### Debugging Lambda Logs
```bash
# List recent async worker log streams
aws logs describe-log-streams \
  --log-group-name "/aws/lambda/mdf-connect-v2-staging-AsyncWorkerFunction-3t7JsGf6A6Ci" \
  --region us-east-1 --order-by LastEventTime --descending --limit 3

# Read events from a specific stream
aws logs get-log-events \
  --log-group-name "/aws/lambda/mdf-connect-v2-staging-AsyncWorkerFunction-3t7JsGf6A6Ci" \
  --log-stream-name '2026/02/07/[$LATEST]<stream-id>' \
  --region us-east-1
```

The API function log group is `mdf-connect-v2-staging-ApiFunction-UciD01YjsOEN`. Search ingest errors appear in the **async worker** logs (not the API logs), since publish runs via SQS.

## v1 Search Index Interaction

The v1 code (`submit.py`, `automate_manager.py`) also reads `SEARCH_INDEX_UUID` and passes it into Globus Automate flows. Setting both `SearchIndexUUID` and `TestSearchIndexUUID` to the same test index ensures all code paths — v1 Flows and v2 async worker — converge on the same index.

## Operational Scripts

- `cs/aws/v2/scripts/grant_search_index_role.py` — One-shot: grant writer role on search index to the confidential app. Uses interactive Globus login, resolves client ID from SSM.
- `cs/aws/v2/scripts/setup_datacite_ssm.sh` — Interactive: create SSM params for production DataCite credentials. `deploy.sh` reads these automatically.
- `cs/aws/v2/scripts/test_search_token.py` — Diagnostic: verify the confidential app can obtain a search token and query the index.

## What's Left (as of 2026-02-07)

- PRs for both repos
- DataCite SSM parameters not yet created for prod (staging uses samconfig.toml values directly)
- Production search index (separate from test index) — create when ready for prod
- Production CORS is configured (`template.yaml`) but not yet deployed to a prod stack

## Ben's Preferences (updated)

- **Always use `mdf_agent` / `BackendClient` for scripts and testing** — it handles auth automatically. Don't use raw `globus_sdk`, raw `curl`, or manually-obtained tokens. If a capability is missing from the client, add it there rather than working around it.
- When something fails silently (like search ingest), **check the Lambda logs first** — the async worker logs show the actual exception. The API logs won't show async job failures.
- **Audit for serialization safety** when building dicts that will be JSON-serialized — Pydantic models, datetime objects, and custom classes all need explicit conversion.
- **Test against real infrastructure** (staging E2E with real Globus Search, real DataCite) rather than assuming mock behavior matches. The mock search client silently succeeds while real Globus Search has auth, scope, and serialization requirements.
- **Deploy frequently and check logs** — don't batch multiple untested changes. Fix one thing, deploy, verify, then move on.
- Prefer diagnostic scripts that can be re-run (`test_search_token.py`) over one-off debug commands.
- When debugging token/auth issues: the scope must be (1) configured on the Globus app registration, (2) explicitly requested in `requested_scopes`, and (3) the app must have the right role on the target resource (e.g., writer on the search index).
