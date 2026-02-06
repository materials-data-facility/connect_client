# MDF Connect v2 Review and Remediation

Date: February 6, 2026  
Scope: `cs/aws/v2` backend, related local tooling, and validation tests.

## What we reviewed

The v2 backend architecture and implementation were reviewed for:

- Security and authorization boundaries
- Data integrity across SQLite/DynamoDB code paths
- Storage/path safety
- Version and pagination correctness
- Local developer workflow drift

## High-impact issues found and corrected

### 1) Server token leakage in upload URL responses

**Issue:** Globus upload URL responses included an `Authorization` header built from server credentials.  
**Risk:** Credential exposure to clients.

**Fix implemented:**

- Removed server bearer token from upload URL responses.
- Upload URL now returns content headers plus `auth_type: bearer` hint.

**Files:**

- `cs/aws/v2/storage/globus_https.py`

---

### 2) Missing ownership checks on stream/submission mutations

**Issue:** Authenticated users could mutate resources they did not own (append, close, upload, status update, etc.).  
**Risk:** Unauthorized modification of other users' data.

**Fix implemented:**

- Added centralized owner-or-curator guard helpers.
- Enforced stream owner/curator checks on stream and file mutation routes.
- Enforced owner/curator checks on stream previews.
- Made submission status updates curator-only.
- Restricted org-wide submissions listing to curators.

**Files:**

- `cs/aws/v2/app/auth.py`
- `cs/aws/v2/app/routers/submissions.py`
- `cs/aws/v2/app/routers/streams.py`
- `cs/aws/v2/app/routers/files.py`
- `cs/aws/v2/app/routers/preview.py`

---

### 3) Path traversal and unsafe path handling

**Issue:** Storage path construction and reads accepted traversal-style inputs (for example `../`).  
**Risk:** Out-of-scope file read/write attempts in local storage and unsafe remote path usage.

**Fix implemented:**

- Added strict `stream_id` and `filename` sanitization in storage base class.
- Local storage now resolves and validates paths under base directory.
- Globus storage now rejects URL-style and traversal paths.
- File confirm/download routes now verify that paths belong to the requested stream.

**Files:**

- `cs/aws/v2/storage/base.py`
- `cs/aws/v2/storage/local.py`
- `cs/aws/v2/storage/globus_https.py`
- `cs/aws/v2/app/routers/files.py`

---

### 4) SQLite upsert dropped `dataset_profile`

**Issue:** `INSERT OR REPLACE` path omitted `dataset_profile`, which could erase profiling data during curation updates.  
**Risk:** Data loss and degraded preview/card behavior.

**Fix implemented:**

- Included `dataset_profile` in SQLite write/upsert SQL and serialization logic.

**Files:**

- `cs/aws/v2/store.py`

---

### 5) Incorrect "latest version" selection

**Issue:** Some paths used lexicographic version ordering, where `1.9` could beat `1.10`.  
**Risk:** Wrong version returned by default.

**Fix implemented:**

- Unified latest-version selection on semantic comparison (`latest_version(...)`).
- Updated generic store `get(...)` and status endpoints to use semantic version choice.

**Files:**

- `cs/aws/v2/store.py`
- `cs/aws/v2/app/routers/submissions.py`

---

### 6) Incomplete DynamoDB pagination

**Issue:** Single-page scans could miss matching records for status lists/search when datasets exceeded one page.  
**Risk:** Partial/incorrect query results.

**Fix implemented:**

- Added explicit pagination loops for:
  - Submission `list_by_status(...)`
  - Submission `list_all(...)`
  - Stream `list_all(...)`

**Files:**

- `cs/aws/v2/store.py`
- `cs/aws/v2/stream_store.py`

---

### 7) Local tooling drift (legacy local server assumptions)

**Issue:** Local scripts referenced removed `local_server.py` and old response shapes.  
**Risk:** Broken local workflows and onboarding friction.

**Fix implemented:**

- Updated `local_start.sh` to run the FastAPI app (`python -m v2.app.main`) from correct working directory.
- Added startup verification and log file output.
- Updated local test scripts to parse FastAPI JSON response format.
- Replaced broken `local_runner.py` with a working HTTP-based local command runner.
- Updated deployment docs to reflect router-based FastAPI endpoint model.

**Files:**

- `cs/aws/v2/local_start.sh`
- `cs/aws/v2/local_test.sh`
- `cs/aws/v2/local_test_stream.sh`
- `cs/aws/v2/local_runner.py`
- `cs/aws/DEPLOYMENT.md`

## Validation completed

- Static compile check:
  - `python -m compileall cs/aws/v2 src/mdf_agent/core/backend_client.py src/mdf_agent/cli/backend.py src/mdf_agent/cli/stream.py`
- Added and ran hardening tests:
  - `pytest -q cs/aws/v2/test_v2_hardening.py`
  - Result: `6 passed`

### New test coverage

`cs/aws/v2/test_v2_hardening.py` verifies:

- Semantic version latest selection (`1.10` over `1.9`)
- SQLite upsert preserves `dataset_profile`
- Local storage traversal rejection
- Globus upload URL does not expose server auth header
- Stream append requires ownership
- Status update requires curator privileges

## Remaining recommended future directions

1. Move profiling/DOI/indexing into async workers (queue-driven) to keep request latency predictable.
2. Add policy-driven ACL model for dataset visibility and org scope.
3. Expand API integration test suite for curation transitions, path validation edge cases, and Dynamo pagination behavior under larger datasets.
4. Replace in-process search with external indexed search synchronization.

---

## Additional remediation pass (append)

Date: February 6, 2026 (follow-up pass)

### A1) `upload-confirm` accepted non-existent paths

**Issue:** `POST /stream/{id}/upload-confirm` could update stream accounting even if file path did not exist.  
**Risk:** Inflated file stats and inconsistent metadata.

**Fix implemented:**

- Added existence check (`storage.get_file(path)`) before updating stream counts.

**Files:**

- `cs/aws/v2/app/routers/files.py`

---

### A2) Curation APIs defaulted to version `1.0`

**Issue:** Curation endpoints defaulted to `1.0` when version was omitted.  
**Risk:** Reviewing/approving/rejecting the wrong version for multi-version datasets.

**Fix implemented:**

- Added resolver that selects the latest semantic version when `version` is omitted.
- Updated `GET /curation/{source_id}`, approve, and reject flows to use resolver.
- Added fallback handling in pending-list title parsing for malformed metadata records.

**Files:**

- `cs/aws/v2/app/routers/curation.py`

---

### A3) `clone_url` token exfiltration risk to arbitrary hosts

**Issue:** Clone operations could send bearer token to any caller-supplied URL host.  
**Risk:** Credential leakage if a malicious URL is provided.

**Fix implemented:**

- Added strict HTTPS host validation for clone downloads.
- Tokens are now only sent to trusted host(s): configured `GLOBUS_HTTPS_SERVER` plus optional `GLOBUS_ALLOWED_HOSTS`.

**Files:**

- `cs/aws/v2/clone.py`

---

### A4) Remaining UTC deprecation warnings in touched paths

**Issue:** Multiple code paths used deprecated `datetime.utcnow()` usage.  
**Risk:** Runtime warnings and future Python compatibility risk.

**Fix implemented:**

- Switched touched paths to timezone-aware UTC timestamps (`datetime.now(timezone.utc)`).

**Files:**

- `cs/aws/v2/store.py`
- `cs/aws/v2/stream_store.py`
- `cs/aws/v2/app/routers/submissions.py`
- `cs/aws/v2/app/routers/streams.py`
- `cs/aws/v2/app/routers/files.py`
- `cs/aws/v2/storage/globus_https.py`

---

### A5) Deployment monitoring snippet still had stale function naming

**Issue:** Monitoring command assumed a static Lambda resource name pattern.  
**Risk:** Broken monitoring commands after stack/function name changes.

**Fix implemented:**

- Updated docs to fetch physical Lambda function name from CloudFormation resources first.

**Files:**

- `cs/aws/DEPLOYMENT.md`

## Follow-up validation

- `pytest -q cs/aws/v2/test_v2_hardening.py`  
  Result: `9 passed`
- `python -m compileall cs/aws/v2`  
  Result: success

### Added tests in this pass

- `test_upload_confirm_requires_existing_file`
- `test_curation_without_version_uses_latest`
- `test_clone_rejects_untrusted_host_for_token_use`

---

### A6) Remaining stale local-server reference in demo script

**Issue:** Demo summary still instructed users to run removed `v2/local_server.py`.  
**Risk:** Confusing onboarding path.

**Fix implemented:**

- Updated demo text to current local startup flow (`cd cs/aws && ./deploy.sh local`).

**Files:**

- `cs/aws/demo_mdf_v2.py`

**Validation:**

- `python -m py_compile cs/aws/demo_mdf_v2.py`  
  Result: success

---

## Next-steps implementation pass (append)

Date: February 6, 2026 (next-step execution)

This section addresses the follow-up priorities:

- Near-term items 1-4: implemented
- Medium-term items 5-7: not fully implemented in this pass (tracked)
- Low-priority item 9: partially implemented (request-correlated structured logs)

### B1) Rate limiting / request throttling

**Implemented:**

- Added HTTP middleware with per-actor, per-route-family in-memory rate limits.
- Added endpoint-class defaults and env-tunable limits:
  - `RATE_LIMIT_DEFAULT_PER_MIN`
  - `RATE_LIMIT_SUBMIT_PER_MIN`
  - `RATE_LIMIT_STREAM_CREATE_PER_MIN`
  - `RATE_LIMIT_STREAM_UPLOAD_PER_MIN`
  - `RATE_LIMIT_STREAM_MUTATION_PER_MIN`
  - `RATE_LIMIT_WINDOW_SECONDS`
- Added SAM HTTP API default throttle settings (rate + burst) at gateway layer.

**Files:**

- `cs/aws/v2/app/middleware.py`
- `cs/aws/v2/app/__init__.py`
- `cs/aws/template.yaml`

---

### B2) Input size limits

**Implemented:**

- Added global request body cap middleware (`MAX_REQUEST_BYTES`).
- Added submit metadata size cap (`MAX_SUBMIT_METADATA_BYTES`).
- Added submit object cardinality caps (`MAX_SUBMIT_DATA_SOURCES`, `MAX_SUBMIT_AUTHORS`).
- Added stream-append caps (`MAX_STREAM_APPEND_COUNT`, `MAX_STREAM_APPEND_BYTES`).
- Added model-level list/count constraints in request models.

**Files:**

- `cs/aws/v2/app/middleware.py`
- `cs/aws/v2/app/routers/submissions.py`
- `cs/aws/v2/app/routers/streams.py`
- `cs/aws/v2/app/models.py`

---

### B3) Expanded integration test coverage

**Implemented:**

- Added end-to-end happy path:
  - submit -> pending_curation -> approve (DOI) -> card
- Added throttling and request-size enforcement test.
- Added submissions pagination test with 55 records and multiple pages.
- Added stream path validation edge-case test for download-url ownership binding.
- Added curation transition test for reject rules (invalid and valid transitions).

**Files:**

- `cs/aws/v2/test_v2_integration.py`

---

### B4) `_mint_doi_for_stream` timestamp consistency

**Implemented:**

- Updated DOI publication year derivation to timezone-aware UTC:
  - `datetime.now(timezone.utc).year`

**Files:**

- `cs/aws/v2/app/routers/streams.py`

---

### B5) Structured logging (low-priority item 9, partial)

**Implemented:**

- Added request-correlated logging for completion/error events.
- Added automatic `X-Request-Id` response header.
- Logs include method, path, status, and duration in structured JSON format.

**Files:**

- `cs/aws/v2/app/middleware.py`

---

### B6) Additional UTC cleanup in touched runtime paths

**Implemented:**

- Replaced remaining deprecated `datetime.utcnow()` in test-exercised runtime code.

**Files:**

- `cs/aws/v2/storage/base.py`
- `cs/aws/v2/datacite.py`

## Validation for this pass

- `pytest -q cs/aws/v2/test_v2_hardening.py cs/aws/v2/test_v2_integration.py`  
  Result: `14 passed`
- `python -m compileall cs/aws/v2`  
  Result: success

## Remaining items not completed in this pass

1. Async workers for profiling/DOI/indexing (item 5): still inline in request path; needs queue + worker architecture.
2. Policy-driven ACL model (item 6): current model remains owner-or-curator.
3. External search backend (item 7): current implementation still in-process scan/filter.
4. `action_id` schema cleanup (item 8): intentionally deferred.

## Cost optimization pass (AWS deployment)

Date: February 6, 2026 (cost review + implementation)

Objective: keep AWS spend low for expected throughput:

- Publishing APIs: hundreds of queries/month
- Streams workload: ~10k requests/month

### C1) Cost review findings

1. Lambda sizing/timeouts were higher than needed for low-throughput serverless usage:
   - API Lambda timeout was 120s (while API Gateway integration timeout is much lower).
   - Both Lambdas were fixed at 512 MB.
2. CloudWatch log retention was unbounded (default indefinite retention can grow storage cost over time).
3. Search endpoint allowed large caller limits and scanned full table caps by default (`list_all()` defaults), increasing variable Dynamo read cost risk.
4. Async worker could invoke at very low message density (no batch window), causing extra Lambda invoke overhead.

### C2) Implemented cost controls

#### Infrastructure right-sizing (`cs/aws/template.yaml`)

- Added tunable parameters with low-cost defaults:
  - `ApiMemorySizeMb` (default `256`)
  - `ApiTimeoutSeconds` (default `30`)
  - `AsyncWorkerMemorySizeMb` (default `256`)
  - `AsyncWorkerTimeoutSeconds` (default `60`)
  - `ApiReservedConcurrency` (default `10`)
  - `AsyncWorkerReservedConcurrency` (default `3`)
  - `AsyncWorkerBatchSize` (default `10`)
  - `AsyncWorkerBatchWindowSeconds` (default `5`)
  - `AsyncQueueMessageRetentionSeconds` (default `345600` = 4 days)
  - `LogRetentionDays` (default `14`)
- Updated function resources to use those parameters.
- Added explicit CloudWatch log groups with bounded retention:
  - `/aws/lambda/${ApiFunction}`
  - `/aws/lambda/${AsyncWorkerFunction}`
- Set API runtime search caps via env vars:
  - `SEARCH_MAX_RESULTS=50`
  - `SEARCH_MAX_DATASET_SCAN=1000`
  - `SEARCH_MAX_STREAM_SCAN=2000`

#### Search read-cost bounding (`cs/aws/v2/search.py`, `cs/aws/v2/app/routers/search.py`)

- Added environment-driven scan caps for dataset and stream searches.
- Search now uses `store.list_all(limit=...)` with caps instead of unbounded defaults.
- Added max-results clamp at API layer (`SEARCH_MAX_RESULTS`, default 50), preventing expensive oversized result requests.

#### Regression test coverage (`cs/aws/v2/test_v2_integration.py`)

- Added `test_search_limit_is_capped`:
  - Seeds 60 matching records.
  - Confirms `/search` clamps output to 50 even when client requests `limit=500`.

### C3) Validation

- `pytest -q cs/aws/v2/test_v2_hardening.py cs/aws/v2/test_v2_integration.py cs/aws/v2/test_v2_async_jobs.py`
  - Result: `17 passed`
- `python -m compileall cs/aws/v2`
  - Result: success

### C4) Cost posture for expected traffic

Given current architecture (Lambda + HTTP API + DynamoDB on-demand + SQS), this remains a low fixed-cost, pay-per-use stack and is appropriate for your stated volume. The new concurrency caps, memory defaults, scan limits, and log retention reduce surprise spend risk without changing product behavior.

### C5) Optional future cost lever (not enabled in this pass)

- Evaluate DynamoDB table class `STANDARD_INFREQUENT_ACCESS` once dataset size grows and access patterns are confirmed cold-heavy. This can reduce storage cost but increases per-request read/write pricing.

## CLI-v2 wiring plan intake (shared context)

Date: February 6, 2026
Source plan reviewed: `/Users/ben/.claude/plans/serene-enchanting-planet.md`

### D1) Plan review status

- Read and validated the full phased plan for wiring CLI auth/routing to v2 backend.
- Confirmed objective: unify all CLI paths through `BackendClient` with Globus/dev auth support, add login/logout/whoami, and deprecate legacy direct submission path.

### D2) Completion snapshot against phases (current codebase)

1. Phase 1 (`backend_client.py` auth + URL resolution): **not implemented**
   - `BackendClient` still only takes `base_url` and sends no auth headers.
   - No `authenticated()` factory, no `_api_url_for_service()`, no service URL map.
2. Phase 2 (`login`/`logout`/`whoami` CLI): **not implemented**
   - Commands not present in `src/mdf_agent/cli/main.py`.
3. Phase 3 (`mdf publish` rewrite): **not implemented**
   - `publish` still uses `--local` split path and legacy `agent.publish(authorizer=...)`.
4. Phase 4 (`backend` + `stream` shared auth options): **not implemented**
   - No Typer callback-level `--service/--token/--dev-user` auth options in `src/mdf_agent/cli/backend.py` or `src/mdf_agent/cli/stream.py`.
5. Phase 5 (search + agent stream helpers + skill handlers): **not implemented**
   - `search` still uses unauthenticated `BackendClient` creation path.
   - `MDFAgent.stream_*` and skill handlers still use unauthenticated `BackendClient(base_url)/from_env()`.
6. Phase 6 (deprecate `submit_submission`): **not implemented**
   - `submit_submission()` currently has no deprecation warning and is still used by `MDFAgent.publish()`.

### D3) What has been completed for this plan

- Plan fully reviewed and translated into a concrete code-level gap analysis against current implementation.
- No code changes from this plan have been applied yet in this pass.

## CLI-v2 wiring implementation pass (completed)

Date: February 6, 2026
Plan source: `/Users/ben/.claude/plans/serene-enchanting-planet.md`

### E1) Phase 1 complete: BackendClient auth + URL resolution

Implemented in `src/mdf_agent/core/backend_client.py`:

- Added `_V2_API_URLS` and `_api_url_for_service()` with `MDF_API_URL` override support.
- Extended `BackendClient.__init__` to accept `token` and `user_id`.
- Updated `_request()` to inject:
  - `Authorization: Bearer <token>` when token present
  - `X-User-Id` when dev user present and no token
- Updated `from_env()` to read:
  - `MDF_API_URL`
  - `MDF_CONNECT_TOKEN`
  - `MDF_DEV_USER_ID`
- Added `BackendClient.authenticated(...)` factory with resolution order:
  1. explicit token
  2. `MDF_CONNECT_TOKEN`
  3. explicit/env dev user (`MDF_DEV_USER_ID`, plus `LOCAL_USER_ID` for local)
  4. interactive Globus login (lazy import)
- Local service (`service_instance=local`) now avoids forced OAuth when no credentials are present.

### E2) Phase 2 complete: `login` / `logout` / `whoami`

Implemented in `src/mdf_agent/cli/main.py`:

- Added `mdf login` with `--service` and optional `--token`.
- Added `mdf logout` to clear cached tokens.
- Added `mdf whoami` to show auth status and token store path.
- Used lazy imports inside command functions to avoid requiring Globus SDK for unrelated CLI usage.

### E3) Phase 3 complete: unified `mdf publish`

Implemented in `src/mdf_agent/cli/main.py` and `src/mdf_agent/core/agent.py`:

- Removed `--local` branch logic in main publish command.
- `--service` now supports `prod/dev/local`; added `--dev-user`.
- All submit paths now go through `BackendClient.authenticated(...).submit(...)`.
- Removed `action_id` output emphasis from CLI success output.
- `MDFAgent.publish()` now supports:
  - `token`, `service_instance`, `api_url`, `dev_user_id`
  - legacy `authorizer` extraction for backward compatibility

### E4) Phase 4 complete: auth callback pattern in backend/stream subcommands

Implemented in:

- `src/mdf_agent/cli/backend.py`
- `src/mdf_agent/cli/stream.py`

Changes:

- Added Typer callbacks with shared options:
  - `--service`
  - `--token`
  - `--dev-user`
- Updated shared `_client()` helper in both files to use `BackendClient.authenticated(...)`.
- Existing subcommands retained their command-level behavior while inheriting shared auth routing.

### E5) Phase 5 complete: remaining consumers

Implemented in:

- `src/mdf_agent/cli/main.py` (`mdf search`)
- `src/mdf_agent/core/agent.py` (`stream_*` helpers)
- `src/mdf_agent/skill/handlers.py`

Changes:

- `mdf search` now accepts `--service`, `--token`, `--dev-user`.
  - Uses authenticated client when credentials are provided.
  - Falls back to unauthenticated client when no creds are provided (public search path).
- `MDFAgent.stream_create/append/status/close/snapshot` now accept auth params and route through `BackendClient.authenticated(...)`.
- Skill publish/stream handlers now pass through `token`, `service_instance`, `dev_user_id`, and use authenticated backend path.

### E6) Phase 6 complete: deprecate old submission path

Implemented in `src/mdf_agent/core/submission.py`:

- Added `DeprecationWarning` in `submit_submission()` directing callers to `BackendClient.authenticated(...).submit(...)`.
- Removed direct `submit_submission` usage from `MDFAgent.publish()` (now backend-client based).

### E7) Additional test coverage for auth routing

Added new tests in `tests/test_backend_client_auth.py`:

- `from_env()` reads auth env vars.
- request header injection behavior (token precedence over dev user).
- authenticated client uses env token when present.
- local service avoids forced OAuth when no creds.
- service URL helper respects env override.

### E8) Validation

- `pytest -q tests/test_backend_client_auth.py tests/test_cli.py`
  - Result: `26 passed`
- `python -m compileall src/mdf_agent`
  - Result: success

### E9) Compatibility notes

- `submit_submission()` remains callable (deprecated, not removed).
- `MDFAgent.publish(authorizer=...)` remains supported via legacy token extraction.
- `src/mdf_agent/cli/publish.py` is now aligned with the authenticated backend flow.

### E10) Phase 2 verification addendum

Additional validation for `login/logout/whoami` was implemented in `tests/test_cli.py`:

- `test_login_invokes_auth`
- `test_logout_reports_success`
- `test_whoami_uses_env_token_status`

Validation run:

- `pytest -q tests/test_cli.py tests/test_backend_client_auth.py`
  - Result: `29 passed`

### E11) Final remaining-phase cleanup

Implemented final legacy cleanup in `src/mdf_agent/cli/publish.py`:

- Removed direct `get_authorizer(...)` usage.
- Routed submit path through `agent.publish(..., token/service_instance/api_url/dev_user_id)` so it follows the same `BackendClient.authenticated(...)` flow as the main CLI.
- Added `--api-url` and `--dev-user`; updated `--service` help to include `local`.

Additional validation:

- `pytest -q tests/test_agent_backend_routing.py tests/test_cli.py tests/test_backend_client_auth.py`
  - Result: `34 passed`
- `python -m compileall src/mdf_agent`
  - Result: success

All phases in the `serene-enchanting-planet` plan are now implemented in code.
