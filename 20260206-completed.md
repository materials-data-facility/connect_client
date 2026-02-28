# MDF Connect v2: AWS Staging Deployment with Globus Auth

**Date:** 2026-02-06
**Branch:** `v2-backend-curation`
**Stack:** `mdf-connect-v2-staging`
**API:** `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging`

---

## What Was Done

Deployed the MDF Connect v2 backend to AWS (staging) and verified the full end-to-end flow: Globus OAuth2 login, stream creation, file upload to the NCSA Globus HTTPS endpoint, snapshot, repository publish, search, dataset cards, and citations.

---

## Architecture

```
Mac (mdf-agent CLI)                         AWS us-east-1
────────────────────                        ─────────────────
mdf login
  └─> Browser → Globus OAuth2
  └─> Tokens cached ~/.config/mdf_agent/tokens.json
       - auth.globus.org     (openid, identity)
       - 4d5f8e8b-...        (MDF Connect)
       - 82f1b5c6-...        (NCSA HTTPS endpoint)
       - transfer.api.globus.org

BackendClient.authenticated()
  └─> Bearer: auth.globus.org token    ──>  API Gateway
  └─> X-Globus-Token: NCSA data token       └─> Lambda (FastAPI + Mangum)
                                                  └─> AuthClient.userinfo(bearer)
                                                  └─> DynamoDB (submissions, streams)
                                                  └─> Globus HTTPS PUT (user's data token)
                                                       └─> data.materialsdatafacility.org/tmp/staging/
```

### Two Globus Apps

| Role | Type | Client ID | Purpose |
|------|------|-----------|---------|
| Client | NativeAppAuthClient | `074cebcc-19ad-4332-bbf2-78402291b659` | User authenticates in browser |
| Server | ConfidentialAppAuthClient | `86e4853e-9bdd-4ea5-9130-e4a0b0638400` | Backend identity resolution, dependent tokens |

---

## Key Decisions & Fixes

### 1. Globus HTTPS Endpoint Scope

The NCSA MDF endpoint requires a scope tied to its **collection UUID**, not the hostname:

```
# Wrong (returns 401):
urn:globus:auth:scope:data.materialsdatafacility.org:all

# Correct:
https://auth.globus.org/scopes/82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/https
```

### 2. Token Strategy (Two Tokens)

A single Globus token can't serve both purposes. The solution uses two tokens:

- **Bearer token** (`auth.globus.org`): Carries `openid` scope. Backend calls `userinfo()` to get the user's identity (`sub`, `email`, `name`).
- **X-Globus-Token** (NCSA collection): Has write access to `data.materialsdatafacility.org`. Forwarded through the backend to the Globus HTTPS endpoint for file uploads.

### 3. Backend Auth: userinfo() Instead of introspect()

`oauth2_token_introspect()` only reports `active: true` for tokens issued to the introspecting app's own resource server. Since the Bearer token is for `auth.globus.org` (not our confidential app), introspection always returned `active: false`.

Fix: Use `AuthClient(authorizer=AccessTokenAuthorizer(token)).userinfo()` which works with any valid Globus token that has the `openid` scope.

### 4. Upload Token Priority

The Globus storage backend prefers the **user's token** (forwarded via `X-Globus-Token`) for uploads. Server credentials (client credentials flow) are only used as a fallback for background operations where no user token is available. This matters because the confidential app may not have direct write access to the endpoint.

### 5. DynamoDB Decimal Serialization

DynamoDB returns numbers as `decimal.Decimal`. The stream snapshot handler hit `TypeError: Object of type Decimal is not JSON serializable` when building the submission record. Fixed with a `default` handler in `json.dumps()`.

### 6. Auto-Populate data_sources from Commits

The repo workflow (`init` → `add` → `commit` → `publish`) doesn't require users to explicitly set `data_sources`. If `manifest.data_sources` is empty but there are committed files, `build_submission()` auto-populates from the commit history. Validation was updated to know about this.

---

## Files Modified

### Client (`src/mdf_agent/`)

| File | Changes |
|------|---------|
| `auth/globus.py` | Fixed `DATA_MDF_SCOPE` to use collection UUID scope. Added `NCSA_MDF_COLLECTION_UUID`. `get_authorizer_for_scopes()` now always includes `auth.globus.org` authorizer. |
| `cli/main.py` | `mdf login` requests MDF Connect + NCSA HTTPS + Transfer scopes in one consent. |
| `core/backend_client.py` | Added `globus_data_token` support. `authenticated()` sends auth.globus.org token as Bearer, NCSA token as X-Globus-Token. Added staging to URL map. |
| `core/submission.py` | Auto-populates `data_sources` from committed files when empty. |
| `core/agent.py` | `validate()` passes `has_committed_files` flag to skip data_sources check. |
| `core/validation.py` | `validate_manifest()` accepts `has_committed_files` parameter. |
| `models/config.py` | Extended manifest config for v2 metadata fields. |
| `models/submission.py` | Extended submission models for v2. |

### Backend (`cs/aws/`)

| File | Changes |
|------|---------|
| `template.yaml` | Added `StorageBackend`, `UseMockDatacite`, `AllowAllCurators` parameters with auto-select logic. Added `GLOBUS_BASE_PATH` env var. Added S3 bucket (conditional). Added S3 policies. |
| `samconfig.toml` | Staging overrides: `AuthMode=production UseMockDatacite=true AllowAllCurators=true`. |
| `deploy.sh` | Fixed macOS Bash 3 compatibility (`${env^}` → `awk`). Fixed parameter_overrides merging for Globus credentials. |
| `v2/app/auth.py` | Switched from `oauth2_token_introspect()` to `AuthClient.userinfo()`. |
| `v2/app/routers/files.py` | Added logging for upload token diagnostics. |
| `v2/app/routers/streams.py` | Fixed `Decimal` serialization in snapshot handler. |
| `v2/storage/globus_https.py` | Fixed client credentials scope. User token preferred over server credentials for uploads. |
| `v2/storage/s3.py` | New S3 storage backend (not active, for future use). |
| `v2/storage/factory.py` | Added `s3` case to factory. |

### New Files

| File | Purpose |
|------|---------|
| `examples/demo_staging_globus.py` | End-to-end staging demo: health, stream workflow, repo workflow, discovery. |
| `examples/test_globus_direct_upload.py` | Isolated Globus HTTPS upload test (no server). |

---

## Deployed Staging Resources

All resources are managed by CloudFormation stack `mdf-connect-v2-staging`. Redeployments update in-place (no resource pollution).

| Resource | Name/ID |
|----------|---------|
| API Gateway | `hjccjf3eqg` → `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging` |
| Lambda (API) | `mdf-connect-v2-staging-ApiFunction-UciD01YjsOEN` |
| Lambda (Async) | `mdf-connect-v2-staging-AsyncWorkerFunction-3t7JsGf6A6Ci` |
| DynamoDB | `mdf-submissions-staging`, `mdf-streams-staging` |
| SQS | `mdf-async-jobs-staging` |
| S3 (deploy) | `mdf-sam-deployments-staging` |
| Globus storage | `data.materialsdatafacility.org/tmp/staging/` |

---

## Verified End-to-End Flow

```
Health check                    ✓  200 OK
Stream create                   ✓  stream-77670a8b...
Stream upload (CSV)             ✓  283 bytes → Globus HTTPS
Stream upload (JSON)            ✓  146 bytes → Globus HTTPS
Stream list files               ✓  2 files, globus backend
Stream status                   ✓  open, 2 files, 429 bytes
Stream snapshot                 ✓  → submission record in DynamoDB
Repo init/add/commit            ✓  local repository workflow
Repo validate                   ✓  auto-populated data_sources from commits
Repo publish (submit)           ✓  → submission record in DynamoDB
Search                          ✓  results returned
Dataset card                    ✓  title, status, authors
Citation                        ✓  BibTeX, APA (mock DOI)
```

---

## Deploy Commands

```bash
# Full deploy
cd cs/aws && sam build && ./deploy.sh staging

# Check logs
aws logs filter-log-events \
  --log-group-name "/aws/lambda/mdf-connect-v2-staging-ApiFunction-UciD01YjsOEN" \
  --start-time $(python3 -c "import time; print(int((time.time() - 300) * 1000))") \
  --query 'events[*].message' --output text

# Teardown (when done)
./deploy.sh teardown staging
```

---

## What's Next

- **Curation workflow**: Test status transitions (submitted → pending_curation → accepted) with `ALLOW_ALL_CURATORS=true`
- **Real DOIs**: Switch from mock DataCite to sandbox/production when ready
- **Globus Search integration**: Backend indexing of submissions into Globus Search
- **Transfer integration**: Large dataset uploads via Globus Transfer (scope already requested at login)
- **Production deploy**: `./deploy.sh prod` with `AuthMode=production`, real DataCite, curator group IDs
