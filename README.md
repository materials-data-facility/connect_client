# MDF Agent

Python client and CLI for the [MDF Connect v2](https://github.com/materials-data-facility/connect_server) backend. Submit datasets, stream data from automated labs, curate submissions, and search the Materials Data Facility.

## Install

```bash
pip install -e .

# With metadata extractors (PDF, CSV, Excel)
pip install -e ".[extractors]"
```

## Quick start

```bash
# Authenticate with Globus
mdf login

# Publish a dataset directly (one command)
mdf publish ./data/ --title "My Dataset" --author "Jane Doe" --submit

# Or use a manifest for persistent metadata
mdf manifest init --title "My Dataset" --author "Jane Doe"
vim mdf.yaml                    # edit metadata, add data_sources
mdf publish --submit

# Check status
mdf status

# Browse your datasets
mdf list
mdf show my_dataset_v1
mdf versions my_dataset_v1
```

## CLI commands

### Auth

```bash
mdf login                     # Authenticate via Globus (opens browser)
mdf login --service staging   # Authenticate against staging
mdf logout                    # Clear cached tokens
mdf whoami                    # Show current auth status
```

### Publishing datasets

```bash
# Direct mode (no manifest needed)
mdf publish ./data/ --title "My Dataset" --author "Jane" --submit
mdf publish ./data/ --title "My Dataset" --author "Jane" --dry-run  # Preview payload

# Manifest mode (mdf.yaml in current directory)
mdf manifest init --title "Title" --author "Name"   # Create mdf.yaml (interactive if no flags)
mdf manifest discover *.csv                          # Extract metadata from files into mdf.yaml
mdf validate                                         # Check manifest
mdf publish --submit                                 # Submit to MDF

# Update an existing dataset
mdf update --data ./new_data/ --submit             # Updates last published dataset
mdf update my_dataset_v1 --title "New" --submit    # Explicit source_id
```

### Manifest management

```bash
mdf manifest init                        # Create mdf.yaml (interactive)
mdf manifest init --title "T" --author "A"  # Create mdf.yaml (non-interactive)
mdf manifest discover *.csv *.json       # Extract metadata from files into mdf.yaml
```

### Discoverability

```bash
mdf list                            # List your submitted datasets
mdf list --limit 50                 # More results

mdf show my_dataset_v1              # Formatted dataset card
mdf show my_dataset_v1 --cite       # Include citation
mdf show my_dataset_v1 --json       # Raw JSON output

mdf versions my_dataset_v1          # Version history table

mdf status                          # Status of last published dataset
mdf status my_dataset_v1            # Status of specific dataset

mdf search "perovskite"             # Keyword search across datasets and streams
mdf search "XRD" --type streams     # Search only streams
mdf search "battery cathodes" --semantic  # Vector search over title + description embeddings

mdf related my_dataset_v1                   # Datasets sharing authors (ORCID-first)
mdf related my_dataset_v1 --by similar      # Nearest neighbors over the embedding snapshot
mdf related my_dataset_v1 --limit 5
```

### Curation

```bash
mdf pending                                          # List datasets awaiting review
mdf pending --organization argonne                   # Filter by org
mdf approve my_dataset_v1                            # Approve for publication
mdf approve my_dataset_v1 --notes "LGTM"             # With curator notes
mdf reject my_dataset_v1 --reason "Missing methods"  # Reject with reason
```

### Streaming (automated labs)

```bash
mdf stream create --title "Lab 42 XRD Run"
mdf stream upload --stream-id ID data.csv            # Upload files (progress bar for >6MB)
mdf stream status --stream-id ID
mdf stream snapshot --stream-id ID                   # Snapshot to dataset
mdf stream close --stream-id ID --mint-doi           # Close and mint DOI
mdf stream files --stream-id ID                      # List uploaded files
```

### Configuration

```bash
mdf config show                          # Show all settings
mdf config set defaults.service staging  # Set default service
mdf config set user.email me@example.com # Set user email (validated)
mdf config get defaults.service          # Get a value
mdf config path                          # Show config file location
```

### Backend operations

Low-level backend API commands. All accept `--json` for raw JSON output (useful for scripting).

```bash
mdf backend health                         # Health check
mdf backend status --source-id X           # Submission status
mdf backend status --source-id X --json    # Raw JSON (for scripts)
mdf backend submissions                    # List submissions
mdf backend card my_dataset_v1             # Dataset preview card
mdf backend cite my_dataset_v1 -f bibtex   # Citation in BibTeX
mdf backend preview my_dataset_v1          # Dataset preview
mdf backend search "iron oxide"            # Search
```

### Service targeting

All commands that talk to the backend accept `--service` to choose the target:

```bash
--service prod      # Production
--service staging   # Staging (default)
--service local     # Local dev server (http://127.0.0.1:8080)
```

Or set `MDF_API_URL` to point to any backend URL.

## Python SDK

### MDFAgent (high-level API)

```python
from mdf_agent import MDFAgent

agent = MDFAgent()

# Search
results = agent.search("perovskite", service_instance="staging")

# Dataset info
card = agent.show("my_dataset_v1", service_instance="staging")
versions = agent.versions("my_dataset_v1", service_instance="staging")
citation = agent.cite("my_dataset_v1", format="bibtex", service_instance="staging")

# Curation
pending = agent.pending(service_instance="staging")
agent.approve("my_dataset_v1", notes="LGTM", service_instance="staging")
agent.reject("my_dataset_v1", reason="Missing methods", service_instance="staging")

# Publishing (manifest mode)
agent = MDFAgent.init_manifest(
    "./my_data",
    title="My Dataset",
    authors=["Jane Doe"],
)
agent.manifest.data_sources = ["./data"]
agent.save_manifest()
result = agent.publish(service_instance="staging", dry_run=False)

# Streaming
stream = agent.stream_create("Lab Run", service_instance="staging")
agent.stream_close(stream["stream_id"], mint_doi=True, service_instance="staging")
```

### BackendClient (low-level API)

```python
from mdf_agent import BackendClient

client = BackendClient.authenticated(service_instance="staging")

# Submit, status, search
result = client.submit({"title": "My Dataset", "authors": [{"name": "Jane"}], ...})
status = client.status(result["source_id"])
results = client.search("iron oxide")

# Versions and citations
versions = client.versions("my_dataset_v1")
citation = client.get_citation("my_dataset_v1", format="bibtex")

# Streaming
stream = client.stream_create("Lab Run")
client.stream_upload(stream["stream_id"], "data.csv", content)
client.stream_close(stream["stream_id"], mint_doi=True)

client.close()
```

### Programmatic / CI authentication

For scripts, notebooks, and CI pipelines you can skip interactive browser login by
setting environment variables. The recommended approach is to register a **confidential
client** at [developers.globus.org](https://developers.globus.org) and export the
credentials:

```bash
export MDF_CLIENT_ID="your-client-uuid"
export MDF_CLIENT_SECRET="your-client-secret"
mdf publish --submit          # no browser required
```

Alternatively, pass a pre-existing access token via `MDF_CONNECT_TOKEN`.

### Auth resolution

`BackendClient.authenticated()` resolves credentials in this order:

1. Explicit `token` parameter
2. `MDF_CONNECT_TOKEN` environment variable
3. `MDF_CLIENT_ID` + `MDF_CLIENT_SECRET` (confidential client credentials)
4. `MDF_DEV_USER_ID` (dev mode, no real auth)
5. Interactive Globus OAuth login (opens browser, caches tokens)

### Error handling

All HTTP requests automatically retry on transient errors:
- **429** (rate limited): respects `Retry-After` header
- **502, 503, 504** (server errors): exponential backoff
- **Connection errors**: 3 retries with backoff

File uploads (`_https_put_file`) also retry on 502/503/504 and connection errors. SSL verification for the Globus HTTPS endpoint is configurable via `MDF_SSL_VERIFY` (default: `false`, as the Globus endpoint uses a private CA).

## Semantic search and embeddings

MDF generates OpenAI `text-embedding-3-small` vectors (1536-dim) over each dataset's title + description and stores them in DynamoDB. A periodic S3 snapshot (`Float32Array` binary + JSON sidecar) feeds both the backend `/search/semantic` endpoint and any frontend that wants to scan client-side. There's also a lightweight in-memory author index that powers `mdf related` — no embeddings required, ORCID matched first.

### Usage

```bash
mdf search "perovskite photovoltaic stability" --semantic
mdf related my_dataset_v1                  # Co-author lookup
mdf related my_dataset_v1 --by similar     # Embedding nearest-neighbors
```

The `--by similar` path serves the dataset detail page's "you might also like" widget. It does no OpenAI call — the dataset's own vector is already in the cached snapshot, so it's a single cosine pass over the in-memory matrix. Frontends can also call `GET /datasets/{source_id}/related?by=similar&limit=5` directly.

### Automatic on publish

When a dataset is approved and published, the publish pipeline fires off a `generate_embedding` async job for the new version. If the embed call fails (OpenAI outage, quota), publish still succeeds — the next `rebuild-embeddings` will pick up the gap.

Editing metadata via `mdf edit` or a curator approve with `metadata_updates` bumps `metadata_updated_at`, which makes the skip check treat the existing embedding as stale on the next rebuild.

### Manual rebuild (curator-only)

```bash
# Check coverage, see current snapshot, spot any stale records
mdf admin embedding-status --service staging

# Dispatch a rebuild — returns immediately, work runs in the async worker
mdf admin rebuild-embeddings --service staging --yes

# Watch progress
mdf admin embedding-status --service staging
```

What the rebuild does:

1. Enqueues **one** dispatcher job (SQS) and returns — the endpoint never blocks on the scan.
2. Async worker scans DynamoDB, skips records whose embedding already matches the current model and isn't stale, and fans out one `generate_embedding` job per pending record.
3. Final `build_embedding_snapshot` job packs every vector into `s3://mdf-embeddings-<env>/embeddings/v1/index-<sha>.bin` + `.json`, then atomically swaps `current.json` to point at it. Content-hashed filenames mean browsers and the Lambda in-process cache can keep aggressive TTLs.

Flags:

```bash
--force         # Re-embed every published dataset (use after a model switch)
--limit N       # Cap OpenAI calls per run — good for phased backfills
--no-snapshot   # Fill Dynamo only, skip the S3 publish
```

### Staleness detection

Each embedded record carries `embedding_generated_at`; each metadata write stamps `metadata_updated_at`. `rebuild-embeddings` treats an embedding as stale whenever `embedding_generated_at < metadata_updated_at` (plus whenever `embedding_model` no longer matches `EMBEDDING_MODEL`). You do not need `--force` for normal edits — stale records are picked up automatically.

### First-time deploy

The embedding pipeline needs an OpenAI key and an S3 bucket. Deploy once with:

```bash
# 1. Stash the key in SSM (one time per environment)
aws ssm put-parameter \
  --name /mdf/staging/openai-api-key \
  --value "sk-..." \
  --type SecureString \
  --overwrite

# 2. Build + deploy — creates the mdf-embeddings-<env> bucket, IAM, env vars
cd cs/aws
sam build
sam deploy \
  --config-file samconfig.toml \
  --config-env staging \
  --parameter-overrides \
    OpenAIApiKey=$(aws ssm get-parameter \
      --name /mdf/staging/openai-api-key --with-decryption \
      --query Parameter.Value --output text)

# 3. Backfill any datasets that pre-date the feature
mdf admin rebuild-embeddings --service staging --yes
mdf admin embedding-status --service staging    # wait until with_embedding == published_total
```

Switching models later: update `EmbeddingModel` (and `EmbeddingDims` if changing size) in the SAM template, redeploy, then `mdf admin rebuild-embeddings` — records with the old model stamp get re-embedded automatically; same-model ones are skipped.

### Frontend integration

The snapshot bucket has CORS open for browser reads. If you front it with CloudFront, set `EmbeddingSnapshotPublicUrl=https://<distribution>` at deploy time; `POST /admin/embeddings/rebuild` then returns `public_urls.{bin,json,pointer}` in its response so the UI can fetch the blob directly. Query embedding is done server-side via `POST /embed` so the OpenAI key never ships to the browser.

## Connecting to the backend

| Environment | API URL | How to deploy |
|-------------|---------|---------------|
| dev | `./deploy.sh status dev` for URL | `cd cs/aws && sam build && ./deploy.sh dev` |
| staging | `https://3xicgt0g7l.execute-api.us-east-1.amazonaws.com/staging` | `cd cs/aws && sam build && ./deploy.sh staging` |
| prod | `./deploy.sh status prod` for URL | `cd cs/aws && sam build && ./deploy.sh prod` |
| local | `http://127.0.0.1:8080` | `cd cs/aws && ./deploy.sh local` |

## Running tests

```bash
# Client tests
python -m pytest tests/ -v --ignore=tests/test_connect_client.py

# Backend tests (no AWS credentials needed)
cd cs/aws && python -m pytest v2/test_v2_*.py -v
```

## Legacy

The original `mdf_forge` and `mdf_connect_client` code is preserved in `legacy/` for reference.

## Support

This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was performed under the following financial assistance award 70NANB19H005 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the Center for Hierarchical Materials Design (CHiMaD). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".
