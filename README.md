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

# Publish a dataset directly
mdf publish ./data/ --title "My Dataset" --author "Jane Doe" --submit

# Or use the repository workflow
mdf init --title "My Dataset" --author "Jane Doe"
mdf add ./data
mdf commit -m "Initial commit"
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
# Direct mode (no repository needed)
mdf publish ./data/ --title "My Dataset" --author "Jane" --submit
mdf publish ./data/ --title "My Dataset" --author "Jane" --dry-run  # Preview payload

# Repository mode
mdf init --title "Title" --author "Name"   # Initialize dataset (interactive if no flags)
mdf add ./data --discover                  # Stage files with auto-metadata extraction
mdf commit -m "message"                    # Commit changes
mdf validate                               # Check manifest
mdf publish --submit                       # Submit to MDF

# Update an existing dataset
mdf update --data ./new_data/ --submit             # Updates last published dataset
mdf update my_dataset_v1 --title "New" --submit    # Explicit source_id
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

mdf search "perovskite"             # Search all datasets and streams
mdf search "XRD" --type streams     # Search only streams
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

# Publishing (repository mode)
agent = MDFAgent.init("./my_data", title="My Dataset", authors=["Jane Doe"])
agent.add("data/*.csv", discover=True)
agent.commit("Add experimental data")
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

## Connecting to the backend

| Environment | API URL | How to deploy |
|-------------|---------|---------------|
| dev | `./deploy.sh status dev` for URL | `cd cs/aws && sam build && ./deploy.sh dev` |
| staging | `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging` | `cd cs/aws && sam build && ./deploy.sh staging` |
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
