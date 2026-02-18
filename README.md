# MDF Agent

Python client and CLI for the [MDF Connect v2](https://github.com/materials-data-facility/connect_server) backend. Submit datasets, stream data from automated labs, curate submissions, and search the Materials Data Facility.

## Install

```bash
pip install -e .
```

## Quick start

```bash
# Authenticate with Globus
mdf login

# Publish a dataset (repository workflow)
mdf init --title "My Dataset" --author "Jane Doe"
mdf add ./data
mdf commit -m "Initial commit"
mdf publish

# Check submission status
mdf backend status SOURCE_ID

# Search datasets
mdf search "iron oxide"
```

## CLI commands

### Auth

```bash
mdf login                     # Authenticate via Globus (opens browser)
mdf login --service staging   # Authenticate against staging
mdf logout                    # Clear cached tokens
mdf whoami                    # Show current auth status
```

### Dataset workflow

```bash
mdf init --title "Title" --author "Name"   # Initialize dataset
mdf add ./data                              # Add files
mdf commit -m "message"                     # Commit changes
mdf publish                                 # Submit to MDF
mdf publish --service staging               # Submit to staging
```

### Streaming (automated labs)

```bash
mdf stream create --title "Lab 42 XRD Run"     # Create stream
mdf stream append STREAM_ID ./data/scan_001.csv # Upload file
mdf stream status STREAM_ID                      # Check stream
mdf stream snapshot STREAM_ID                    # Snapshot to dataset
mdf stream close STREAM_ID                       # Close stream
mdf stream close STREAM_ID --mint-doi            # Close and mint DOI
```

### Backend operations

```bash
mdf backend health                                 # Health check
mdf backend status SOURCE_ID                       # Submission status
mdf backend submissions                            # List submissions
mdf backend curation-pending                       # Pending curation queue
mdf backend curation-approve SOURCE_ID             # Approve submission
mdf backend curation-approve SOURCE_ID --mint-doi  # Approve and mint DOI
mdf backend curation-reject SOURCE_ID --reason "..." # Reject
mdf backend preview SOURCE_ID                      # Dataset preview
```

### Service targeting

All commands that talk to the backend accept `--service` to choose the target:

```bash
--service prod      # Production (default)
--service staging   # Staging
--service local     # Local dev server (http://127.0.0.1:8080)
```

Or set `MDF_API_URL` to point to any backend URL.

## Python SDK

```python
from mdf_agent import BackendClient

# Authenticate (interactive Globus login, tokens cached)
client = BackendClient.authenticated(service_instance="staging")

# Submit a dataset
result = client.submit({
    "title": "My Dataset",
    "authors": [{"name": "Jane Doe"}],
    "data_sources": ["https://example.com/data.csv"],
})

# Check status
status = client.status(result["source_id"])

# Search
results = client.search("iron oxide")

# Streaming
stream = client.stream_create("Lab XRD Run")
client.stream_append(stream["stream_id"], files=[...])
client.stream_close(stream["stream_id"], mint_doi=True)
```

### Auth resolution

`BackendClient.authenticated()` resolves credentials in this order:

1. Explicit `token` parameter
2. `MDF_CONNECT_TOKEN` environment variable
3. `MDF_CLIENT_ID` + `MDF_CLIENT_SECRET` (confidential client credentials)
4. `MDF_DEV_USER_ID` (dev mode, no real auth)
5. Interactive Globus OAuth login (opens browser, caches tokens)

## Connecting to the backend

The backend is deployed as a separate service ([connect_server](https://github.com/materials-data-facility/connect_server)). See that repo's README for deployment instructions.

| Environment | API URL | How to deploy |
|-------------|---------|---------------|
| dev | `./deploy.sh status dev` for URL | `cd cs/aws && sam build && ./deploy.sh dev` |
| staging | `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging` | `cd cs/aws && sam build && ./deploy.sh staging` |
| prod | `./deploy.sh status prod` for URL | `cd cs/aws && sam build && ./deploy.sh prod` |
| local | `http://127.0.0.1:8080` | `cd cs/aws && ./deploy.sh local` |

## Running tests

```bash
python -m pytest tests/ -v
```

## Legacy

The original `mdf_forge` and `mdf_connect_client` code is preserved in `legacy/` for reference.

## Support

This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was performed under the following financial assistance award 70NANB19H005 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the Center for Hierarchical Materials Design (CHiMaD). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".
