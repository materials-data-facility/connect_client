# Publishing a Local Dataset to MDF: Complete Walkthrough

This walkthrough covers the full end-to-end flow of publishing a dataset from a local directory to the Materials Data Facility, from initial setup through publication and discovery.

---

## Prerequisites

```bash
# Install mdf_agent (with optional extractors for auto-metadata)
pip install -e ".[extractors]"

# Authenticate with Globus (opens browser for OAuth)
mdf login
```

If running against a local backend for testing:

```bash
# Terminal 1: start the local backend
cd cs/aws
STORE_BACKEND=sqlite AUTH_MODE=dev python -m v2.app.main
# Server starts at http://127.0.0.1:8080

# Terminal 2: all commands below use --service local
```

---

## Step 1: Prepare your data

Suppose you have a directory of experimental results:

```
my_experiment/
  measurements.csv        # 500 rows of X-ray diffraction data
  parameters.json         # Instrument settings and sample metadata
  supplementary/
    calibration.csv       # Calibration reference data
```

---

## Step 2: Initialize the dataset

You can provide metadata via flags or interactively.

**With flags:**

```bash
cd my_experiment

mdf init . \
  --title "X-ray Diffraction Study of Iron Oxide Nanoparticles" \
  --author "Jane Doe" \
  --author "John Smith" \
  --description "XRD patterns for Fe2O3 and Fe3O4 nanoparticles"
```

**Interactively (just run `mdf init`):**

```
$ mdf init .

Initialize MDF dataset

Dataset title: X-ray Diffraction Study of Iron Oxide Nanoparticles
Enter author names one per line. Empty line to finish.
Author: Jane Doe
Author: John Smith
Author:
Description (optional): XRD patterns for Fe2O3 and Fe3O4 nanoparticles
```

**What happens:** This creates two things in your directory:
- `mdf.yaml` — the dataset manifest (title, authors, description, data sources)
- `.mdf/` — internal state directory (staged files, commits)

Output:
```
Initialized MDF repository at .
  Title: X-ray Diffraction Study of Iron Oxide Nanoparticles
  Authors: Jane Doe, John Smith
```

---

## Step 3: Stage and commit files

Stage your data files. Use `--discover` to automatically extract metadata from CSVs, PDFs, and Excel files:

```bash
mdf add measurements.csv parameters.json supplementary/ --discover
```

Output:
```
Staged:
  + measurements.csv
  + parameters.json
  + supplementary/calibration.csv
```

Now commit:

```bash
mdf commit -m "Initial dataset with XRD measurements and calibration"
```

Output:
```
Committed: Initial dataset with XRD measurements and calibration
  3 files recorded
```

---

## Step 4: Check status and validate

```bash
mdf status
```

Output:
```
No files staged

Commits (1):
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ #  ┃ Message                                          ┃ Files ┃ Time                ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 1  │ Initial dataset with XRD measurements and cal... │     3 │ 2026-02-27T14:30:00 │
└────┴──────────────────────────────────────────────────┴───────┴─────────────────────┘
```

Validate the manifest before publishing:

```bash
mdf validate
```

Output:
```
Validation passed
```

---

## Step 5: Preview the submission (dry run)

```bash
mdf publish
```

By default, `publish` does a dry run. It shows you the exact JSON payload that would be submitted:

```
Dry run - would submit:
Target: https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging (staging)
{
  "title": "X-ray Diffraction Study of Iron Oxide Nanoparticles",
  "authors": [
    {"name": "Jane Doe"},
    {"name": "John Smith"}
  ],
  "description": "XRD patterns for Fe2O3 and Fe3O4 nanoparticles",
  "publisher": "Materials Data Facility",
  "publication_year": 2026,
  "resource_type": "Dataset",
  "data_sources": [
    "/Users/jane/my_experiment/measurements.csv",
    "/Users/jane/my_experiment/parameters.json",
    "/Users/jane/my_experiment/supplementary/calibration.csv"
  ],
  "test": false,
  "update": false
}
```

Review the payload. If something looks wrong, edit `mdf.yaml` and re-run.

---

## Step 6: Publish

When ready, use `--submit` to actually send:

```bash
mdf publish --submit
```

**What happens behind the scenes:**

1. **File upload**: Local files are uploaded to MDF's Globus HTTPS storage via streaming PUT (8 MB chunks). A progress bar shows upload status for each file.
2. **Payload submission**: The `data_sources` are replaced with `globus://` URIs pointing to the uploaded files, and the metadata payload is POSTed to `POST /submit`.
3. **Backend processing**: The backend validates the metadata (via the `DatasetMetadata` Pydantic model), generates a `source_id`, sets version to `1.0`, stores the record as `pending_curation`, and enqueues a profiling job to scan the data files.

Output:
```
measurements.csv  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  2.1 MB  1.2 MB/s
parameters.json   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  4.0 KB  2.0 MB/s
calibration.csv   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  512 KB  1.5 MB/s

Published successfully!
  Source ID: xrd-iron-oxide-nanoparticles
  Version: 1.0
```

The source ID and version are saved to your global config so you can check status easily.

---

## Step 7: Check backend status

```bash
mdf status
```

Now shows both local repo state and backend status:

```
No files staged

Commits (1):
...

Backend status: xrd-iron-oxide-nanoparticles v1.0
  Status: pending_curation
  Title: X-ray Diffraction Study of Iron Oxide Nanoparticles
  Next: Waiting for curation review
```

The `Next` hint tells you what happens next. Your dataset is now in the curation queue.

---

## Step 8: Curation (curator perspective)

A curator reviews and approves your dataset:

```bash
# Curator lists pending datasets
mdf pending

# Output:
# Pending curation (3):
# ┏━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
# ┃ # ┃ Source ID                        ┃ Title               ┃ Version ┃ Submitted           ┃
# ┡━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
# │ 1 │ xrd-iron-oxide-nanoparticles     │ X-ray Diffraction...│ 1.0     │ 2026-02-27T14:35:00 │
# └───┴──────────────────────────────────┴─────────────────────┴─────────┴─────────────────────┘

# Curator approves
mdf approve xrd-iron-oxide-nanoparticles --notes "Metadata complete, data verified"

# Output:
# Approved: xrd-iron-oxide-nanoparticles
#   DOI: https://doi.org/10.18126/xxxxx
```

After approval, the backend enqueues a publish job that mints a DOI via DataCite and indexes the dataset in Globus Search.

---

## Step 9: Discover your published dataset

Check status again:

```bash
mdf status
```

```
Backend status: xrd-iron-oxide-nanoparticles v1.0
  Status: published
  Title: X-ray Diffraction Study of Iron Oxide Nanoparticles
  DOI: https://doi.org/10.18126/xxxxx
  Root version: xrd-iron-oxide-nanoparticles-1.0
  Next: Dataset is live!
```

### Browse your datasets

```bash
mdf list
```

```
Your datasets (1):

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ Source ID                        ┃ Title                             ┃ Version ┃ Status    ┃ Updated             ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ xrd-iron-oxide-nanoparticles     │ X-ray Diffraction Study of Iro... │ 1.0     │ published │ 2026-02-27T14:40:00 │
└──────────────────────────────────┴───────────────────────────────────┴─────────┴───────────┴─────────────────────┘
```

### View the dataset card

```bash
mdf show xrd-iron-oxide-nanoparticles
```

```
╭─ xrd-iron-oxide-nanoparticles v1.0 ─────────────────────────────────────╮
│ X-ray Diffraction Study of Iron Oxide Nanoparticles                     │
│ XRD patterns for Fe2O3 and Fe3O4 nanoparticles                          │
╰─────────────────────────────────────────────────────────────────────────╯

  Authors         Jane Doe, John Smith
  Publisher       Materials Data Facility
  Year            2026
  DOI             https://doi.org/10.18126/xxxxx
  Status          published

  Files: 3 sources | Types: csv, json
```

### Get a citation

```bash
mdf show xrd-iron-oxide-nanoparticles --cite
```

Appends to the card output:
```
Citation:
  Doe, J. & Smith, J. (2026). X-ray Diffraction Study of Iron Oxide
  Nanoparticles [Dataset]. Materials Data Facility. https://doi.org/10.18126/xxxxx
```

### Search for it

```bash
mdf search "iron oxide XRD"
```

```
Found 1 results for 'iron oxide XRD'

┏━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ # ┃ Type    ┃ Title                                    ┃ ID                                        ┃ Status    ┃
┡━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ 1 │ dataset │ X-ray Diffraction Study of Iron Oxide ... │ xrd-iron-oxide-nanoparticles v1.0          │ published │
└───┴─────────┴──────────────────────────────────────────┴───────────────────────────────────────────┴───────────┘
```

---

## Step 10: Update the dataset (new version)

You've collected more data and want to publish an update:

```bash
mdf update --data ./new_measurements/ --title "Updated XRD Study" --submit
```

This automatically:
- Uses the last published source ID from config
- Sets `update=True` so the backend increments the version to `1.1`
- Uploads the new files and submits

```
new_scan_003.csv  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1.8 MB  1.1 MB/s

Updated successfully!
  Source ID: xrd-iron-oxide-nanoparticles
  Version: 1.1
```

### View version history

```bash
mdf versions xrd-iron-oxide-nanoparticles
```

```
Versions for xrd-iron-oxide-nanoparticles

┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ Version ┃ Title                                    ┃ Status            ┃ DOI                                  ┃ Updated             ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 1.0     │ X-ray Diffraction Study of Iron Oxide ...│ published         │ https://doi.org/10.18126/xxxxx       │ 2026-02-27T14:40:00 │
│ 1.1     │ Updated XRD Study                        │ pending_curation  │                                      │ 2026-02-27T15:10:00 │
└─────────┴──────────────────────────────────────────┴───────────────────┴──────────────────────────────────────┴─────────────────────┘

Dataset DOI: https://doi.org/10.18126/xxxxx
```

---

## Alternative: Direct publish (no repository)

If you don't need the git-style workflow, publish directly in a single command:

```bash
mdf publish ./my_experiment/ \
  --title "X-ray Diffraction Study of Iron Oxide Nanoparticles" \
  --author "Jane Doe" \
  --author "John Smith" \
  --description "XRD patterns for Fe2O3 and Fe3O4 nanoparticles" \
  --submit
```

This skips `init`/`add`/`commit` entirely. The directory contents become data sources and are uploaded directly.

---

## Alternative: Python SDK

```python
from mdf_agent import MDFAgent

# Repository mode
agent = MDFAgent.init(
    path="./my_experiment",
    title="X-ray Diffraction Study of Iron Oxide Nanoparticles",
    authors=["Jane Doe", "John Smith"],
    description="XRD patterns for Fe2O3 and Fe3O4 nanoparticles",
)
agent.add("*.csv", "*.json", discover=True)
agent.commit("Initial dataset")
result = agent.publish(service_instance="staging", dry_run=False)

print(f"Published: {result['source_id']} v{result['version']}")

# Later: check versions
versions = agent.versions(result["source_id"], service_instance="staging")
print(versions)

# Get citation
citation = agent.cite(result["source_id"], format="bibtex", service_instance="staging")
print(citation.get("bibtex"))
```

---

## What happens at each stage

| Stage | CLI Command | What happens on the backend |
|-------|-------------|----------------------------|
| **Init** | `mdf init` | Creates local `mdf.yaml` manifest and `.mdf/` state directory |
| **Add** | `mdf add` | Records files in local staging area; `--discover` extracts metadata from CSVs/PDFs |
| **Commit** | `mdf commit` | Snapshots staged files into a local commit record |
| **Validate** | `mdf validate` | Checks manifest for required fields (title, authors) and common issues |
| **Dry run** | `mdf publish` | Builds the JSON payload locally, displays it without sending |
| **Publish** | `mdf publish --submit` | Uploads files via HTTPS PUT, submits metadata to `POST /submit` |
| **Profile** | *(automatic)* | Backend scans uploaded files, extracts schema and statistics |
| **Curation** | `mdf approve` | Curator reviews and approves; backend enqueues publish job |
| **Publish** | *(automatic)* | Backend mints DOI via DataCite, indexes in Globus Search |
| **Discovery** | `mdf show` / `mdf search` | Dataset is visible, citable, and searchable |
| **Update** | `mdf update --submit` | Creates new version (1.1), previous version marked as non-latest |

---

## Error handling

The CLI provides actionable error messages:

| Scenario | CLI output |
|----------|-----------|
| Not authenticated | `Error: Authentication required` / `Run: mdf login` |
| Dataset not found | `Error: Not found` |
| Rate limited | `Error: Rate limited — try again shortly` |
| Server error (502/503) | Automatic retry (3 attempts with backoff) |
| Connection failure | Automatic retry, then `Connection failed after 4 attempts` |
| Validation error | Field-by-field error details |

Use `--json` on any `mdf backend` or `mdf stream` command to get raw JSON output for scripting:

```bash
mdf backend status --source-id my_dataset --json | jq '.submission.status'
```
