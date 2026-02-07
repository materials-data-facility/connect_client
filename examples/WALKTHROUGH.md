# MDF Agent Walkthrough

Two ways to publish datasets to the Materials Data Facility: **human-driven CLI** or **AI-assisted zero-touch publishing**.

---

## Example 1: Human Workflow (CLI)

You're a materials scientist with DFT calculation results. Here's how to publish them.

### Step 1: Initialize your dataset

```bash
cd examples/dft-alloys

mdf init . \
  --title "High-Throughput DFT Study of Binary Intermetallic Alloys" \
  --author "Jane Doe" \
  --author "John Smith" \
  --author "Alice Chen" \
  --description "Formation energies and electronic properties for 15 Al-X intermetallics"
```

**Output:**
```
Initialized MDF repository at .
  Title: High-Throughput DFT Study of Binary Intermetallic Alloys
  Authors: Jane Doe, John Smith, Alice Chen
```

### Step 2: Stage your data files

```bash
mdf add calculations.csv parameters.json --discover
```

**Output:**
```
Staged:
  + calculations.csv
  + parameters.json
```

The `--discover` flag automatically extracts metadata:
- CSV: 6 columns, 15 rows (composition, formation_energy, volume, etc.)
- JSON: VASP 6.3.2, PBE functional, PAW pseudopotentials

### Step 3: Commit your changes

```bash
mdf commit -m "Initial dataset with 15 Al-X intermetallic calculations"
```

**Output:**
```
Committed: Initial dataset with 15 Al-X intermetallic calculations
  2 files recorded
```

### Step 4: Check repository status

```bash
mdf status
```

**Output:**
```
No files staged

Commits (1):
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ #  ┃ Message                                           ┃ Files ┃ Time                ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 1  │ Initial dataset with 15 Al-X intermetallic calc… │     2 │ 2026-01-31T06:15:30 │
└────┴───────────────────────────────────────────────────┴───────┴─────────────────────┘
```

### Step 5: Add data sources to manifest

Edit `mdf.yaml` to specify where data lives:

```yaml
title: High-Throughput DFT Study of Binary Intermetallic Alloys
authors:
  - Jane Doe
  - John Smith
  - Alice Chen
description: Formation energies and electronic properties for 15 Al-X intermetallics

data_sources:
  - "./calculations.csv"
  - "./parameters.json"

tags:
  - DFT
  - intermetallics
  - VASP
  - formation-energy

acl:
  - public
```

### Step 6: Validate before publishing

```bash
mdf validate
```

**Output:**
```
Validation passed
```

### Step 7: Preview the submission

```bash
mdf publish --dry-run
```

**Output:** (syntax-highlighted JSON)
```json
{
  "dc": {
    "titles": [{"title": "High-Throughput DFT Study of Binary Intermetallic Alloys"}],
    "creators": [
      {"creatorName": "Doe, Jane", "familyName": "Doe", "givenName": "Jane"},
      {"creatorName": "Smith, John", "familyName": "Smith", "givenName": "John"},
      {"creatorName": "Chen, Alice", "familyName": "Chen", "givenName": "Alice"}
    ],
    "publisher": "Materials Data Facility",
    "publicationYear": "2026",
    "resourceType": {"resourceTypeGeneral": "Dataset", "resourceType": "Dataset"},
    "descriptions": [{"description": "Formation energies...", "descriptionType": "Other"}]
  },
  "data_sources": ["./calculations.csv", "./parameters.json"],
  "test": false,
  "update": false
}
```

### Step 8: Publish to MDF Connect

```bash
# Test submission (sandbox)
mdf publish --submit --test

# Production submission
mdf publish --submit
```

**Output:**
```
Published successfully!
  Source ID: mdf-12345678
```

---

## Example 2: AI Agent Workflow (Claude Code)

---

## Example 3: Streaming (Local MVP)

This example uses the local v2 backend to simulate streaming data from instruments.

### Step 1: Start the local backend

```bash
cd cs/aws/v2
./local_start.sh
```

Optional: start the flow simulator to auto-advance statuses:

```bash
START_FLOW_SIM=true ./local_start.sh
```

### Step 2: Create a stream

```bash
mdf stream create --title "Argonne XRD+TGA Stream" --lab-id anl-xrd-tga --api-url http://127.0.0.1:8080
```

### Step 3: Append files

```bash
mdf stream append --stream-id <STREAM_ID> --file-count 2 --total-bytes 24576 --api-url http://127.0.0.1:8080
```

### Step 4: Snapshot the stream into a dataset

```bash
mdf stream snapshot --stream-id <STREAM_ID> --api-url http://127.0.0.1:8080
```

### Step 5: Check status and close

```bash
mdf stream status --stream-id <STREAM_ID> --api-url http://127.0.0.1:8080
mdf stream close --stream-id <STREAM_ID> --api-url http://127.0.0.1:8080
```

For a narrated demo, run:

```bash
./cs/aws/v2/local_lab_stream_demo.sh
```

The demo simulates an Argonne lab run (XRD + TGA), shows stream creation,
file appends, status updates, and a final snapshot into a dataset record.

You're using Claude Code and want to publish XRD data with zero manual work.

### The Conversation

**You:**
> I have XRD patterns in `examples/xrd-patterns/`. Can you publish this to MDF?

**Claude:**
> I'll analyze your data and prepare it for MDF. Let me scan the folder first.

```python
# Claude uses the MDF Agent skill handlers
from mdf_agent.skill.handlers import scan_folder, create_manifest, validate_and_preview

# Step 1: Scan and understand the data
metadata = scan_folder("examples/xrd-patterns")
# Returns: table schemas for CSVs, instrument config from YAML
```

> I found 2 XRD pattern files and instrument configuration. Here's what I detected:
>
> **Files:**
> - `sample_001.csv` - 15 data points (two_theta, intensity, d_spacing)
> - `sample_002.csv` - 15 data points (two_theta, intensity, d_spacing)
> - `instrument.yaml` - Rigaku SmartLab, Cu K-alpha, scan 20-90°
>
> **Samples:**
> - Fe2O3 (ball milled)
> - Fe3O4 (hydrothermal synthesis)
>
> What title and authors should I use?

**You:**
> Title: "XRD Patterns of Iron Oxide Nanoparticles"
> Authors: Me (Ben Johnson, MIT) and my advisor (Sarah Williams, MIT)

**Claude:**
```python
# Step 2: Create the manifest
manifest = create_manifest(
    path="examples/xrd-patterns",
    title="XRD Patterns of Iron Oxide Nanoparticles",
    authors=["Ben Johnson", "Sarah Williams"],
    description="X-ray diffraction patterns for Fe2O3 and Fe3O4 nanoparticles synthesized via ball milling and hydrothermal methods"
)
```

> I've created the manifest. Let me add the data sources and validate.

```python
# Step 3: Validate and preview
result = validate_and_preview("examples/xrd-patterns")
```

> **Validation passed!** Here's what will be submitted:
>
> ```json
> {
>   "dc": {
>     "titles": [{"title": "XRD Patterns of Iron Oxide Nanoparticles"}],
>     "creators": [
>       {"creatorName": "Johnson, Ben", ...},
>       {"creatorName": "Williams, Sarah", ...}
>     ],
>     ...
>   },
>   "data_sources": ["./sample_001.csv", "./sample_002.csv"],
>   "test": false
> }
> ```
>
> Ready to publish? I can submit to the test environment first.

**You:**
> Yes, submit to test first.

**Claude:**
```python
# Step 4: Publish
from mdf_agent.skill.handlers import publish

result = publish(
    path="examples/xrd-patterns",
    test=True,
    submit=True
)
```

> **Published successfully!**
>
> - Source ID: `mdf-xrd-iron-oxide-2026`
> - Status: Processing in test environment
> - Check status: `mdf status --source-id mdf-xrd-iron-oxide-2026`
>
> Once you verify it looks correct, say "publish to production" and I'll submit the final version.

---

## What Just Happened?

| Step | Human (CLI) | AI Agent |
|------|-------------|----------|
| Understand data | You read files manually | `scan_folder()` auto-extracts |
| Create manifest | `mdf init` with flags | `create_manifest()` from conversation |
| Add metadata | Edit YAML by hand | Claude fills from context |
| Validate | `mdf validate` | `validate_and_preview()` |
| Publish | `mdf publish --submit` | `publish(submit=True)` |

**The AI workflow reduces a 10-step process to a conversation.**

---

## Try It Yourself

```bash
# Clone this repo
git clone https://github.com/materials-data-facility/mdf_client
cd mdf_client

# Install
pip install -e .

# Human workflow
cd examples/dft-alloys
mdf init . --title "My Dataset" --author "Your Name"
mdf add *.csv *.json
mdf commit -m "Initial commit"
mdf validate
mdf publish --dry-run

# AI workflow
# Open Claude Code in examples/xrd-patterns and say:
# "Publish this data to MDF"
```

---

## Sample Data Included

### `examples/dft-alloys/`
- `calculations.csv` - Formation energies for 15 Al-X intermetallics
- `parameters.json` - VASP calculation settings
- `README.txt` - Paper abstract with authors and DOI

### `examples/xrd-patterns/`
- `sample_001.csv` - XRD pattern for Fe2O3
- `sample_002.csv` - XRD pattern for Fe3O4
- `instrument.yaml` - Rigaku SmartLab configuration

---

## Key Commands

| Command | Description |
|---------|-------------|
| `mdf init` | Create new dataset repository |
| `mdf add` | Stage files (with `--discover` for auto-metadata) |
| `mdf commit` | Record staged files |
| `mdf status` | Show repository state |
| `mdf validate` | Check manifest before publishing |
| `mdf publish` | Submit to MDF Connect (`--dry-run` for preview) |
| `mdf clone` | Derive from existing dataset |

---

*MDF Agent: From data folder to published dataset in minutes, not hours.*
