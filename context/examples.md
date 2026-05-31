# MDF Agent and MDF Connect v2 Examples

These examples show how the new MDF Agent CLI and MDF Connect v2 backend can be demonstrated in reports, walkthroughs, or rendered documentation. The commands assume the repository root is the working directory.

## 1. CLI: First-Time Setup

The MDF Agent CLI gives researchers a direct terminal workflow for authentication, configuration, publication, search, and reuse.

```bash
# Show the welcome screen and available commands
mdf

# Authenticate with Globus
mdf login

# Check local configuration and environment readiness
mdf doctor

# Show configured defaults
mdf config show
```

## 2. CLI: Publish a Dataset in One Command

For a small dataset, a user can publish directly from a local folder without hand-writing a manifest first.

```bash
mdf publish ./examples/xrd-patterns \
  --title "XRD Patterns of Iron Oxide Nanoparticles" \
  --author "Ben Johnson" \
  --author "Sarah Williams" \
  --submit
```

The CLI can also run in preview mode before sending anything to the backend.

```bash
mdf publish ./examples/xrd-patterns \
  --title "XRD Patterns of Iron Oxide Nanoparticles" \
  --author "Ben Johnson" \
  --author "Sarah Williams" \
  --dry-run
```

## 3. CLI: Clone a Dataset for Local Reuse

After a dataset has been found, the CLI can bring the files back into a local working directory for analysis. This is the other half of publication: data should be easy to reuse, not only easy to cite.

```bash
# Inspect the dataset before downloading files
mdf show abx3_perovs_alloys_v1

# Preview file and profile information
mdf preview abx3_perovs_alloys_v1

# Clone the dataset into a local folder
mdf clone abx3_perovs_alloys_v1 ./abx3-perovskites
```

Users can also request a specific version or create a local manifest that records the new work as derived from the cloned dataset.

```bash
# Clone a specific published version
mdf clone abx3_perovs_alloys_v1 ./abx3-v1 --version 1.0

# Clone a dataset by DOI when a DOI is easier to paste than a source ID
mdf clone 10.18126/example ./dataset-from-doi

# Create a local mdf.yaml with derived-from lineage
mdf clone abx3_perovs_alloys_v1 ./derived-analysis --derive
```

For stream files, the CLI can list what has been collected. Scripts can then use the Python backend client for filtered downloads.

```bash
mdf stream files \
  --stream-id stream-abc123 \
  --api-url http://127.0.0.1:8080
```

## 4. CLI: Search, Inspect, Cite, and Clone

After publication, the same client supports discovery and reuse.

```bash
# Search MDF
mdf search "perovskite"

# Show a formatted dataset card
mdf show abx3_perovs_alloys_v1

# View version history
mdf versions abx3_perovs_alloys_v1

# Export citation metadata
mdf cite abx3_perovs_alloys_v1 -f bibtex

# Preview dataset files and profile information
mdf preview abx3_perovs_alloys_v1

# Clone files into a local working directory
mdf clone abx3_perovs_alloys_v1 ./local-copy
```

## 5. CLI: Streaming Data from an Instrument Run

Streaming commands demonstrate the route from instrument-style output to a dataset submission.

```bash
# Start a stream for an experimental campaign
mdf stream create \
  --title "Argonne XRD+TGA Stream - ANL-SAMPLE-042" \
  --lab-id anl-xrd-tga \
  --organization ANL \
  --api-url http://127.0.0.1:8080

# Upload files as they are produced
mdf stream upload \
  --stream-id stream-abc123 \
  examples/xrd-patterns/sample_001.csv \
  examples/xrd-patterns/sample_002.csv \
  --api-url http://127.0.0.1:8080

# Check stream status
mdf stream status \
  --stream-id stream-abc123 \
  --api-url http://127.0.0.1:8080

# List files attached to the stream
mdf stream files \
  --stream-id stream-abc123 \
  --api-url http://127.0.0.1:8080

# Snapshot the stream into a dataset submission
mdf stream snapshot \
  --stream-id stream-abc123 \
  --title "XRD and TGA Measurements for ANL-SAMPLE-042" \
  --api-url http://127.0.0.1:8080

# Close the stream when the run is complete
mdf stream close \
  --stream-id stream-abc123 \
  --mint-doi \
  --api-url http://127.0.0.1:8080
```

## 6. Backend: Start the Local v2 Service

The v2 backend can run locally with SQLite, local file storage, development authentication, and mock DOI minting.

```bash
cd cs/aws
python -m v2.app.main
```

The repository also includes a convenience script:

```bash
cd cs/aws/v2
./local_start.sh
```

The local API is available at:

```text
http://127.0.0.1:8080
```

Health check:

```bash
curl http://127.0.0.1:8080/health
```

Example response:

```json
{
  "status": "ok",
  "service": "mdf-v2"
}
```

## 7. Backend API: Submit a Dataset

The backend accepts a flat v2 metadata payload and creates a versioned submission record.

```bash
curl -X POST http://127.0.0.1:8080/submit \
  -H "Content-Type: application/json" \
  -H "X-User-Id: demo-user" \
  -d '{
    "title": "High-Throughput DFT Study of Perovskite Stability",
    "authors": [
      {
        "name": "Alice Chen",
        "affiliations": ["Argonne National Laboratory"]
      },
      {
        "name": "Raj Kumar",
        "affiliations": ["University of Chicago"]
      }
    ],
    "description": "Density functional theory calculations examining thermodynamic stability of perovskite compositions.",
    "keywords": ["perovskite", "DFT", "solar cells", "stability"],
    "data_sources": ["https://example.org/perovskite_dft_2026/data.csv"],
    "organization": "MDF Open",
    "license": {
      "name": "CC-BY-4.0"
    }
  }'
```

Example response:

```json
{
  "success": true,
  "source_id": "high_throughput_dft_study_of_perovskite_stability",
  "version": "1.0",
  "versioned_source_id": "high_throughput_dft_study_of_perovskite_stability-1.0",
  "status": "pending_curation"
}
```

## 8. Backend API: Curation and Publication

Curators can list pending submissions, inspect a record, approve it, reject it, or request revisions.

```bash
# List pending submissions
curl http://127.0.0.1:8080/curation/pending \
  -H "X-User-Id: curator-user"

# Approve and mint a DOI
curl -X POST \
  http://127.0.0.1:8080/curation/high_throughput_dft_study_of_perovskite_stability/approve \
  -H "Content-Type: application/json" \
  -H "X-User-Id: curator-user" \
  -d '{
    "notes": "Metadata reviewed and approved.",
    "mint_doi": true
  }'
```

Example rejection:

```bash
curl -X POST \
  http://127.0.0.1:8080/curation/high_throughput_dft_study_of_perovskite_stability/reject \
  -H "Content-Type: application/json" \
  -H "X-User-Id: curator-user" \
  -d '{
    "reason": "Methods section needs more detail.",
    "suggestions": "Add calculation settings, code version, and convergence criteria."
  }'
```

## 9. Backend API: Dataset Cards, Citations, and Search

The backend exposes public-style endpoints that can support a frontend, scripts, or the CLI.

```bash
# Dataset card
curl http://127.0.0.1:8080/card/high_throughput_dft_study_of_perovskite_stability

# Citation export
curl "http://127.0.0.1:8080/citation/high_throughput_dft_study_of_perovskite_stability?format=bibtex"

# Search
curl "http://127.0.0.1:8080/search?q=perovskite&type=datasets&limit=10"

# Faceted search
curl "http://127.0.0.1:8080/search?q=perovskite&year=2026&organization=MDF%20Open"
```

## 10. Backend API: Create and Snapshot a Stream

The stream API supports ongoing data collection and later promotion into a dataset submission.

```bash
# Create a stream
curl -X POST http://127.0.0.1:8080/stream/create \
  -H "Content-Type: application/json" \
  -H "X-User-Id: demo-user" \
  -d '{
    "title": "Argonne XRD+TGA Stream - ANL-SAMPLE-042",
    "lab_id": "anl-xrd-tga",
    "organization": "ANL",
    "metadata": {
      "facility": "Argonne National Laboratory",
      "instruments": ["XRD", "TGA"],
      "sample_id": "ANL-SAMPLE-042",
      "run_id": "RUN-2026-01-31-01",
      "operator": "A. Researcher"
    }
  }'
```

Append file metadata:

```bash
curl -X POST http://127.0.0.1:8080/stream/stream-abc123/append \
  -H "Content-Type: application/json" \
  -H "X-User-Id: demo-user" \
  -d '{
    "files": [
      {
        "filename": "pattern_001.xy",
        "size": 24576
      },
      {
        "filename": "tga_run_001.csv",
        "size": 10240
      }
    ],
    "last_file": {
      "instrument": "TGA",
      "sample_id": "ANL-SAMPLE-042",
      "run_id": "RUN-2026-01-31-01",
      "timestamp": "2026-01-31T10:02:00Z"
    }
  }'
```

Snapshot the stream:

```bash
curl -X POST http://127.0.0.1:8080/stream/stream-abc123/snapshot \
  -H "Content-Type: application/json" \
  -H "X-User-Id: demo-user" \
  -d '{
    "title": "XRD and TGA Measurements for ANL-SAMPLE-042",
    "description": "Snapshot of XRD and TGA data collected during an Argonne instrument run.",
    "author": "A. Researcher"
  }'
```

## 11. Python SDK: Publish and Search

The same workflows are available from Python for notebooks, scripts, and automated pipelines.

```python
from mdf_agent import MDFAgent

agent = MDFAgent.init_manifest(
    "./examples/dft-alloys",
    title="High-Throughput DFT Study of Binary Intermetallic Alloys",
    authors=["Jane Doe", "John Smith"],
    description="Formation energies and electronic properties for Al-X intermetallics",
)

agent.manifest.data_sources = [
    "./examples/dft-alloys/calculations.csv",
    "./examples/dft-alloys/parameters.json",
]
agent.save_manifest()

result = agent.publish(
    service_instance="staging",
    dry_run=False,
)

print(result["source_id"])
```

Search from Python:

```python
from mdf_agent import MDFAgent

agent = MDFAgent()
results = agent.search("perovskite", service_instance="staging")

for item in results.get("results", []):
    print(item["title"], item.get("doi"))
```

## 12. Python Backend Client: Streams and Curation

The lower-level backend client is useful for scripts and integration tests.

```python
from mdf_agent import BackendClient

client = BackendClient.authenticated(service_instance="local")

stream = client.stream_create(
    title="Autonomous XRD Synthesis Campaign",
    lab_id="selfdriving-lab-01",
    organization="ANL",
)

stream_id = stream["stream_id"]

client.stream_append(
    stream_id,
    files=[
        {
            "path": "xrd/sample_001.xy",
            "size": 24576,
            "instrument": "XRD",
            "sample_id": "sample-001",
        }
    ],
)

snapshot = client.stream_snapshot(
    stream_id,
    title="Autonomous XRD Synthesis Campaign Snapshot",
)

print(snapshot["source_id"])
client.close()
```

Filtered stream clone from Python:

```python
from mdf_agent import BackendClient

client = BackendClient.authenticated(service_instance="local")

result = client.stream_clone(
    stream_id="stream-abc123",
    dest_dir="./stream-csv-files",
    file_filter="*.csv",
)

print(result["downloaded"])
client.close()
```

Curation from Python:

```python
from mdf_agent import BackendClient

client = BackendClient.authenticated(service_instance="local")

pending = client.curation_pending(limit=20)

for submission in pending.get("submissions", []):
    print(submission["source_id"], submission["status"])

client.curation_approve(
    source_id="high_throughput_dft_study_of_perovskite_stability",
    notes="Reviewed and approved.",
    mint_doi=True,
)

client.close()
```

## 13. Example Output: Dataset Card

A dataset card gives users enough context to decide whether the dataset is relevant before downloading it.

```json
{
  "success": true,
  "source_id": "abx3_perovs_alloys_v1",
  "version": "1.0",
  "card": {
    "title": "ABX3 Perovskite Alloys Dataset",
    "authors": ["Chibueze Amanchukwu", "Chris Wolverton"],
    "description": "A dataset of ABX3 perovskite alloy calculations.",
    "keywords": ["perovskite", "DFT", "alloys"],
    "publisher": "Materials Data Facility",
    "publication_year": 2026,
    "organization": "MDF Open",
    "status": "published",
    "doi": "10.18126/example",
    "stats": {
      "file_count": 42,
      "total_bytes": 10485760,
      "file_types": ["csv", "json"]
    },
    "profile_summary": {
      "total_files": 3,
      "formats": {
        "csv": 2,
        "json": 1
      },
      "sample_rows": [
        {
          "composition": "CsPbI3",
          "bandgap": 1.73
        }
      ]
    }
  }
}
```

## 14. Example Output: Stream Summary

Stream records preserve the run context while data are still being produced.

```json
{
  "success": true,
  "stream": {
    "stream_id": "stream-abc123",
    "title": "Argonne XRD+TGA Stream - ANL-SAMPLE-042",
    "status": "open",
    "lab_id": "anl-xrd-tga",
    "organization": "ANL",
    "file_count": 2,
    "total_bytes": 34816,
    "metadata": {
      "facility": "Argonne National Laboratory",
      "instruments": ["XRD", "TGA"],
      "sample_id": "ANL-SAMPLE-042",
      "run_id": "RUN-2026-01-31-01"
    }
  }
}
```
