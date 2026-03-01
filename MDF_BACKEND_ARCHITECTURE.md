# MDF Backend Server: Architecture Design

---

## Current Architecture (Actual - cs repo)

The **actual production system** is in the `cs` repo (not `connect_server`, which is legacy). It uses **AWS Lambda + Globus Flows** - a much cleaner architecture than the old SQS processor.

### System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          MDF Connect Architecture                                │
└─────────────────────────────────────────────────────────────────────────────────┘

  Client                API Gateway              Lambda                Globus Flow
    │                       │                      │                       │
    │  POST /submit         │                      │                       │
    │──────────────────────>│  auth.py             │                       │
    │                       │─────────────────────>│                       │
    │                       │  (Globus Auth check) │                       │
    │                       │<─────────────────────│                       │
    │                       │                      │                       │
    │                       │  submit.py           │                       │
    │                       │─────────────────────>│                       │
    │                       │                      │  AutomateManager      │
    │                       │                      │  .submit()            │
    │                       │                      │─────────────────────>│
    │                       │                      │                       │
    │                       │                      │  Flow triggered       │
    │                       │                      │<─────────────────────│
    │                       │                      │                       │
    │  {source_id, version} │                      │                       │
    │<──────────────────────│<─────────────────────│                       │
    │                       │                      │                       │
    │                       │                      │       ┌───────────────┴───────────────┐
    │                       │                      │       │   GLOBUS FLOW EXECUTION       │
    │                       │                      │       │   (runs async, hours/days)    │
    │                       │                      │       │                               │
    │                       │                      │       │  1. UserTransfer (Globus)     │
    │                       │                      │       │  2. Xtraction (metadata)      │
    │                       │                      │       │  3. Curation (optional)       │
    │                       │                      │       │  4. SearchIngest (SIAP)       │
    │                       │                      │       │  5. DataDestTransfer          │
    │                       │                      │       │  6. SearchUpdate              │
    │                       │                      │       │  7. Notify User               │
    │                       │                      │       └───────────────────────────────┘
```

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| **Lambda: auth** | `cs/aws/auth.py` | Globus Auth token validation |
| **Lambda: submit** | `cs/aws/submit.py` | Validate metadata & trigger Flow |
| **Lambda: status** | `cs/aws/status.py` | Query submission status |
| **Lambda: submissions** | `cs/aws/submissions.py` | List user's submissions |
| **AutomateManager** | `cs/aws/automate_manager.py` | FlowsClient wrapper |
| **DynamoManager** | `cs/aws/dynamo_manager.py` | Database operations |
| **Flow Definition** | `cs/automate/mdf_flow_def.json` | State machine (JSON) |
| **Flow DSL** | `cs/automate/minimus_mdf_flow.py` | Python DSL to generate Flow |
| **Infra (Terraform)** | `cs/infra/mdf/` | Lambda, API Gateway, DynamoDB |

### Globus Flow States

```
StartSubmission → UserPermissions → UserTransfer → CheckUserTransfer
                                                         │
                              ┌──────────────────────────┴──────────────────────────┐
                              │ SUCCESS                                      FAIL   │
                              ▼                                                     ▼
                         Xtraction                                         FailUserTransfer
                              │                                                     │
                              ▼                                                     ▼
                       ChooseCuration ───────────────────────────────> ChooseNotifyUserEnd
                              │                                                     │
               ┌──────────────┴──────────────┐                                      │
               │ curation=true        false  │                                      │
               ▼                             ▼                                      │
        CurateSubmission              SearchIngest ◄────────────────────────────────┘
               │                             │
               ▼                             ▼
        ChooseAcceptance              DataDestTransfer
               │                             │
               ▼                             ▼
        SearchIngest                  ChoosePublish → MDFPublish → ChooseCitrine → ...
                                             │
                                             ▼
                                      PrepareSearchUpdate → SearchUpdate → EndSubmission
```

### External Services

| Service | Endpoint/ID | Purpose |
|---------|-------------|---------|
| **SIAP** | `https://siap.globuscs.info/` | Search ingest action provider |
| **Globus Search (dev)** | `ab71134d-0b36-473d-aa7e-7b19b2124c88` | Development index |
| **Globus Search (prod)** | `1a57bbe5-5272-477f-9d31-343b8258b7a5` | Production index |
| **Globus Search (test)** | `5acded0c-a534-45af-84be-dcf042e36412` | Test submissions |
| **Flow (dev)** | `0c7ee169-cefc-4a23-81e1-dc323307c863` | Development flow |
| **DataCite** | Via secrets | DOI minting |

### Database Schema (Supabase)

**Primary Key:** `(source_id, version)`

```sql
datasets (
    source_id VARCHAR(255),
    version VARCHAR(50),
    versioned_source_id VARCHAR(255),
    user_id VARCHAR(255),
    organization VARCHAR(255),
    status VARCHAR(50),
    dataset_mdata JSONB,          -- Full metadata
    previous_versions TEXT[],
    active BOOLEAN,
    cancelled BOOLEAN,
    hibernating BOOLEAN,
    code VARCHAR(255),            -- Legacy status tracking
    messages TEXT[],
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
```

---

## Is This Overengineered?

**Short answer: No, but it could be simpler.**

### What's Actually Reasonable

Given the constraints (~10 datasets/month, TB-scale transfers, 24+ hour processing), the current architecture is **appropriate**:

| Aspect | Current Approach | Assessment |
|--------|-----------------|------------|
| **Lambdas for API** | 4 small functions | ✅ Right-sized, serverless scales to zero |
| **Globus Flows for orchestration** | State machine | ✅ Perfect for long-running transfers |
| **Globus Transfer** | Built-in action | ✅ Only option for TB-scale data |
| **Globus Search** | SIAP action provider | ✅ Federated search infrastructure |

### What IS Overengineered

| Issue | Current State | Recommendation |
|-------|---------------|----------------|
| **Two repos** | `cs` (active) + `connect_server` (dead) | Archive `connect_server`, it causes confusion |
| **Database migration limbo** | DynamoDB → Supabase incomplete | Pick one and finish |
| **12 status steps** | Legacy from old processor | Simplify to 4-5 steps for Flow model |
| **Curation workflow** | Complex email/approval system | Rarely used, could be optional module |
| **Multiple search indexes** | dev/test/prod separation | Good, but consider single dev+test |

### Complexity Budget Analysis

```
Current complexity breakdown:

  Lambda code:        ~400 lines (reasonable)
  Flow definition:    ~320 lines (reasonable)
  DynamoManager:      ~800 lines (could trim to 400)
  AutomateManager:    ~230 lines (clean)
  SourceIDManager:    ~900 lines (overcomplicated)
  Terraform:          ~200 lines (minimal)
  ─────────────────────────────────────
  Total active code:  ~2,850 lines

  Legacy (connect_server): ~15,000+ lines (DELETE THIS)
```

---

## Simplification Plan

### Phase 0: Clean House (1 week)

- [ ] **Archive `connect_server` repo** - Move to `mdf-connect-legacy` or delete
- [ ] **Document actual architecture** - Update README in `cs` repo
- [ ] **Remove dead code paths** - Citrine, MRR, unused extractors
- [ ] **Consolidate config** - Single source of truth for env vars

### Phase 1: Database Decision (1 week)

**Analysis of actual access patterns:**

| Access Pattern | Frequency | Query Type |
|----------------|-----------|------------|
| Get by `source_id` + `version` | Every request | Primary key |
| List by `user_id` | Dashboard loads | GSI query |
| List by `organization` | Admin views | GSI query |
| Update status | During processing | Key update |
| Store full metadata | On submit | JSON blob (not queried) |

**What we DON'T need:**
- Complex JSONB queries → Globus Search handles discovery
- Real-time subscriptions → Polling is fine for ~10/month
- Row-level security → Auth handled at API layer via Globus
- SQL joins → Single-table design works

**Database comparison:**

| Option | Fit | Rationale |
|--------|-----|-----------|
| **DynamoDB** | ✅ Best | Key-value + GSI matches patterns exactly, AWS-native, pay-per-request scales to zero, no external deps |
| **Supabase** | ⚠️ Overkill | Adds external service dependency, SQL power unused, solves problems we don't have |
| **SQLite/Turso** | ❌ Risky | Emerging, blob size limits, not battle-tested at TB metadata scale |

**Recommendation: Stay with DynamoDB**

The Supabase migration was likely motivated by developer ergonomics, not actual requirements. DynamoDB is:
- Already integrated and working
- Matches access patterns perfectly
- Pay-per-request ideal for low volume
- No external service to manage/pay for
- Streaming use case is still key-value (stream_id → metadata)

**Proper DynamoDB design for new infrastructure:**

```
Table: mdf-connect-v2-submissions

Primary Key:
  PK: source_id (String)
  SK: version (String)

GSI1 (user-submissions):
  PK: user_id
  SK: updated_at

GSI2 (org-submissions):
  PK: organization
  SK: source_id

Attributes:
  source_id, version, versioned_source_id
  user_id, user_email, organization
  status (submitted|transferring|processing|indexing|complete|failed)
  dataset_mdata (JSON string - full DataCite metadata)
  action_id (Globus Flow run ID)
  created_at, updated_at
  test (boolean)
```

For streaming, add a second table:

```
Table: mdf-connect-v2-streams

Primary Key:
  PK: stream_id (String)

GSI1 (lab-streams):
  PK: lab_id
  SK: created_at

Attributes:
  stream_id, lab_id, title
  status (open|closed|archived)
  file_count, total_bytes
  last_append_at, created_at
  user_id, organization
```

- [ ] Design new DynamoDB tables (above schema)
- [ ] Abandon Supabase migration (remove dead code)
- [ ] Keep existing DynamoDB for current system (parallel operation)

### Phase 2: Simplify Status Tracking (1 week)

Current 12 steps → New 5 steps:

| Old Steps | New Step |
|-----------|----------|
| sub_start, old_cancel | `submitted` |
| data_download, data_transfer | `transferring` |
| extracting, curation | `processing` |
| ingest_search, ingest_backup, ingest_publish, ingest_citrine, ingest_mrr | `indexing` |
| ingest_cleanup | `complete` / `failed` |

- [ ] Update Flow to report simplified status
- [ ] Update database schema
- [ ] Update status Lambda to translate for backward compatibility

### Phase 3: Streamline Flow Definition (2 weeks)

The current Flow has vestigial states. Simplify to:

```
StartSubmission
    │
    ▼
UserTransfer (Globus Transfer action)
    │
    ├─── FAIL ──> NotifyFailure ──> End
    │
    ▼ SUCCESS
SearchIngest (SIAP action)
    │
    ▼
UpdateSearchEntry (add DOI, finalize metadata)
    │
    ▼
NotifySuccess
    │
    ▼
End
```

**Remove:**
- Xtraction state (no longer extracting server-side)
- Curation states (make optional, trigger via separate endpoint)
- Citrine/MRR states (unused)
- Multiple destination transfers (single destination sufficient)

### Phase 4: Add Streaming Layer (3 weeks)

Once simplified, add streaming support:

```
New endpoints (Lambda functions):

POST /stream/create      → Create stream, return stream_id
POST /stream/:id/append  → Record file metadata
POST /stream/:id/close   → Finalize, optionally mint DOI
GET  /stream/:id         → Stream status

New DynamoDB/Supabase table:

streams (
    stream_id VARCHAR PRIMARY KEY,
    lab_id VARCHAR,
    title VARCHAR,
    status VARCHAR,          -- open, closed, archived
    file_count INTEGER,
    total_bytes BIGINT,
    last_append_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ
)
```

---

## Deployability Improvements

### Current Pain Points

1. **Docker images for Lambdas** - Adds build complexity
2. **Separate Flow deployment** - GitHub release triggers
3. **Secrets scattered** - AWS Secrets Manager + env vars + Terraform
4. **No local development** - Hard to test without deploying

### Recommendations

#### 1. Switch to ZIP-based Lambdas

Docker images are overkill for ~400 lines of Python:

```hcl
# Instead of:
image_uri = "${var.ecr_repos["submit"]}:${var.env}"

# Use:
filename         = "submit.zip"
source_code_hash = filebase64sha256("submit.zip")
handler          = "submit.lambda_handler"
runtime          = "python3.11"
```

**Benefits:** Faster deploys, simpler CI/CD, no ECR costs.

#### 2. Single Deployment Command

```bash
# Current (multiple steps):
git push origin dev          # Triggers Lambda deploy
gh release create v1.0.0     # Triggers Flow deploy (separate)

# Better (one command):
make deploy ENV=dev          # Deploys everything
```

#### 3. Local Development Setup

```python
# local_server.py
from flask import Flask
from aws.submit import lambda_handler

app = Flask(__name__)

@app.route('/submit', methods=['POST'])
def submit():
    event = {"body": request.data, "headers": dict(request.headers), ...}
    return lambda_handler(event, None)

if __name__ == '__main__':
    app.run(port=5000)
```

#### 4. Consolidated Secrets

```yaml
# secrets.yaml (encrypted with SOPS or AWS KMS)
dev:
  DATACITE_USERNAME: "..."
  DATACITE_PASSWORD: "..."
  API_CLIENT_ID: "..."
  API_CLIENT_SECRET: "..."
  SES_ACCESS_KEY: "..."

prod:
  DATACITE_USERNAME: "..."
  ...
```

---

## Summary: Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     Simplified MDF Connect v2 Architecture                       │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────┐
                              │  API Gateway    │
                              │  /submit        │
                              │  /status        │
                              │  /submissions   │
                              │  /stream/*      │  ◄── NEW
                              └────────┬────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
              ▼                        ▼                        ▼
     ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
     │  submit.py      │    │  status.py      │    │  stream.py      │
     │  (ZIP Lambda)   │    │  (ZIP Lambda)   │    │  (ZIP Lambda)   │
     └────────┬────────┘    └────────┬────────┘    └────────┬────────┘
              │                      │                      │
              │                      ▼                      │
              │             ┌─────────────────┐             │
              │             │    DynamoDB     │◄────────────┘
              │             │  v2-submissions │
              │             │  v2-streams     │
              │             └─────────────────┘
              │
              ▼
     ┌─────────────────┐
     │  Globus Flow    │
     │  (simplified)   │
     │                 │
     │  Transfer ──────┼──> Globus Transfer
     │      │          │
     │      ▼          │
     │  SearchIngest ──┼──> SIAP / Globus Search
     │      │          │
     │      ▼          │
     │  Notify ────────┼──> Email (SES)
     └─────────────────┘
```

**Why DynamoDB over Supabase:**
- Access patterns are pure key-value (source_id → record, user_id → list)
- Complex queries handled by Globus Search, not database
- Pay-per-request scales to zero for low volume
- No external service dependency (all AWS)
- Streaming is also key-value (stream_id → metadata)

**Total complexity:** ~1,200 lines (down from ~2,850 active + 15,000 legacy)

---

## Performance Analysis: Why Current System Is Slow

The current system takes **~1 minute for a simple status check**. This is unacceptable.

### Root Cause Analysis

Looking at `cs/aws/status.py`:

```python
def lambda_handler(event, context):
    dynamo_manager = DynamoManager()
    automate_manager = AutomateManager(get_secret(...))  # ← Network call #1
    automate_manager.authenticate()                       # ← Network call #2 (OAuth!)

    status_rec = dynamo_manager.read_status_record(...)   # ← Network call #3

    result = {
        "flow_status": automate_manager.get_status(...)   # ← Network call #4
    }
```

**Every status request makes 4+ network calls:**

| Call | Target | Latency | Purpose |
|------|--------|---------|---------|
| 1 | AWS Secrets Manager | ~100ms | Fetch OAuth credentials |
| 2 | Globus Auth | ~500-2000ms | OAuth client credentials flow |
| 3 | DynamoDB | ~10-50ms | Read status record |
| 4 | Globus Flows API | ~500-2000ms | Get Flow run status |

**Plus cold start overhead:**

| Lambda Type | Cold Start |
|-------------|------------|
| Docker-based (current) | 5-15 seconds |
| ZIP-based | 500ms-2s |
| With provisioned concurrency | <100ms |

**Total worst case: 15s cold start + 5s network = 20+ seconds**
**Warm case: 4-5 seconds** (still too slow for status check)

### Design Flaws

1. **Fetching Flow status on every call** - The Flow status is fetched from Globus API every time, even though it only changes when the Flow progresses (rare)

2. **No credential caching** - OAuth tokens are fetched fresh every request, but they're valid for hours

3. **No connection reuse** - New HTTP clients created per request

4. **Docker images** - Massive cold start penalty for simple Python functions

### v2 Performance Requirements

| Operation | Target Latency | Strategy |
|-----------|---------------|----------|
| GET /status | <200ms | Read from DynamoDB only, no external calls |
| POST /submit | <2s | Single Globus call (trigger Flow) |
| POST /stream/append | <100ms | DynamoDB write only |
| GET /stream/:id | <100ms | DynamoDB read only |

### v2 Performance Design

#### 1. Store Status in DynamoDB (Don't Fetch from Globus)

```
CURRENT (slow):
  Client → Lambda → Globus Flows API → return status

v2 (fast):
  Client → Lambda → DynamoDB → return status

  Flow updates status via callback:
  Flow step completes → Lambda action → DynamoDB update
```

The Flow itself updates DynamoDB as it progresses. Status endpoint just reads from DynamoDB.

#### 2. Cache OAuth Tokens

```python
# BAD (current) - fetch every request
def lambda_handler(event, context):
    automate_manager.authenticate()  # Network call every time

# GOOD (v2) - cache in Lambda memory
_cached_token = None
_token_expires = 0

def get_cached_token():
    global _cached_token, _token_expires
    if time.time() > _token_expires - 300:  # Refresh 5 min early
        _cached_token = fetch_new_token()
        _token_expires = time.time() + 3600
    return _cached_token
```

#### 3. Use ZIP-based Lambdas

```hcl
# v2 Terraform
resource "aws_lambda_function" "status" {
  function_name = "MDF-Connect3-status"
  runtime       = "python3.11"
  handler       = "status.lambda_handler"
  filename      = "status.zip"
  memory_size   = 256  # More memory = faster CPU
  timeout       = 10
}
```

#### 4. Minimal Dependencies

```
# v2 requirements.txt (status Lambda)
boto3  # Already in Lambda runtime, don't bundle
```

Compare to current which bundles entire `globus_sdk`, `globus_automate_client`, etc.

#### 5. Status Lambda v2 (Target: <50 lines)

```python
import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('mdf-connect-v2-submissions')

def lambda_handler(event, context):
    source_id = event['pathParameters']['source_id']
    version = event.get('queryStringParameters', {}).get('version')

    if version:
        response = table.get_item(Key={'source_id': source_id, 'version': version})
    else:
        # Get latest version
        response = table.query(
            KeyConditionExpression='source_id = :sid',
            ExpressionAttributeValues={':sid': source_id},
            ScanIndexForward=False,
            Limit=1
        )
        response = {'Item': response['Items'][0]} if response['Items'] else {}

    if 'Item' not in response:
        return {'statusCode': 404, 'body': json.dumps({'error': 'Not found'})}

    return {
        'statusCode': 200,
        'body': json.dumps(response['Item'], default=str)
    }
```

**Expected latency: 10-50ms** (DynamoDB only, no external calls)

---

## Globus Search Schema Analysis

### Hard Constraints

1. **Type immutability**: Once a key is added with a type, it cannot be changed
2. **Key limit**: ~1000 keys including all nested subkeys
3. **Data doesn't move**: Files stay on Globus endpoints; only index entries change

### v2 Schema: Clean Slate Design

**Key insight: Record entries are no longer needed.** In v1, we indexed individual files with extracted metadata (`material`, `dft`, `crystal_structure`, etc.). This caused schema bloat and key exhaustion.

In v2:
- **Datasets** = discoverable, citable units (what users search for)
- **Streams** = live data feeds (separate index, different use case)
- **No record-level indexing** = no `files[]`, `material`, `dft`, `custom`, etc.

### What Actually Needs to be Discoverable?

| Field | Purpose | Searchable? |
|-------|---------|-------------|
| Title, description | Find by topic | Yes (full-text) |
| Authors | Find by researcher | Yes |
| DOI | Citation lookup | Yes (exact) |
| Keywords/subjects | Topic filtering | Yes (faceted) |
| Organization | Filter by source | Yes (faceted) |
| Date | Time-based filtering | Yes (range) |
| Data location | Access the files | No (just stored) |
| File count, size | Informational | No (just stored) |

Everything else (material composition, DFT parameters, crystal structure) is **domain-specific** and should be:
1. Stored in the dataset's own metadata files
2. Searchable via specialized tools (not general MDF search)
3. Or added to a future domain-specific index

### v2 Dataset Schema (Minimal)

**Target: ~25 keys total**

```json
{
  "mdf": {
    "source_id": "fe_al_dft_v1.0",
    "source_name": "fe_al_dft",
    "version": "1.0",
    "organization": "MDF Open",
    "acl": ["public"],
    "ingest_date": "2024-01-31T12:00:00Z"
  },

  "dc": {
    "title": "Fe-Al Intermetallic Formation Energies",
    "creators": ["Doe, Jane", "Smith, John"],
    "publisher": "Materials Data Facility",
    "year": 2024,
    "doi": "10.18126/abc123",
    "description": "DFT calculations of formation energies...",
    "subjects": ["DFT", "intermetallics", "iron", "aluminum"],
    "license": "CC-BY-4.0"
  },

  "data": {
    "location": "globus://endpoint-uuid/datasets/fe_al_dft/v1.0/",
    "size_bytes": 1234567890,
    "file_count": 42
  }
}
```

**Key count breakdown:**

```
mdf.*     6 keys
dc.*      9 keys
data.*    3 keys
──────────────────
Total:    18 keys

Headroom: 982 keys (massive safety margin)
```

### What We're Removing

| v1 Field | Why Remove |
|----------|------------|
| `mdf.scroll_id` | No records, no scrolling |
| `mdf.resource_type` | Always "dataset" |
| `mdf.versioned_source_id` | Redundant with source_id |
| `mdf.domains` | Unused |
| `dc.titles[]` | Flatten to single `dc.title` |
| `dc.creators[]` objects | Flatten to string array |
| `dc.resourceType` | Always "Dataset" |
| `dc.identifier` | Just use `dc.doi` |
| `services` | Unused |
| `mrr` | Unused |
| `custom` | Key explosion risk |
| `files[]` | No record indexing |
| `material.*` | Domain-specific, not general search |
| `dft.*` | Domain-specific |
| `crystal_structure.*` | Domain-specific |
| `calphad.*` | Domain-specific |

### Streams: Separate Index

Streams have fundamentally different characteristics:
- High volume (1000s of appends/day)
- Ephemeral (may be deleted)
- Different access patterns (by lab, by time range)
- Different ACLs (often private to lab)

**Recommendation: Dedicated `mdf-streams` index**

```json
{
  "stream": {
    "id": "stream_abc123",
    "lab_id": "argonne-lab-42",
    "title": "Perovskite Synthesis Campaign",
    "status": "open",
    "created_at": "2024-01-15T00:00:00Z",
    "last_append": "2024-01-31T12:34:56Z",
    "append_count": 1247,
    "acl": ["urn:globus:auth:identity:user-uuid"]
  },

  "data": {
    "location": "globus://endpoint-uuid/streams/abc123/",
    "size_bytes": 52428800
  },

  "summary": {
    "compositions": ["BaTiO3", "SrTiO3", "PbTiO3"],
    "yield_range": [45.2, 98.7],
    "file_types": ["xy", "csv"]
  }
}
```

**Key count: ~15 keys**

The `summary` block contains **aggregated** metadata (updated periodically), not per-file metadata. This keeps the index small while still enabling discovery.

### Index Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         v2 SEARCH INDEX ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────┐      ┌─────────────────────────────┐
  │     mdf-datasets-v2         │      │      mdf-streams-v2         │
  │                             │      │                             │
  │  • Published datasets       │      │  • Active streams           │
  │  • DOI-minted               │      │  • Lab data feeds           │
  │  • Permanent                │      │  • May be ephemeral         │
  │  • ~18 keys/entry           │      │  • ~15 keys/entry           │
  │  • ~10 new entries/month    │      │  • ~1000s entries/month     │
  │                             │      │                             │
  │  Schema: mdf, dc, data      │      │  Schema: stream, data,      │
  │                             │      │           summary           │
  └─────────────────────────────┘      └─────────────────────────────┘
              │                                    │
              │                                    │
              ▼                                    ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │                     Globus Search Queries                        │
  │                                                                  │
  │  Datasets: "Find iron oxide datasets from 2024"                 │
  │  Streams:  "Show active streams from Lab 42"                    │
  └─────────────────────────────────────────────────────────────────┘
```

### Migration Strategy (Simplified)

Since we're only migrating **dataset entries** (not records), migration is straightforward:

```python
# Migration script pseudocode
def migrate_v1_to_v2():
    v1_index = "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    v2_index = "mdf-datasets-v2"

    # Query only dataset entries (not records)
    query = "mdf.resource_type:dataset"

    for entry in paginated_search(v1_index, query):
        v2_entry = {
            "mdf": {
                "source_id": entry["mdf"]["source_id"],
                "source_name": entry["mdf"]["source_name"],
                "version": str(entry["mdf"]["version"]),
                "organization": entry["mdf"].get("organizations", ["MDF Open"])[0],
                "acl": entry["mdf"]["acl"],
                "ingest_date": entry["mdf"]["ingest_date"]
            },
            "dc": {
                "title": entry["dc"]["titles"][0]["title"],
                "creators": [c.get("creatorName", c.get("familyName", ""))
                            for c in entry["dc"]["creators"]],
                "publisher": entry["dc"].get("publisher", "Materials Data Facility"),
                "year": int(entry["dc"].get("publicationYear", 2024)),
                "doi": entry["dc"].get("identifier", {}).get("identifier"),
                "description": entry["dc"].get("descriptions", [{}])[0].get("description", ""),
                "subjects": [s["subject"] for s in entry["dc"].get("subjects", [])],
                "license": "CC-BY-4.0"
            },
            "data": {
                "location": entry.get("data", {}).get("endpoint_path", ""),
                "size_bytes": entry.get("data", {}).get("total_size", 0),
                "file_count": 0  # Not tracked in v1
            }
        }

        ingest_to_index(v2_index, v2_entry)
```

**Migration volume:**
- v1 has ~1 million entries, but most are **records**
- Datasets only: ~5,000-10,000 entries
- Migration time: minutes, not hours

### Search Index Configuration (Final)

| Index | Purpose | Keys | Volume |
|-------|---------|------|--------|
| `mdf-datasets-v2` | Published datasets | ~18 | ~10K entries, growing slowly |
| `mdf-streams-v2` | Active streams | ~15 | Variable, can be large |
| `mdf` (v1 prod) | Legacy, read-only | ~120+ | ~1M entries (frozen) |

---

## Vision: What This Enables

With the fully implemented backend, MDF becomes a **living data infrastructure** rather than just a publication archive. Traditional researchers can still publish polished, DOI-minted datasets through the familiar workflow. But now, automated laboratories can stream experimental data in real-time, making it searchable within minutes. A self-driving lab running 24/7 synthesis experiments can continuously push XRD patterns, mass spec results, and synthesis parameters to MDF, where they're automatically indexed and made discoverable. Other researchers can query this data as it's generated, enabling a new paradigm of "science as it happens" rather than "science after publication." The system handles everything from a single CSV upload to petabyte-scale continuous data streams, all through unified APIs that work equally well for humans typing CLI commands, Python scripts in Jupyter notebooks, or AI agents orchestrating complex experimental workflows.

---

## Use Case Examples

### 1. Traditional Dataset Publication (Human, CLI)

A materials scientist publishes DFT calculation results with a DOI.

```bash
# Initialize dataset with metadata
mdf init ./fe-al-dft \
  --title "Fe-Al Intermetallic Formation Energies" \
  --author "Jane Doe" \
  --author "John Smith"

# Add data files with auto-extraction
mdf add calculations.csv parameters.json --discover

# Commit and validate
mdf commit -m "Initial dataset"
mdf validate

# Publish to MDF (mints DOI, indexes to Search)
mdf publish --submit

# Output:
# Published successfully!
#   Source ID: fe_al_intermetallic_v1.0
#   DOI: 10.18126/abc123
```

---

### 2. Automated Lab Streaming Data (Python SDK)

A self-driving synthesis lab continuously pushes experimental results.

```python
from mdf_agent import MDFStream

# Create a persistent stream for the lab
stream = MDFStream.create(
    title="Autonomous Perovskite Synthesis - Lab 42",
    lab_id="argonne-lab-42",
    schema={"composition": "str", "xrd_file": "path", "yield": "float"}
)

print(f"Stream created: {stream.id}")
# Stream created: stream_8f3a2b1c

# In the lab's control loop (runs 24/7)
while lab.is_running():
    sample = lab.synthesize_next()
    xrd_path = lab.measure_xrd(sample)

    # Upload XRD file directly to Globus (returns pre-signed URL)
    upload_url = stream.get_upload_url(f"xrd/{sample.id}.xy")
    requests.put(upload_url, data=open(xrd_path, 'rb'))

    # Record metadata (indexed every 15 min automatically)
    stream.append({
        "composition": sample.composition,
        "xrd_file": f"xrd/{sample.id}.xy",
        "yield": sample.yield_percent,
        "timestamp": datetime.now().isoformat()
    })

# After a month of experiments, promote to permanent dataset
stream.close(
    mint_doi=True,
    title="Perovskite Synthesis Campaign Q1 2026",
    description="1,247 samples synthesized autonomously"
)
# DOI: 10.18126/xyz789
```

---

### 3. Real-Time Experiment Monitoring (AI Agent via Claude)

A researcher asks Claude to monitor their running experiment.

```
User: I started a new synthesis run in Lab 42. Can you watch for any
      anomalies in the XRD patterns as they come in?

Claude: I'll monitor the stream for Lab 42. Let me connect to it.

        [Uses mdf_agent skill: stream.connect("argonne-lab-42")]

        Connected to stream stream_8f3a2b1c
        Current status: 47 samples, last append 3 minutes ago

        I'll check every 15 minutes when new data is indexed.
        I'll alert you if I see:
        - Unexpected phase formation
        - Peak shifts suggesting strain
        - Anomalous intensity ratios

        [30 minutes later...]

        ⚠️ Alert: Sample #52 shows an unexpected peak at 2θ=28.3°
        This could indicate BaTiO3 contamination. Here's the pattern:

        [displays XRD plot]

        Should I flag this sample for manual review?
```

---

### 4. Cross-Lab Data Discovery (Python Query)

A researcher searches across all active lab streams for specific materials.

```python
from mdf_agent import MDFSearch

# Search across both published datasets AND active streams
results = MDFSearch.query(
    "material.composition:Fe* AND measurement.type:XRD",
    include_streams=True  # Include live lab data
)

print(f"Found {len(results)} results")
# Found 2,847 results

# Filter to just streaming data from the last 24 hours
live_data = [r for r in results if r.is_stream and r.age_hours < 24]
print(f"Live data points: {len(live_data)}")
# Live data points: 156

# Download the XRD files for analysis
for record in live_data[:10]:
    record.download_file("xrd_file", dest="./analysis/")
```

---

### 5. Derived Dataset from Stream Snapshot (CLI + Python)

A researcher creates a curated subset from a live stream.

```bash
# Take a snapshot of interesting samples from an active stream
mdf stream snapshot stream_8f3a2b1c \
  --filter "yield > 85" \
  --output ./high-yield-samples

# Output:
# Snapshot created: 23 samples matching filter
# Files downloaded to ./high-yield-samples/

cd ./high-yield-samples
```

```python
# Curate and enrich the snapshot
from mdf_agent import MDFAgent

agent = MDFAgent.from_repo(".")

# Add derived analysis
agent.manifest.derived_from = [{
    "source_id": "stream_8f3a2b1c",
    "relationship": "filtered_subset",
    "description": "High-yield (>85%) perovskite samples"
}]

# Add your analysis results
agent.add("analysis/*.json", discover=True)
agent.commit("Add phase identification analysis")

# Publish as a new DOI-minted dataset
result = agent.publish()
print(f"Published: {result['doi']}")
# Published: 10.18126/derived-456
```

---

## Problem Statement

Design a new backend for MDF with these constraints:
- **Volume**: ~10 datasets/month (very low)
- **Data size**: TB-scale transfers, can take 24+ hours
- **New requirement**: Streaming datasets for automated labs
- **Budget**: Free or nearly free

## Existing Infrastructure

- **Current backend**: AWS Lambda + DynamoDB (working but poorly documented)
- **Storage**: 2 Globus endpoints, ~1PB of published data
- **Access**: Globus Auth (institutional credentials) + Globus Transfer or HTTPS PUT/GET
- **Pain point**: Previous developer left, documentation lacking

## Two Distinct Workflows

| Aspect | Legacy Publishing | Ephemeral/Streaming |
|--------|------------------|---------------------|
| **Use case** | Traditional dataset publication | Automated labs, continuous data |
| **Frequency** | ~10/month | Potentially thousands of appends/day |
| **Data lifecycle** | Permanent, versioned, DOI | May be temporary, rolling window |
| **Trigger** | Human initiates `mdf publish` | Machine continuously appends |
| **Metadata** | Rich DataCite, curated | Minimal, auto-generated |
| **Indexing** | Full extraction + Search | Lightweight, incremental |

---

## Recommended Approach: Improve Existing AWS + Add Streaming Layer

Given you already have working AWS Lambda + DynamoDB infrastructure, the pragmatic approach is:

1. **Document and refactor** the existing Lambda backend (legacy publishing)
2. **Add a new streaming layer** for ephemeral datasets (can be same or different infra)
3. **Unify the API surface** so `mdf_agent` talks to one endpoint

```
                          ┌─────────────────────────────────────────────────────┐
                          │                  API Gateway                         │
                          │         api.materialsdatafacility.org               │
                          └──────────────────────┬──────────────────────────────┘
                                                 │
                    ┌────────────────────────────┼────────────────────────────┐
                    │                            │                            │
                    ▼                            ▼                            ▼
         ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
         │  Legacy Publishing  │    │  Streaming Ingest   │    │   Status/Query      │
         │  (existing Lambda)  │    │   (new service)     │    │   (shared)          │
         │                     │    │                     │    │                     │
         │  POST /submit       │    │  POST /stream/new   │    │  GET /status/:id    │
         │  POST /update       │    │  POST /stream/:id   │    │  GET /datasets      │
         │  POST /curate       │    │  POST /stream/close │    │  GET /search        │
         └──────────┬──────────┘    └──────────┬──────────┘    └──────────┬──────────┘
                    │                          │                          │
                    ▼                          ▼                          ▼
         ┌─────────────────────────────────────────────────────────────────────────┐
         │                            DynamoDB                                      │
         │  submissions | streams | status_log | stream_files                      │
         └─────────────────────────────────────────────────────────────────────────┘
                                                 │
                    ┌────────────────────────────┼────────────────────────────┐
                    ▼                            ▼                            ▼
         ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
         │  Globus Endpoints   │    │   Globus Search     │    │     DataCite        │
         │  (2 endpoints, 1PB) │    │   (indexing)        │    │   (DOI minting)     │
         └─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

---

## Workflow 1: Legacy Publishing (Refactor Existing)

**Goal**: Clean up and document the existing AWS Lambda backend.

### Current Flow (to preserve)
```
1. Client POSTs to /submit with DataCite metadata + data_sources
2. Lambda validates payload, creates submission record in DynamoDB
3. Lambda initiates Globus Transfer if needed
4. Step Functions or EventBridge polls transfer status
5. On completion: extract metadata, index to Globus Search, mint DOI
6. Client polls /status/:id for progress
```

### Improvements Needed
- [ ] Document the existing Lambda functions
- [ ] Add comprehensive logging
- [ ] Improve error messages
- [ ] Add unit tests
- [ ] Create local development setup

---

## Workflow 2: Streaming/Ephemeral Datasets (New)

**Goal**: Support automated labs that generate data continuously.

### API Design
```
POST /stream/create
  → Create stream with minimal metadata (title, lab_id, schema)
  → Returns stream_id

POST /stream/:id/append
  → Append file(s) to stream
  → Can include inline metadata per file
  → Data written directly to Globus endpoint via HTTPS PUT

POST /stream/:id/snapshot
  → Create searchable checkpoint of current data
  → Indexes accumulated files to Globus Search
  → Optional: triggers partial processing

POST /stream/:id/close
  → Finalize stream
  → Option A: Convert to permanent dataset (mint DOI)
  → Option B: Mark as ephemeral (auto-delete after N days)

GET /stream/:id/status
  → Current file count, bytes, last append time
```

### Data Storage Strategy
```
Globus Endpoint (ephemeral zone)
└── streams/
    └── {stream_id}/
        ├── manifest.json      # Stream metadata
        ├── 2024-01-15/
        │   ├── 001.csv
        │   ├── 002.csv
        │   └── ...
        └── 2024-01-16/
            └── ...
```

- **Hot data**: Recent files on fast storage
- **Cold data**: Older files can be moved to cheaper storage
- **Cleanup**: Manual deletion by lab owners (no auto-TTL)

### Indexing Strategy ✓ DECIDED

**Choice: Every 15 minutes via cron**

| Trigger | Action |
|---------|--------|
| Every append | Update file count in DynamoDB only |
| **Every 15 min (cron)** | Batch index new files to Globus Search |
| On snapshot | Full re-index of stream (manual trigger) |
| On close | Final index + DOI mint (if permanent) |

### Data Lifecycle ✓ DECIDED

**Choice: No auto-delete (manual cleanup)**

- Streams persist until explicitly deleted
- Labs responsible for managing their storage
- Future: Add optional quotas per lab if needed

---

## Key Technical Decisions

### 1. Data Path for Streaming ✓ DECIDED

**Choice: HTTPS PUT directly to Globus endpoint**

```
1. Lab calls POST /stream/:id/append with file metadata
2. Backend returns pre-signed HTTPS URL for Globus endpoint
3. Lab PUTs file directly to Globus (no Lambda in data path)
4. Lab confirms upload complete, backend records in DynamoDB
```

Benefits:
- No 6MB Lambda payload limit
- Lower latency for uploads
- Backend only handles metadata, not data

### 2. Metadata Extraction

| Data Type | Strategy |
|-----------|----------|
| Local files | Client-side extraction via `mdf_agent` |
| Globus-only files | Defer to indexing phase, or skip |
| Streaming files | Minimal extraction, schema-based |

### 3. DynamoDB Schema Additions

```
# Existing tables (preserve)
submissions, status_log, ...

# New tables for streaming
streams
  PK: stream_id
  lab_id, title, schema, status (open|closed|expired)
  created_at, last_append_at, file_count, total_bytes
  ttl_days, close_action (permanent|ephemeral)

stream_files
  PK: stream_id, SK: file_path
  uploaded_at, size_bytes, metadata (JSON)
  indexed: boolean
```

---

## API Endpoints (Unified Surface)

### Legacy Publishing (existing, keep compatible)
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/submit` | POST | Accept dataset submission |
| `/status/:source_id` | GET | Check processing status |
| `/submissions` | POST | List/filter submissions |
| `/update/:source_id` | POST | Metadata-only update |
| `/curate/:source_id` | GET/POST | Curation workflow |

### Streaming (new)
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/stream/create` | POST | Create new stream |
| `/stream/:id/append` | POST | Add files to stream |
| `/stream/:id/snapshot` | POST | Index current state |
| `/stream/:id/close` | POST | Finalize stream |
| `/stream/:id` | GET | Stream status |

---

## Implementation Phases

### Phase 0: Understand Existing (Week 1)
- [ ] Get access to current Lambda source code
- [ ] Document existing Lambda functions and DynamoDB schema
- [ ] Identify pain points and technical debt
- [ ] Set up local development environment

### Phase 1: Stabilize Legacy (Week 2-3)
- [ ] Add logging and error handling
- [ ] Write tests for existing endpoints
- [ ] Fix known issues
- [ ] Improve deployment process

### Phase 2: Add Streaming Layer (Week 3-5)
- [ ] Design DynamoDB tables for streams
- [ ] Implement `/stream/create` and `/stream/:id` endpoints
- [ ] Implement `/stream/:id/append` with direct Globus upload
- [ ] Add EventBridge cron for periodic indexing

### Phase 3: Integrate Streaming with Search (Week 5-6)
- [ ] Implement `/stream/:id/snapshot` for manual indexing
- [ ] Implement `/stream/:id/close` with DOI option
- [ ] Add TTL-based cleanup for ephemeral streams

### Phase 4: Client Integration (Week 6-7)
- [ ] Add `mdf stream` commands to `mdf_agent` CLI
- [ ] Add streaming skill handlers for Claude
- [ ] Documentation and examples

---

## Remaining Questions

1. **Existing backend access**: Can you share the current Lambda source code so I can understand the architecture before refactoring?

2. **Globus endpoint structure**: How are the 2 endpoints organized?
   - One for legacy published data, one for streaming?
   - Or partitioned differently?

3. **HTTPS PUT capability**: Does the Globus endpoint support pre-signed URLs for direct PUT? Or do we need a proxy layer?

---

## Files to Create/Modify

### Existing Backend (refactor)
```
mdf-connect-backend/          # Current Lambda code
├── README.md                 # NEW: Document architecture
├── docs/
│   ├── api.md               # NEW: API documentation
│   └── dynamo-schema.md     # NEW: DynamoDB schema
├── tests/                    # NEW: Test coverage
└── ...
```

### Streaming Extension
```
mdf-connect-backend/
├── streaming/
│   ├── create.py            # POST /stream/create
│   ├── append.py            # POST /stream/:id/append
│   ├── snapshot.py          # POST /stream/:id/snapshot
│   ├── close.py             # POST /stream/:id/close
│   └── status.py            # GET /stream/:id
├── cron/
│   └── index_streams.py     # Periodic indexing job
└── shared/
    ├── globus_client.py     # Shared Globus utilities
    └── search_client.py     # Globus Search utilities
```

### Client Updates
```
mdf_client/src/mdf_agent/
├── cli/
│   └── stream.py            # NEW: mdf stream commands
├── core/
│   └── streaming.py         # NEW: Streaming API client
└── skill/
    └── handlers.py          # UPDATE: Add streaming handlers
```

---

## Verification Plan

1. **Legacy regression**: Existing `mdf publish` workflow still works
2. **Stream lifecycle**: Create → append → snapshot → close flow
3. **Indexing latency**: Measure time from append to searchable
4. **Ephemeral cleanup**: Verify TTL-based deletion works
5. **Scale test**: Simulate 1000 appends/hour for 24 hours

---

## Legacy Code Review: connect_server (DEPRECATED)

> **NOTE:** The `connect_server` repo is the **old system** and should be **archived**. The production system is in the `cs` repo. This section documents issues found during review - these do NOT need to be fixed, as this code should be retired.

After reviewing the `connect_server` codebase at `/Users/ben/Desktop/git/connect_server`, the following issues were found. **These are documented for historical reference only** - the recommendation is to archive this repo rather than fix it.

### Critical Issues (Historical - Do Not Fix, Archive Instead)

#### 1. SQS Message Loss - Race Condition
**File**: `mdf_connect_server/processor/processor.py:54-116`

Messages are deleted from SQS immediately after receipt, before processing completes:

```python
# Current (dangerous)
submissions = utils.retrieve_from_queue(wait_time=20)
for sub in submissions["entries"]:
    driver = multiprocessing.Process(target=submission_driver, kwargs=sub)
    driver.start()
    active_processes.append(driver)
utils.delete_from_queue(submissions["delete_info"])  # DELETED BEFORE PROCESSING DONE
sleep(40)
```

**Impact**: If the processor crashes during a long-running submission, the message is lost forever.

**Fix**: Delete messages only after `submission_driver` completes successfully.

#### 2. DOI Collision - Race Condition
**File**: `mdf_connect_server/utils/utils.py:1111-1131`

DOI generation uses a check-then-mint pattern without locks:

```python
# Pseudocode of current flow
existing = check_if_doi_exists(source_id)
if not existing:
    doi = mint_doi(source_id)  # Another process could mint between check and mint
```

**Impact**: Two concurrent submissions could generate the same DOI.

**Fix**: Use atomic conditional writes in DynamoDB, or generate DOI first and handle conflicts.

#### 3. Missing VisibilityTimeout on SQS
**File**: SQS configuration

The FIFO queue uses the default 30-second visibility timeout, but processing can take hours (TB-scale transfers).

**Impact**: A message becomes visible again while still being processed, causing duplicate processing.

**Fix**: Set `VisibilityTimeout` to match maximum expected processing time, or extend it dynamically.

#### 4. ACL Cleanup Failures Leave Security Holes
**File**: `mdf_connect_server/utils/utils.py` (backup_data function)

Write permissions on Globus endpoints may not be revoked if cleanup fails.

**Impact**: Leftover write ACLs could allow unauthorized data modification.

**Fix**: Track ACL grants in database and add a cleanup cron job.

### High Priority Issues

#### 5. 123 Bare Exception Blocks
**Files**: Throughout codebase

Pattern `except:` or `except Exception:` with no logging or re-raise.

```python
try:
    critical_operation()
except:
    pass  # Silent failure - no logging, no recovery
```

**Impact**: Failures are silently swallowed, making debugging impossible.

**Fix**: Add structured logging to all exception handlers, re-raise where appropriate.

#### 6. No Retry Logic for External Services
**Files**: DataCite API calls, Globus API calls

External service calls have no timeout, retry, or circuit breaker.

**Impact**: A temporary DataCite outage causes permanent submission failure.

**Fix**: Add `tenacity` or similar retry library with exponential backoff.

#### 7. DynamoDB Overwrites Without Conditional Checks
**File**: `mdf_connect_server/utils/utils.py:1812-1919` (update_status function)

Full `put_item` calls without conditional expressions.

**Impact**: Concurrent updates can overwrite each other, losing data.

**Fix**: Use conditional writes or atomic update expressions.

### Medium Priority Issues

#### 8. Credentials in Error Logs
**Files**: API error handlers

Error messages may include tokens or credentials.

**Impact**: Security risk if logs are exposed.

**Fix**: Sanitize error messages before logging.

#### 9. Multiprocessing Without Resource Management
**File**: `mdf_connect_server/processor/processor.py`

Worker processes are spawned without limits.

**Impact**: Could exhaust system resources under load.

**Fix**: Use a process pool with bounded concurrency.

#### 10. 24 TODO Comments
**Files**: Throughout codebase

Unfinished work scattered throughout the code.

**Impact**: Potential bugs or missing functionality.

**Fix**: Triage and address or document as known limitations.

#### 11. Incomplete Supabase Migration
**Files**: Various

Code references Supabase but migration appears incomplete.

**Impact**: Confusion about data source of truth.

**Fix**: Complete migration or revert to single database.

### Legacy Architecture Diagram (For Reference Only)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     LEGACY Data Flow (connect_server - DO NOT USE)               │
└─────────────────────────────────────────────────────────────────────────────────┘

  Client                  API (Flask)              SQS FIFO              Processor
    │                         │                       │                      │
    │  POST /submit           │                       │                      │
    │────────────────────────>│                       │                      │
    │                         │  validate + create    │                      │
    │                         │  submission record    │                      │
    │                         │  in DynamoDB          │                      │
    │                         │                       │                      │
    │                         │  send_to_queue()      │                      │
    │                         │──────────────────────>│                      │
    │                         │                       │                      │
    │  {"source_id": "..."}   │                       │                      │
    │<────────────────────────│                       │                      │
    │                         │                       │                      │
    │                         │                       │  retrieve_from_queue │
    │                         │                       │<─────────────────────│
    │                         │                       │                      │
    │                         │                       │  delete IMMEDIATELY  │  ⚠️ BUG
    │                         │                       │<─────────────────────│
    │                         │                       │                      │
    │                         │              ┌────────┴───────────────────────┘
    │                         │              │
    │                         │              ▼  submission_driver() [hours]
    │                         │        ┌─────────────┐
    │                         │        │ 1. download │ Globus Transfer
    │                         │        │ 2. backup   │ Copy to destination
    │                         │        │ 3. extract  │ Parse metadata
    │                         │        │ 4. index    │ Globus Search
    │                         │        │ 5. mint DOI │ DataCite API
    │                         │        │ 6. cleanup  │ Remove ACLs
    │                         │        └─────────────┘
```

> **Recommendation:** Archive this repo. The `cs` repo with Lambda + Globus Flows is the correct modern architecture.

---

## Final Implementation Plan

### Key Principle: Parallel Infrastructure

**The new system MUST run on completely separate infrastructure.** The current production system continues operating unchanged until we explicitly flip the switch. This means:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         PARALLEL DEPLOYMENT STRATEGY                             │
└─────────────────────────────────────────────────────────────────────────────────┘

  CURRENT (keep running)                    NEW (build separately)
  ──────────────────────                    ──────────────────────

  api.materialsdatafacility.org             api-v2.materialsdatafacility.org
           │                                          │
           ▼                                          ▼
  ┌─────────────────┐                       ┌─────────────────┐
  │ MDF-Connect2-*  │                       │ MDF-Connect3-*  │
  │ Lambda funcs    │                       │ Lambda funcs    │
  └────────┬────────┘                       └────────┬────────┘
           │                                          │
           ▼                                          ▼
  ┌─────────────────┐                       ┌─────────────────┐
  │ prod-status-*   │                       │ v2-submissions  │
  │ DynamoDB tables │                       │ v2-streams      │
  └─────────────────┘                       │ DynamoDB tables │
                                            └─────────────────┘
           │                                          │
           ▼                                          ▼
  ┌─────────────────┐                       ┌─────────────────┐
  │ Flow: 4c37a999  │                       │ Flow: (new ID)  │
  │ (prod)          │                       │ mdf-flow-v2     │
  └─────────────────┘                       └─────────────────┘
           │                                          │
           └──────────────┬───────────────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Globus Search   │  ◄── Same indexes (backwards compatible)
                 │ (shared)        │
                 └─────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Globus Endpoints│  ◄── Same storage (data doesn't move)
                 │ (shared)        │
                 └─────────────────┘
```

**Shared resources** (no duplication needed):
- Globus Search indexes (same schema)
- Globus storage endpoints (same data locations)
- DataCite credentials (same DOI prefix)
- AWS Secrets Manager (add v2 secrets alongside existing)

**Separate resources** (new instances):
- API Gateway (new domain: `api-v2.materialsdatafacility.org`)
- Lambda functions (new names: `MDF-Connect3-*`)
- DynamoDB tables (new names: `mdf-connect-v2-*`)
- Globus Flow (new flow ID, new definition)
- Terraform state (separate state file)

**Cutover strategy:**
1. Build and test v2 completely on separate infra
2. Run v2 in shadow mode (process test submissions)
3. Validate v2 produces identical Search entries
4. DNS cutover: point `api.materialsdatafacility.org` → v2
5. Keep v1 running (read-only) for 30 days
6. Decommission v1 after validation period

---

### Phase 0: Setup Parallel Infrastructure (Week 1-2)

- [ ] Create new Terraform workspace: `infra/mdf-v2/`
- [ ] Define new resource names (MDF-Connect3-*, mdf-connect-v2-*)
- [ ] Create new API Gateway with temporary domain (`api-v2.mdf...`)
- [ ] Create new DynamoDB tables with proper schema (see Phase 1 design)
- [ ] Create new Globus Flow (fork existing, new flow_id)
- [ ] Set up CI/CD for v2 (separate GitHub Actions workflow)
- [ ] Archive `connect_server` repo (rename to `mdf-connect-legacy`)
- [ ] Document v1 → v2 mapping for reference

### Phase 1: Core Lambda Functions (Week 3-4)

Build simplified Lambdas from scratch (don't copy old code):

- [ ] `auth.py` - Globus Auth validation (minimal changes)
- [ ] `submit.py` - Validate + trigger Flow (simplified)
- [ ] `status.py` - Query status from new DynamoDB
- [ ] `submissions.py` - List user submissions

**Simplifications:**
- Remove SourceIDManager complexity (use UUID + optional user-provided name)
- Remove 12-step status (use 5 states: submitted → transferring → processing → indexing → complete/failed)
- Remove organization complexity (single config, not per-org)
- Remove curation workflow (add back later if needed)

### Phase 2: Simplified Globus Flow (Week 5-6)

Create new flow definition (`mdf_flow_v2_def.json`):

```
StartSubmission
    │
    ▼
ValidateInputs (ExpressionEval - check required fields)
    │
    ▼
UserTransfer (Globus Transfer action)
    │
    ├─── FAIL ──> UpdateStatusFailed ──> NotifyFailure ──> End
    │
    ▼ SUCCESS
UpdateStatusProcessing (Lambda action - update DynamoDB)
    │
    ▼
SearchIngest (SIAP action)
    │
    ├─── FAIL ──> UpdateStatusFailed ──> NotifyFailure ──> End
    │
    ▼ SUCCESS
UpdateStatusComplete (Lambda action - update DynamoDB)
    │
    ▼
NotifySuccess (Email action)
    │
    ▼
End
```

- [ ] Write new flow definition (target: <100 lines JSON)
- [ ] Remove Xtraction state (client-side extraction only)
- [ ] Remove Curation states (optional future module)
- [ ] Remove Citrine/MRR states (dead code)
- [ ] Remove multiple destination transfers
- [ ] Add status update Lambda actions (Flow → DynamoDB)
- [ ] Deploy to new flow_id
- [ ] Test with sample submissions

### Phase 3: Streaming Layer (Week 7-9)

New Lambda functions for streaming:

- [ ] `stream_create.py` - POST /stream/create
- [ ] `stream_append.py` - POST /stream/:id/append
- [ ] `stream_status.py` - GET /stream/:id
- [ ] `stream_close.py` - POST /stream/:id/close

New DynamoDB table: `mdf-connect-v2-streams`

- [ ] Implement file metadata tracking (no actual file storage - just references)
- [ ] Implement 15-minute batch indexing (EventBridge → Lambda → SIAP)
- [ ] Implement stream → dataset promotion (close with DOI minting)
- [ ] Add rate limiting (prevent abuse)

### Phase 4: Client Integration (Week 10-11)

Update `mdf_agent` to support both v1 and v2:

```python
# mdf_agent/core/client.py
class MDFClient:
    def __init__(self, api_version="v1"):
        if api_version == "v2":
            self.base_url = "https://api-v2.materialsdatafacility.org"
        else:
            self.base_url = "https://api.materialsdatafacility.org"
```

- [ ] Add `--api-version` flag to CLI
- [ ] Implement `mdf stream create/append/close` commands
- [ ] Add streaming support to Python SDK
- [ ] Update AI agent skill handlers
- [ ] Write migration guide for existing users

### Phase 5: Validation & Cutover (Week 12)

- [ ] Run v2 in shadow mode (duplicate submissions to both systems)
- [ ] Compare Search entries: v1 vs v2 (must be identical)
- [ ] Load test streaming endpoint (1000 appends/hour)
- [ ] Security review (auth, rate limiting, input validation)
- [ ] DNS cutover plan (with rollback procedure)
- [ ] Execute cutover during low-traffic window
- [ ] Monitor for 48 hours
- [ ] Begin 30-day parallel operation period

### Phase 6: Decommission v1 (Week 16)

- [ ] Verify no traffic to v1 API
- [ ] Export v1 DynamoDB data (archive)
- [ ] Delete v1 Lambda functions
- [ ] Delete v1 DynamoDB tables
- [ ] Delete v1 API Gateway
- [ ] Keep v1 Globus Flow (read-only, for historical run inspection)
- [ ] Update documentation to remove v1 references
- [ ] Celebrate 🎉
