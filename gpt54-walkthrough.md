# GPT-5.4 Walkthrough: End-to-End Demo of the New CLI Workflow

This walkthrough is optimized for copy/paste testing.

It shows:

- `mdf setup`
- manifest creation and inspection
- preview-safe metadata discovery
- validation and submit-time preflight
- publish with the new post-submit status flow
- curator review, rejection feedback, edit, resubmit, approval
- search, show, cite, preview, list, versions

## Terminal A: start a local backend

Run this in a separate terminal and leave it running.

```bash
cd /Users/ben/Desktop/git/mdf_client/cs/aws

STORE_BACKEND=sqlite \
SQLITE_PATH=/tmp/mdf-gpt54.db \
ASYNC_DISPATCH_MODE=inline \
STORAGE_BACKEND=local \
FILE_STORE_PATH=/tmp/mdf-gpt54-files \
AUTH_MODE=dev \
ALLOW_ALL_CURATORS=true \
USE_MOCK_DATACITE=true \
USE_MOCK_SEARCH=true \
python -m v2.app.main
```

You should end up with a server on `http://127.0.0.1:8080`.

## Terminal B: install the client and create demo data

```bash
cd /Users/ben/Desktop/git/mdf_client
python -m pip install -e ".[extractors]"

rm -rf /tmp/mdf-gpt54-demo
mkdir -p /tmp/mdf-gpt54-demo/raw /tmp/mdf-gpt54-demo/update

cat > /tmp/mdf-gpt54-demo/raw/measurements.csv <<'EOF'
sample_id,temperature_c,intensity
A1,25,10.2
A1,50,12.9
A1,75,15.1
EOF

cat > /tmp/mdf-gpt54-demo/raw/notes.json <<'EOF'
{
  "instrument": "XRD-9000",
  "operator": "GPT54 Demo User",
  "campaign": "gpt54 walkthrough"
}
EOF

cat > /tmp/mdf-gpt54-demo/README.txt <<'EOF'
Demo dataset for the new MDF Agent workflow.
EOF
```

## 1. First-run experience and setup

```bash
mdf
```

Now run the new setup flow:

```bash
printf 'local\nGPT54 Demo Lab\nGPT54 Demo Publisher\n\nn\n' | mdf setup /tmp/mdf-gpt54-demo
```

What this does:

- sets the default service to `local`
- stores organization and publisher defaults
- skips manifest creation so you can test that separately

Check the resulting config:

```bash
mdf config show
mdf doctor --service local
```

## 2. Create and inspect a manifest

```bash
cd /tmp/mdf-gpt54-demo

mdf manifest init . \
  --title "GPT54 XRD Demo Dataset" \
  --author "GPT54 Demo User" \
  --description "Copy-paste walkthrough dataset for the new MDF Agent UX" \
  --organization "GPT54 Demo Lab" \
  --keyword "gpt54" \
  --keyword "walkthrough"

mdf manifest inspect
```

## 3. Preview metadata discovery before writing

```bash
mdf manifest discover --preview raw/measurements.csv raw/notes.json
```

If the preview looks good, write it into `mdf.yaml`:

```bash
mdf manifest discover raw/measurements.csv raw/notes.json
mdf manifest inspect
```

## 4. Validate and run the new preflight flow

```bash
mdf validate --service local
mdf publish --preflight-only --service local
```

Show the full dry-run payload too:

```bash
mdf publish --service local
```

## 5. Submit the dataset

This now records the dataset, checks status immediately, and exits cleanly once the submission is confirmed in `pending_curation`.

```bash
mdf publish --submit --service local
```

Capture the source ID for the rest of the demo:

```bash
SOURCE_ID=$(mdf status --json --service local | python -c 'import json,sys; print(json.load(sys.stdin)["submission"]["source_id"])')
echo "$SOURCE_ID"
```

Inspect the newly submitted record:

```bash
mdf status "$SOURCE_ID" --service local
mdf pending --service local
mdf list --service local
```

## 6. Curator review: reject once to show the new feedback flow

Reject the pending submission with a reason and suggestion:

```bash
mdf reject "$SOURCE_ID" \
  --reason "Methods section is too short" \
  --suggestions "Add one sentence explaining the temperature sweep" \
  --yes \
  --service local
```

The improved `status` output should now surface that feedback:

```bash
mdf status "$SOURCE_ID" --service local
```

## 7. Edit and resubmit

```bash
mdf edit "$SOURCE_ID" \
  --description "Copy-paste walkthrough dataset for the new MDF Agent UX. This run includes a temperature sweep from 25C to 75C." \
  --service local

mdf resubmit "$SOURCE_ID" \
  --notes "Expanded the methods text and clarified the temperature sweep." \
  --service local

mdf status "$SOURCE_ID" --service local
```

## 8. Approve and watch to publication

```bash
mdf approve "$SOURCE_ID" --yes --service local
mdf watch "$SOURCE_ID" --service local --interval 1 --timeout 30
```

Once published, exercise the discovery/read-only commands:

```bash
mdf show "$SOURCE_ID" --cite --service local
mdf preview "$SOURCE_ID" --service local
mdf cite "$SOURCE_ID" --format bibtex --service local
mdf open "$SOURCE_ID" --url --service local
mdf versions "$SOURCE_ID" --service local
mdf list --latest-only --service local
mdf search "gpt54" --service local
```

## 9. Show the update workflow and update-time preflight

Create new data for a follow-up version:

```bash
cat > /tmp/mdf-gpt54-demo/update/new_measurements.csv <<'EOF'
sample_id,temperature_c,intensity
A1,100,17.8
A1,125,18.9
EOF
```

Run the update preflight first:

```bash
mdf update "$SOURCE_ID" \
  --data /tmp/mdf-gpt54-demo/update \
  --preflight-only \
  --service local
```

Submit the update:

```bash
mdf update "$SOURCE_ID" \
  --data /tmp/mdf-gpt54-demo/update \
  --submit \
  --service local
```

Approve the new version and watch it complete:

```bash
mdf approve "$SOURCE_ID" --yes --service local
mdf watch "$SOURCE_ID" --service local --interval 1 --timeout 30
mdf versions "$SOURCE_ID" --service local
```

## 10. Optional: demonstrate DOI-style input

If `mdf show` printed a DOI, you can test DOI resolution directly. Replace the value below with the DOI shown by your local run:

```bash
mdf show 10.0000/example-doi --service local
mdf open 10.0000/example-doi --url --service local
mdf cite 10.0000/example-doi --service local
```

## Fast recap command list

If you want the shortest possible test sequence:

```bash
cd /Users/ben/Desktop/git/mdf_client
python -m pip install -e ".[extractors]"
printf 'local\nGPT54 Demo Lab\nGPT54 Demo Publisher\n\nn\n' | mdf setup /tmp/mdf-gpt54-demo
cd /tmp/mdf-gpt54-demo
mdf manifest init . --title "GPT54 XRD Demo Dataset" --author "GPT54 Demo User" --description "Walkthrough dataset"
mdf manifest inspect
mdf publish --preflight-only --service local
mdf publish --submit --service local
SOURCE_ID=$(mdf status --json --service local | python -c 'import json,sys; print(json.load(sys.stdin)["submission"]["source_id"])')
mdf reject "$SOURCE_ID" --reason "Methods section is too short" --suggestions "Add one sentence explaining the temperature sweep" --yes --service local
mdf status "$SOURCE_ID" --service local
mdf edit "$SOURCE_ID" --description "Expanded methods text for the walkthrough dataset." --service local
mdf resubmit "$SOURCE_ID" --notes "Expanded methods text." --service local
mdf approve "$SOURCE_ID" --yes --service local
mdf watch "$SOURCE_ID" --service local --interval 1 --timeout 30
mdf show "$SOURCE_ID" --cite --service local
mdf search "gpt54" --service local
```
