# MDF Quick Start: Publish & Approve

## Publish a dataset

```bash
# 1. Log in
mdf login

# 2. Publish a local directory
mdf publish ./my_data/ \
  --title "My Dataset" \
  --author "Jane Doe" \
  --submit
```

Files upload with a progress bar, then the dataset enters the curation queue.

## Check status

```bash
mdf status
```

## Approve the dataset (curator)

```bash
# See what's waiting
mdf pending

# Approve it
mdf approve my_dataset_v1
```

After approval the backend mints a DOI and indexes the dataset for search.

## Find it

```bash
mdf list                    # your datasets
mdf show my_dataset_v1      # dataset card with DOI
mdf search "my dataset"     # full-text search
```

## Update it later

### Add new data (major version bump)

```bash
mdf update <source_id> --data ./new_data/ --submit
```

New data → **major** version bump (1.0 → 2.0). The new data_sources replace the prior version's.

### Update metadata only (minor version bump)

```bash
mdf update <source_id> --title "Better Title" --submit
```

No `--data` → **minor** version bump (2.0 → 2.1). The prior version's `data_sources` are inherited automatically.

### Version chain example

```
1.0  →  2.0  →  2.1  →  3.0
 ↑       ↑       ↑       ↑
init   new data  metadata  new data
                 only
```

### Check versions

```bash
mdf versions <source_id>
```
