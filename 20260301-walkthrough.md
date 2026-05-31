# MDF Agent CLI Walkthrough

## First Run

```bash
# Bare invocation — shows welcome panel on first run
mdf
```

```
MDF Agent v0.2.0

Get started:
  mdf init                  Create a dataset manifest
  mdf publish --submit      Publish to MDF Connect
  mdf search "perovskite"   Find datasets
  mdf login                 Authenticate with Globus

Run mdf --help for all commands.
```

## Setup & Auth

```bash
# Install shell completion (bash/zsh/fish)
mdf --install-completion

# Authenticate with Globus
mdf login

# Check who you're logged in as
mdf whoami

# JSON output for scripting
mdf whoami --json

# Point at a specific service
mdf login --service staging

# Run diagnostics
mdf doctor
```

```
MDF Agent v0.2.0

  *  Config         ~/.config/mdf_agent/config.json
  *  Auth           Logged in (token cached)
  *  Service        staging (hjccjf3eqg...)
  *  Connectivity   Backend healthy (142ms)
  -  Manifest       No mdf.yaml in current directory
```

```bash
# CI-friendly diagnostics
mdf doctor --json | jq .checks
```

## Creating a Dataset Manifest

```bash
# Interactive — prompts for title, authors, description
mdf init

# Non-interactive
mdf init --title "High-Entropy Alloy Diffusion Coefficients" \
         --author "Ben Blaiszik" \
         --author "Logan Ward" \
         -d "Interdiffusion coefficients for CoCrFeMnNi HEA system measured via EPMA"

# In a subdirectory
mdf init ./hea_diffusion_data --title "HEA Diffusion Dataset" --author "Ben Blaiszik"

# Discover metadata from files
mdf manifest discover *.csv data/*.json
```

## Validation

```bash
# Validate the manifest in current directory
mdf validate

# Validate specific data paths
mdf validate ./xrd_patterns/ ./metadata.json

# JSON output for CI pipelines
mdf validate --json
```

## Publishing

```bash
# Dry run — preview the payload without submitting
mdf publish

# Direct mode — no mdf.yaml needed
mdf publish ./perovskite_xrd/ \
  --title "Perovskite XRD Patterns for ABX3 Compositions" \
  --author "Ben Blaiszik" \
  --submit

# Manifest mode — uses mdf.yaml in current directory
mdf publish --submit

# Override manifest fields at publish time
mdf publish --title "Updated: Perovskite XRD v2" --submit

# JSON output for dry run
mdf publish --json

# Publish to a specific service
mdf publish --submit --service staging
```

## Watching a Submission

```bash
# Publish and watch in one shot
mdf publish --submit && mdf watch

# Watch a specific dataset
mdf watch li_ion_cathode_stability_v1

# Custom poll interval (5 seconds) and timeout (1 hour)
mdf watch --interval 5 --timeout 3600

# JSON streaming — one line per poll, useful for logging
mdf watch li_ion_cathode_stability_v1 --json
```

```
  ~ pending_curation  (0s)
  ~ pending_curation  (10s)
  > approved  (20s)
  * published  (30s)

  DOI: https://doi.org/10.18126/xxxx

Done!
```

## Checking Status

```bash
# Status of last published dataset
mdf status

# Status of a specific dataset
mdf status perovskite_xrd_abx3_v1

# JSON for scripting
mdf status perovskite_xrd_abx3_v1 --json | jq .submission.status
```

## Listing Your Datasets

```bash
mdf list
mdf list --limit 10
mdf list --json
```

## Searching

```bash
mdf search "perovskite"
mdf search "lithium ion battery cathode" --limit 5
mdf search "XRD copper oxide" --type datasets
mdf search "in-situ TEM" --type streams
mdf search "machine learning interatomic potential" --json
```

## Dataset Details

```bash
# Full detail view
mdf show perovskite_xrd_abx3_v1

# With citation included
mdf show perovskite_xrd_abx3_v1 --cite

# JSON output
mdf show perovskite_xrd_abx3_v1 --json

# Version history
mdf versions perovskite_xrd_abx3_v1
```

## Citations

```bash
# APA (default)
mdf cite perovskite_xrd_abx3_v1

# BibTeX for LaTeX papers
mdf cite perovskite_xrd_abx3_v1 -f bibtex

# RIS for Zotero/Mendeley
mdf cite perovskite_xrd_abx3_v1 -f ris

# DataCite XML
mdf cite perovskite_xrd_abx3_v1 -f datacite

# Copy to clipboard
mdf cite perovskite_xrd_abx3_v1 --copy

# Specific version
mdf cite perovskite_xrd_abx3_v1 --version 2.0

# JSON envelope
mdf cite perovskite_xrd_abx3_v1 --json
```

## Opening in Browser

```bash
# Opens DOI link (or MDF portal fallback) in default browser
mdf open perovskite_xrd_abx3_v1

# Just print the URL
mdf open perovskite_xrd_abx3_v1 --url
```

## Previewing Data

```bash
# File listing (name, size, type)
mdf preview perovskite_xrd_abx3_v1

# Tabular data sample
mdf preview perovskite_xrd_abx3_v1 --sample

# JSON for programmatic access
mdf preview perovskite_xrd_abx3_v1 --json
```

## Cloning / Downloading

```bash
# Download to current directory (auto-selects fastest method)
mdf clone hea_diffusion_coefficients_v1

# Download to a specific directory
mdf clone hea_diffusion_coefficients_v1 ./local_hea_data

# Use Globus Transfer for large datasets
mdf clone hea_diffusion_coefficients_v1 --transfer

# Clone and set up a derived dataset
mdf clone hea_diffusion_coefficients_v1 ./derived_analysis --derive

# JSON output
mdf clone hea_diffusion_coefficients_v1 --json
```

## Updating a Dataset

```bash
# Update last published dataset with new data
mdf update --data ./updated_xrd_patterns/ --submit

# Update a specific dataset
mdf update perovskite_xrd_abx3_v1 \
  --title "Perovskite XRD Patterns — Extended Composition Range" \
  --data ./extended_data/ \
  --submit

# Dry run first
mdf update perovskite_xrd_abx3_v1 --data ./new_data/
mdf update perovskite_xrd_abx3_v1 --data ./new_data/ --json
```

## Curation (Curators Only)

```bash
# See what's waiting for review
mdf pending
mdf pending --organization argonne
mdf pending --json

# Approve a dataset
mdf approve li_ion_cathode_stability_v1
mdf approve li_ion_cathode_stability_v1 --notes "Excellent metadata, LGTM"
mdf approve li_ion_cathode_stability_v1 --no-mint-doi
mdf approve li_ion_cathode_stability_v1 --json

# Reject with feedback
mdf reject li_ion_cathode_stability_v1 \
  --reason "Missing experimental methods section" \
  --suggestions "Add synthesis conditions and XRD instrument parameters"
```

## Scripting & CI Patterns

```bash
# Validate, publish, and watch in a CI pipeline
mdf validate --json && \
mdf publish --submit --json && \
mdf watch --json --timeout 600

# Extract status from JSON
mdf status my_dataset_v1 --json | jq -r '.submission.status'

# Check if doctor passes (exit code 0 = all checks pass)
mdf doctor --json | jq -e '.success'

# Pipe-safe — spinners auto-suppress, no ANSI in redirected output
mdf list --json > datasets.json
mdf search "titanium" --json | jq '.results[].title'

# Respect NO_COLOR convention
NO_COLOR=1 mdf list
NO_COLOR=1 mdf doctor
```

## Power User: Backend Subcommands

```bash
# These still work but are hidden from mdf --help
mdf backend health
mdf backend submit --payload submission.json
mdf backend update-status --source-id my_dataset_v1 --version 1.0 --status published
mdf backend stream-create --title "In-Situ TEM Observations"
```

## Full Pipeline Example

```bash
# 1. Set up
mdf login
mkdir cobalt_oxide_dft && cd cobalt_oxide_dft

# 2. Create manifest
mdf init \
  --title "DFT Formation Energies of Cobalt Oxide Polymorphs" \
  --author "Ben Blaiszik" \
  -d "PBE+U formation energies for CoO, Co2O3, Co3O4 computed with VASP 6.4"

# 3. Add your data files to the directory
cp ~/calculations/cobalt_oxide/*.json ./

# 4. Discover metadata from files
mdf manifest discover *.json

# 5. Validate
mdf validate

# 6. Publish
mdf publish --submit

# 7. Watch until done
mdf watch

# 8. Cite your new dataset
mdf cite cobalt_oxide_dft_v1 -f bibtex --copy

# 9. Share with collaborators
mdf open cobalt_oxide_dft_v1
```
