# MDF Agent — Feature Audit, Test, Fix & Re-test

This folder is the evidence trail behind the single canonical tracker
[`../FEATURE_STATUS.csv`](../FEATURE_STATUS.csv). That CSV is the source of truth;
everything here is supporting material.

## How this was produced

Four phases, run with **2 Opus agents + 1 Codex agent** plus live CLI testing
against the public **staging** backend (`…/staging`, reachable, search/read endpoints public):

1. **Inventory + user stories** — every feature read out of the code and given a
   user story + expected behavior.
   - Opus agent 1 → write/auth surface → `inventory_core_opus.json` (24 features)
   - Opus agent 2 → read/curation/config/skill surface → `inventory_util_opus.json` (70 features)
   - Codex agent → independent whole-app cross-check + README-drift + top-bugs → `inventory_codex_crosscheck.md`
   - Merged + 6 cross-cutting rows → **100 feature rows** in the CSV.
2. **Test every user story** — ~50 live `mdf …` invocations (logs in `test_logs/`),
   plus the unit suite. Each row carries a `phase2_result` and `phase2_errors`.
3. **Fix logistical/UX errors** — see "Fixes applied" below.
4. **Re-test** — `test_logs/phase4_retest.log` + unit suite. Each fixed row carries
   `phase4_retest`.

Test environment: Python 3.12 venv (`.venv/`), `pip install -e ".[extractors]"`,
sandboxed `HOME` so config/tokens never touched the real user profile.

## Phase-2 result snapshot (100 rows)

| result | count | meaning |
|---|---|---|
| PASS | 19 | works as expected |
| PASS-minor | 5 | works, minor UX nit |
| FAIL | 7 | concrete bug |
| FAIL-auth-public | 14 | public endpoint, but CLI forced interactive login |
| BLOCKED-AUTH | 48 | genuinely needs a live Globus login we can't complete (failure path verified graceful) |
| BLOCKED-INTERACTIVE | 1 | needs an interactive TTY |
| NOT-RUN | 6 | destructive/deploy-only/no fixture |

## Fixes applied (Phase 3) — all verified green (unit suite 200/200; baseline was 197/3-fail)

| # | Fix | Files |
|---|---|---|
| 1 | Pin `globus-sdk>=3.0,<4` (4.x removed `globus_sdk.tokenstorage` → every command crashed on a fresh install) | `pyproject.toml` |
| 2 | **Public reads no longer force a browser login.** New `read_client()` builds an unauthenticated client when no creds exist (mirrors `search`). Applied to `show`, `related`, `dataset cite/open/preview/versions/diff`, `backend health/card/cite/preview/search` | `cli/formatting.py`, `cli/main.py`, `cli/dataset_cmd.py`, `cli/backend.py` |
| 3 | Removed the always-on `mdf backend` hint ("try mdf cite/preview/doctor") that printed on every call | `cli/backend.py` |
| 4 | `mdf backend health` now reads `{status: ok}` correctly (was "Error: Unknown error") | `cli/backend.py` |
| 5 | `mdf --version` / `-V` added | `cli/main.py` |
| 6 | Unknown `--service` prints a clean one-liner instead of a raw `ValueError` traceback | `cli/formatting.py` |
| 7 | `.tsv` extracted with a tab delimiter (was collapsing the whole header into one column) | `extractors/tabular.py` |
| 8 | **Import provenance fix:** emit flat `external_doi/external_url/external_source` (what the v2 backend actually reads) instead of a nested `external` dict the backend ignored. Fixes 3 failing tests + makes `mdf import` provenance real | `models/config.py`, `models/submission.py` |
| 9 | Zip-Slip guard on `clone` archive extraction (reject members resolving outside the output dir) | `core/agent.py` |
| 10 | `manifest discover` save summary now lists file/columns/rows instead of bare `mdf` | `cli/manifest_cmd.py` |

One unit test (`test_show_resolves_doi_before_loading_card`) was updated to patch the
new `read_client` seam — same intent (DOI resolved before card load), new code path.

## Re-test highlights (Phase 4, live vs staging)

- `mdf show / dataset cite / related / dataset open --url / backend card / backend cite / backend search` → **return real data with no browser login and no hint** (were all auth-aborts before).
- `mdf backend health` → `Backend is healthy (mdf-v2)`.
- `mdf --version` → `mdf 0.2.0`; bogus `--service` → clean error; `.tsv` → 3 columns; provenance payload → flat keys.

## Post-login round (authenticated, live staging)

After a real Globus staging login, the 48 `BLOCKED-AUTH` rows were exercised on
**datasets created by this session only** (then soft-deleted for cleanup — no
third-party data touched). Validated end-to-end:

- **Publish**: `publish --submit` performs the real Globus HTTPS upload → `pending_curation`.
- **Curation lifecycle**: `admin pending/approve/reject/delete/stats/embedding-status`, `dataset edit/withdraw/resubmit`, `status`, and `status --watch` (saw `approved → published` in ~26s). Resubmit correctly refuses a `withdrawn` record (rejected-only).
- **List/reads**: `list` (35 datasets), `dataset versions/preview/diff/cite/open`.
- **Import**: `import zenodo:… --submit` downloaded 6 files, uploaded, submitted — and the **provenance fix is confirmed in the real payload** (`build_submission` emits flat `external_doi/url/source`).

Two **new bugs** found here, fixed, and re-tested live:

| Fix | What | Re-test |
|---|---|---|
| Clone `globus://` crash | The archive `download_url` is a `globus://` URI handed straight to httpx → `UnsupportedProtocol` traceback. Now converted to its HTTPS form in `_prepare_clone` (+ non-http guard + graceful CLI errors). `core/agent.py`, `cli/main.py` | Published dataset with a `globus://` archive now clones via "Archive download" (2 files), no traceback. |
| Rich-markup mangling | Titles/descriptions containing `[...]` were parsed as Rich markup and dropped (e.g. `[BRACKET] x` shown as ` x`). Now `rich.markup.escape`'d in show card, status detail, search/list tables. `cli/main.py` | `[BRACKET] clone+escape retest` / `Has [markup] in [the] title.` now render verbatim. |

`mdf stream *` is **BLOCKED-BACKEND** — the streaming endpoints return 404 (not deployed on this staging), and `stream create` also exits 0 on that failure (deferred exit-code fix).

Unit suite stayed green throughout (**200 passed**).

## Backend contract alignment (cs/aws/v2/CONTRACT_CHANGES.md)

The backend moved staging and changed several wire-contract rules. Client changes made + verified against the **new** staging (`https://3xicgt0g7l…/staging`):

| Contract change | Client update | Verified |
|---|---|---|
| New staging API Gateway URL | `_V2_API_URLS['staging']` → new URL (+ README + `examples/*`). Prod left for the forthcoming change. | `config doctor` shows new URL + healthy; search/list/publish/delete work. |
| `/search/semantic` + `/embed` now require auth | `search --semantic` routes through the auth-aware `read_client`; anonymous 401 → "Semantic search requires login" hint. | Anonymous → login hint; logged-in → token sent (reaches "no snapshot" availability check). |
| ACL-gated reads (`/card`,`/citation`,`/preview`,…) → 404 for anonymous/restricted | **`read_client` now treats a cached Globus login as credentials** (`is_logged_in`), so logged-in users authenticate (no new prompt) and can read their own restricted data; anonymous stays public-only. | Anonymous restricted `show` → clean "Not found", no prompt; logged-in `read_client` returns a token-bearing client. |
| New status `publish_failed` | Added to status badge styles, `--watch` terminal states, and a `_render_status_details` next-step ("approved but not yet live; will retry"). | Badge + terminal-state + next-step verified (live trigger not reproducible). |
| `/status` sanitized for non-owners; `/submit update` → 403; direct publish → 400; `is_curator` now group-gated | No code change needed — the renderer already uses `.get()` (missing fields safe) and the error mapper handles 403/400/401/404. | Owner status unaffected; 403/404 render cleanly. |

This `read_client` change also retro-fixes the earlier "public reads forced a login" work to the new ACL world: **anonymous = public-only (no prompt), logged-in = full token access (no prompt)** — the right behavior under both the old and new contracts.

## Deferred / still open (documented, not fixed this pass)

- `mdf login --token <bad>` reports "Authentication ready" without verifying the token.
- `mdf setup` scaffolds an `mdf.yaml` with no title/author yet points at `publish --preflight-only` (which then blocks).
- `mdf list --json` ignores `--status/--search/--latest-only`; `mdf stream upload` exits 0 even when every upload fails.
- SDK reader helpers still call `authenticated()` and default `service_instance="prod"` (CLI defaults `staging`) — CLI was fixed, SDK left for follow-up.
- **README ↔ CLI drift**: README documents many commands as primary that are hidden aliases (`mdf update/validate/whoami/pending/approve/reject/versions/edit`, the `manifest`/`stream`/`backend` groups), and `MDF_SSL_VERIFY` default is documented as `false` but the code defaults to `true`. See the drift table in `inventory_codex_crosscheck.md`.
- Deeper clone bugs (transfer uses only the first source; zip→https fallback gap) — need live auth to exercise.
- `packaging`: a legacy root `setup.py` (for the v1 `mdf_connect_client`) shadows `pyproject.toml` under old pip; a modern PEP 517 build installs `mdf_agent` cleanly.

## Regenerate the canonical CSV

```bash
python feature_audit/build_feature_csv.py   # writes ../FEATURE_STATUS.csv
```
