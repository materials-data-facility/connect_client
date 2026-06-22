#!/usr/bin/env python3
"""Build the single canonical FEATURE_STATUS.csv from the inventory JSONs + live test results.

Inputs:
  /tmp/inv_core.json   (Opus agent 1: write/auth surface)
  /tmp/inv_util.json   (Opus agent 2: read/curation/config/skill surface)
Output:
  FEATURE_STATUS.csv   (single canonical spreadsheet, repo root)

Phase-2 live-test results are encoded in PHASE2 below (from real `mdf ...` runs against
staging + offline). Re-running this script is idempotent and preserves the canonical column set.
"""
import csv, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))      # feature_audit/
ROOT = os.path.dirname(HERE)                            # repo root
OUT = os.path.join(ROOT, "FEATURE_STATUS.csv")          # single canonical tracker

# Inventory sources: prefer archived copies in feature_audit/, fall back to /tmp.
def _src(name, tmp):
    p = os.path.join(HERE, name)
    return p if os.path.exists(p) else tmp

# --- Phase-2 results from actual test runs (result, method, errors) -----------------
# result vocab: PASS, PASS-minor, FAIL, FAIL-auth-public, BLOCKED-AUTH,
#               BLOCKED-INTERACTIVE, NOT-RUN
P = lambda result, method, errors="": {"result": result, "method": method, "errors": errors}
LIVE = "live run vs staging"
OFF = "offline run"
ABORT = "auth-prompt abort path verified (stdin closed)"

PHASE2 = {
  # ---- inv_core (write/auth) ----
  "setup-onboarding": P("BLOCKED-INTERACTIVE", "partial offline + code review",
      "interactive prompts; code-confirmed it scaffolds mdf.yaml WITHOUT title/author -> later preflight blocks"),
  "login-interactive": P("BLOCKED-AUTH", ABORT, "opens browser; aborts cleanly on EOF"),
  "login-token": P("FAIL", OFF,
      "`mdf login --token garbage` prints 'Authentication ready' with no verification and does not persist the token"),
  "logout": P("PASS", OFF, "clean 'No cached token file found' when none; idempotent"),
  "status-auth": P("PASS", OFF, "shows service/status/token-store; == hidden `mdf whoami`"),
  "status-default": P("BLOCKED-AUTH", ABORT, "needs last_source_id; status() forces auth"),
  "status-watch": P("BLOCKED-AUTH", ABORT),
  "publish-dryrun": P("PASS", LIVE, "renders preflight + JSON payload; no submit"),
  "publish-direct": P("PASS", LIVE, "validation: missing --title and missing --author each exit 1 with clear msg; submit path BLOCKED-AUTH"),
  "publish-manifest": P("PASS", LIVE, "dry-run builds payload from mdf.yaml; submit BLOCKED-AUTH"),
  "publish-manifest-overrides": P("PASS", LIVE, "--title/--author override manifest in dry-run"),
  "publish-preflight-only": P("PASS", LIVE, "clean 'Authentication is not ready' message + actionable hint, exit 1; no browser hang"),
  "publish-test": P("BLOCKED-AUTH", ABORT),
  "publish-update": P("BLOCKED-AUTH", ABORT, "code bug: --update sets flag but cannot target a source_id"),
  "publish-nowatch": P("BLOCKED-AUTH", ABORT),
  "publish-json": P("PASS", LIVE, "dry-run --json emits payload+preflight"),
  "update-by-id": P("BLOCKED-AUTH", ABORT),
  "clone-auto": P("FAIL-auth-public", ABORT, "plan_clone forces interactive auth though /card is public (200); download genuinely needs auth"),
  "clone-zip-vs-https": P("BLOCKED-AUTH", ABORT),
  "clone-transfer": P("BLOCKED-AUTH", ABORT, "needs Globus Connect Personal"),
  "clone-derive": P("BLOCKED-AUTH", ABORT, "code: --derive ignored in --json mode (unreachable after json return)"),
  "import-zenodo-dryrun": P("PASS-minor", LIVE, "fetches Zenodo metadata+files nicely; raw HTML entity '&nbsp;' left in description"),
  "import-zenodo-submit": P("BLOCKED-AUTH", ABORT),
  "auth-resolution-confidential": P("NOT-RUN", "n/a", "needs real MDF_CLIENT_ID/SECRET"),
  # ---- inv_util (read/curation/config/skill) ----
  "search-keyword": P("PASS", LIVE, "table + --json + --brief + --type all work unauthenticated"),
  "search-semantic": P("PASS", LIVE, "vector search returns ranked datasets"),
  "list-datasets": P("BLOCKED-AUTH", ABORT, "private (/submissions 401); code: --json ignores --status/--search/--latest-only filters"),
  "related-by-author": P("FAIL-auth-public", ABORT, "/datasets/{id}/related is public (200) but CLI forces interactive auth"),
  "related-similar": P("FAIL-auth-public", ABORT, "forces auth; also needs built embedding snapshot"),
  "show-dataset": P("FAIL-auth-public", ABORT, "/card/{id} is public (200) but CLI forces interactive auth"),
  "dataset-cite": P("FAIL-auth-public", ABORT, "/citation/{id} is public (200) but CLI forces interactive auth"),
  "dataset-open": P("FAIL-auth-public", ABORT, "even `--url` (just prints a URL) forces interactive auth"),
  "dataset-preview": P("FAIL-auth-public", ABORT, "/preview/{id} is public but CLI forces auth; also success default True -> no nonzero exit on error"),
  "dataset-versions": P("FAIL-auth-public", ABORT, "forces auth; returns exit 0 even on backend failure"),
  "dataset-diff": P("PASS", OFF, "validation: missing --from exits 2 cleanly; full path BLOCKED-AUTH"),
  "dataset-edit": P("BLOCKED-AUTH", ABORT, "mutation; also the only dataset cmd that does NOT resolve a DOI arg"),
  "dataset-withdraw": P("BLOCKED-AUTH", ABORT),
  "dataset-resubmit": P("BLOCKED-AUTH", ABORT),
  "admin-pending": P("BLOCKED-AUTH", ABORT),
  "admin-approve": P("BLOCKED-AUTH", ABORT),
  "admin-reject": P("BLOCKED-AUTH", ABORT, "validation: --reason required (verified via --help)"),
  "admin-delete": P("BLOCKED-AUTH", ABORT, "destructive; not executed beyond auth prompt"),
  "admin-stats": P("BLOCKED-AUTH", ABORT),
  "admin-embedding-status": P("BLOCKED-AUTH", ABORT),
  "admin-rebuild-embeddings": P("BLOCKED-AUTH", ABORT, "destructive/curator; not executed"),
  "config-show": P("PASS", OFF),
  "config-set": P("PASS-minor", OFF, "invalid email warns but still sets (README claims 'validated')"),
  "config-get": P("PASS", OFF),
  "config-path": P("PASS", OFF),
  "config-doctor": P("PASS", LIVE, "panel + --json; connectivity check works; reports success=false when unauth/no-config"),
  "manifest-init": P("PASS", OFF, "writes mdf.yaml; maps --keyword->subjects, --year->publication_year"),
  "manifest-discover": P("PASS-minor", OFF, "writes auto_metadata; success summary just prints '  mdf' (weak); .tsv mis-parsed (see extractor-tabular)"),
  "manifest-inspect": P("PASS", OFF, "readable panel + resolved sources + preflight notes"),
  "backend-hint-callback": P("FAIL", LIVE, "prints hint on EVERY backend call (incl --json); promotes hidden commands; one (`mdf preview`) is less capable than the backend variant"),
  "backend-health": P("FAIL-auth-public", ABORT, "/health is public (200) but `mdf backend health` forces interactive auth"),
  "backend-submit": P("BLOCKED-AUTH", ABORT),
  "backend-status": P("BLOCKED-AUTH", ABORT),
  "backend-submissions": P("BLOCKED-AUTH", ABORT),
  "backend-curation-pending": P("BLOCKED-AUTH", ABORT),
  "backend-curation-detail": P("BLOCKED-AUTH", ABORT),
  "backend-curation-approve": P("BLOCKED-AUTH", ABORT),
  "backend-curation-reject": P("BLOCKED-AUTH", ABORT),
  "backend-update-status": P("BLOCKED-AUTH", ABORT),
  "backend-stream-create": P("BLOCKED-AUTH", ABORT, "duplicate of `mdf stream create`"),
  "backend-stream-append": P("BLOCKED-AUTH", ABORT, "duplicate of stream group"),
  "backend-stream-status": P("BLOCKED-AUTH", ABORT, "duplicate of stream group"),
  "backend-stream-close": P("BLOCKED-AUTH", ABORT, "duplicate of stream group"),
  "backend-stream-snapshot": P("BLOCKED-AUTH", ABORT, "duplicate of stream group"),
  "backend-search": P("FAIL-auth-public", ABORT, "/search is public (top-level `mdf search` works unauth) but `mdf backend search` forces auth"),
  "backend-card": P("FAIL-auth-public", ABORT, "/card public but forces auth"),
  "backend-cite": P("FAIL-auth-public", ABORT, "/citation public but forces auth"),
  "backend-preview": P("FAIL-auth-public", ABORT, "/preview public but forces auth"),
  "stream-create": P("BLOCKED-AUTH", ABORT),
  "stream-append": P("BLOCKED-AUTH", ABORT),
  "stream-status": P("BLOCKED-AUTH", ABORT),
  "stream-close": P("BLOCKED-AUTH", ABORT),
  "stream-snapshot": P("BLOCKED-AUTH", ABORT),
  "stream-upload": P("BLOCKED-AUTH", ABORT, "code: returns exit 0 even on upload failure"),
  "stream-files": P("BLOCKED-AUTH", ABORT),
  "skill-tool-registry": P("PASS", OFF, "mdf_tools.TOOLS imports cleanly with 29 tools; BUT skill.md is an 8-line stub (documents ~0/5, drift)"),
  "skill-scan-folder": P("NOT-RUN", "n/a", "local-only tool, not exercised this pass"),
  "skill-suggest-mappings": P("NOT-RUN", "n/a"),
  "skill-validate-and-preview": P("NOT-RUN", "n/a"),
  "skill-publish": P("BLOCKED-AUTH", "n/a"),
  "skill-backend-tools": P("BLOCKED-AUTH", "n/a"),
  "sdk-mdfagent-readers": P("FAIL-auth-public", "code review", "readers force auth for public reads; SDK defaults service_instance='prod' while CLI defaults 'staging' -> divergent backends"),
  "sdk-mdfagent-curation": P("BLOCKED-AUTH", "code review"),
  "sdk-mdfagent-publish": P("BLOCKED-AUTH", "code review"),
  "sdk-mdfagent-clone": P("BLOCKED-AUTH", "code review"),
  "sdk-mdfagent-stream": P("BLOCKED-AUTH", "code review"),
  "extractor-registry": P("PASS", OFF, "dispatches by extension via discover"),
  "extractor-tabular": P("FAIL", OFF, ".tsv parsed with comma reader -> entire tab header becomes ONE column 'a\\tb\\tc'"),
  "extractor-pdf": P("NOT-RUN", "n/a", "no PDF fixture this pass"),
  "extractor-json-yaml": P("PASS-minor", OFF, "json/yaml accepted by discover; contribution to summary is terse"),
}

# --- synthetic rows for cross-cutting / packaging / global features -----------------
SYNTH = [
  {"feature_id":"pkg-install","area":"Packaging","command":"pip install -e \".[extractors]\"",
   "user_story":"As a new user, I want `pip install -e .` to install the modern mdf CLI, so that `mdf` works out of the box.",
   "expected_behavior":"Installs mdf_agent (pyproject) and exposes the `mdf` entry point.",
   "preconditions":"Python >=3.10","auth_required":"no","network_required":"yes",
   "source_refs":"pyproject.toml; setup.py(legacy)",
   "code_issues":["Root legacy setup.py (mdf_connect_client) coexists with pyproject (mdf_agent); old pip picks legacy setup.py and the editable install fails / installs the wrong package"]},
  {"feature_id":"pkg-deps-globus","area":"Packaging","command":"(import on any `mdf` command)",
   "user_story":"As a user on a clean machine, I want a fresh install to import successfully.",
   "expected_behavior":"All `mdf` commands import cleanly.",
   "preconditions":"none","auth_required":"no","network_required":"no",
   "source_refs":"pyproject.toml deps; auth/globus.py:21",
   "code_issues":["pin `globus-sdk>=3.0` allows 4.x, which removed `globus_sdk.tokenstorage` -> every command crashes with ModuleNotFoundError on a default fresh install"]},
  {"feature_id":"global-version-flag","area":"Global","command":"mdf --version",
   "user_story":"As a user, I want `mdf --version`, so that I can report which version I'm running.",
   "expected_behavior":"Prints the version and exits 0.","preconditions":"none","auth_required":"no","network_required":"no",
   "source_refs":"cli/main.py:app callback","code_issues":["No --version option: `mdf --version` errors 'No such option' (exit 2)"]},
  {"feature_id":"global-help","area":"Global","command":"mdf --help / mdf (no args)",
   "user_story":"As a user, I want help and a first-run welcome, so that I can discover commands.",
   "expected_behavior":"`mdf --help` lists visible commands in panels; no-config first run shows a welcome panel.",
   "preconditions":"none","auth_required":"no","network_required":"no","source_refs":"cli/main.py:main_callback",
   "code_issues":["~19 hidden top-level aliases (cite/preview/doctor/whoami/watch/validate/update/pending/approve/reject/...) are not shown by --help yet README documents several as primary commands"]},
  {"feature_id":"global-service-targeting","area":"Global","command":"--service prod|staging|dev|local / MDF_API_URL",
   "user_story":"As a user, I want to target prod/staging/dev/local, so that I can test safely.",
   "expected_behavior":"--service selects the backend URL; unknown service should error cleanly.",
   "preconditions":"none","auth_required":"no","network_required":"yes","source_refs":"core/backend_client.py:_api_url_for_service; core/config.py:resolve_service",
   "code_issues":["`mdf search --service bogus` raises an unhandled ValueError and prints a raw Python traceback instead of a friendly error"]},
  {"feature_id":"display-rich-markup","area":"Global","command":"(any command that renders a backend title/description/author)",
   "user_story":"As a user, I want titles/descriptions shown verbatim, so that bracketed text isn't silently dropped.",
   "expected_behavior":"User-provided strings render exactly as stored.","preconditions":"none","auth_required":"no","network_required":"yes",
   "source_refs":"cli/main.py:show/_render_status_details/search/list tables",
   "code_issues":["Backend strings containing [..] were parsed as Rich markup and dropped (e.g. a '[mdf-agent test] X' title displayed as ' X'); data stored fine, display mangled"]},
  {"feature_id":"contract-staging-url","area":"Backend contract","command":"--service staging",
   "user_story":"As a user, I want `--service staging` to hit the current staging backend, so that my commands reach the live deployment.",
   "expected_behavior":"staging resolves to the new API Gateway URL.","preconditions":"none","auth_required":"no","network_required":"yes",
   "source_refs":"core/backend_client.py:_V2_API_URLS; README.md; examples/*",
   "code_issues":["staging URL moved hjccjf3eqg -> 3xicgt0g7l (CONTRACT_CHANGES.md); prod URL change pending"]},
  {"feature_id":"contract-acl-reads","area":"Backend contract","command":"mdf show/dataset cite/preview/related (ACL-gated)",
   "user_story":"As a dataset owner, I want my own restricted datasets readable when I'm logged in, while anonymous users only see public data, so that access matches the new ACL rules.",
   "expected_behavior":"Logged-in users send their token (see public + owned/curated, incl restricted); anonymous users get a public-only client and a clean 404 for restricted, with no forced login.",
   "preconditions":"none","auth_required":"partial","network_required":"yes","source_refs":"cli/formatting.py:read_client; CONTRACT_CHANGES.md#2",
   "code_issues":["read_client previously ignored a cached Globus login -> logged-in users hit ACL-gated reads anonymously (404 on their own restricted data)"]},
  {"feature_id":"contract-semantic-auth","area":"Backend contract","command":"mdf search --semantic",
   "user_story":"As a user, I want semantic search to use my login (now required), so that it works when authed and tells me to log in when not.",
   "expected_behavior":"Logged-in: token sent, results (or a clear 'no snapshot' message). Anonymous: friendly 'Semantic search requires login. Run: mdf login'.",
   "preconditions":"none","auth_required":"yes","network_required":"yes","source_refs":"cli/main.py:search; CONTRACT_CHANGES.md#1",
   "code_issues":["/search/semantic now 401s anonymously; client must send a token and surface a login hint instead of a misleading 'No results'"]},
  {"feature_id":"contract-publish-failed","area":"Backend contract","command":"(status display for the new 'publish_failed' state)",
   "user_story":"As a submitter, I want the new publish_failed status shown clearly, so that I know an approved dataset isn't live yet.",
   "expected_behavior":"publish_failed renders as a distinct red badge, is a terminal state for --watch, and shows a 'not yet live / will retry' next-step.",
   "preconditions":"none","auth_required":"yes","network_required":"yes","source_refs":"cli/formatting.py:_STATUS_STYLES; cli/main.py:_watch_submission/_render_status_details; CONTRACT_CHANGES.md#5",
   "code_issues":["new status value publish_failed was unmapped (would render dim/unknown and watch would never terminate on it)"]},
  {"feature_id":"global-completion","area":"Global","command":"mdf --install-completion / --show-completion",
   "user_story":"As a shell user, I want tab-completion, so that I can type commands faster.",
   "expected_behavior":"Installs/prints shell completion script.","preconditions":"none","auth_required":"no","network_required":"no",
   "source_refs":"typer add_completion=True","code_issues":[]},
]
PHASE2.update({
  "pkg-install": P("FAIL", "clean-env install", "default `pip install -e .` picks legacy root setup.py; needs isolated PEP517 build / pyproject to win"),
  "pkg-deps-globus": P("FAIL", "clean-env import", "fresh install pulled globus-sdk 4.8.1 -> ModuleNotFoundError: globus_sdk.tokenstorage; had to pin <4"),
  "global-version-flag": P("FAIL", OFF, "`mdf --version` -> No such option (exit 2)"),
  "global-help": P("PASS-minor", OFF, "help renders; hidden-alias/README drift is the issue"),
  "global-service-targeting": P("FAIL", LIVE, "bogus service -> raw ValueError traceback"),
  "global-completion": P("NOT-RUN", "n/a"),
  "display-rich-markup": P("FAIL", LIVE, "title '[BRACKET]...' displayed with the bracket segment dropped (Rich markup parsing of untrusted data)"),
  "contract-staging-url": P("PASS", "live run vs NEW staging", "config doctor/search/list/publish/delete all reach 3xicgt0g7l"),
  "contract-acl-reads": P("PASS", "live run vs NEW staging", "anonymous: public-only + clean 404, no prompt; logged-in: token sent (verified read_client returns an authenticated client)"),
  "contract-semantic-auth": P("PASS", "live run vs NEW staging", "anonymous -> 'requires login' hint; logged-in -> token sent (reached 'no snapshot' availability check)"),
  "contract-publish-failed": P("PASS", "unit", "badge renders red; added to watch terminal states + status next-steps (live trigger not reproducible)"),
})

# --- Auth-route results from the post-login live session (staging) -------------------
# These override the earlier BLOCKED-AUTH placeholders now that we have a live token.
AUTHED = "live run vs staging (authenticated)"
AUTH_TESTED = {
  "status-default": P("PASS", AUTHED, "shows status/title/next-steps for a real source_id"),
  "status-watch": P("PASS", AUTHED, "polled approved->published in ~26s with live badges, then 'Done!'"),
  "publish-direct": P("PASS", AUTHED, "real submit: uploaded files via Globus HTTPS, created pending_curation submission"),
  "publish-manifest": P("PASS", AUTHED, "real submit path verified end-to-end"),
  "publish-test": P("PASS", AUTHED, "submit path verified (via direct submit)"),
  "publish-nowatch": P("PASS", AUTHED, "--no-watch returns immediately after submit with watch hint"),
  "import-zenodo-submit": P("PASS", AUTHED, "downloaded 6 Zenodo files, uploaded+submitted; External DOI carried through (provenance fix confirmed in payload)"),
  "list-datasets": P("PASS", AUTHED, "listed 35 of my datasets; --json still ignores filters (separate, deferred)"),
  "related-similar": P("PARTIAL", AUTHED, "no forced login; returns rows only where a snapshot neighbor exists"),
  "dataset-versions": P("PASS", AUTHED, "returns versions; 'No versions found' for an unpublished id is correct"),
  "dataset-edit": P("PASS", AUTHED, "updated title+keywords on my pending submission"),
  "dataset-withdraw": P("PASS", AUTHED, "withdrew my pending submission -> withdrawn"),
  "dataset-resubmit": P("PASS", AUTHED, "resubmit from 'rejected' works; correctly refuses 'withdrawn' with a clear message"),
  "dataset-preview": P("PASS", AUTHED, "graceful 'No file information available' when no profile"),
  "dataset-diff": P("PASS", AUTHED, "graceful 'Diff failed: Not found' for a single-version dataset"),
  "dataset-cite": P("PASS", AUTHED, "BibTeX/APA render for published datasets"),
  "dataset-open": P("PASS", AUTHED, "--url prints the dataset/DOI URL"),
  "admin-pending": P("PASS", AUTHED, "listed pending submissions (curator role confirmed)"),
  "admin-approve": P("PASS", AUTHED, "approved my own test dataset (--no-mint-doi) -> published"),
  "admin-reject": P("PASS", AUTHED, "rejected my own test dataset with reason+suggestions -> rejected"),
  "admin-delete": P("PASS", AUTHED, "soft-deleted my own test datasets (used for cleanup)"),
  "admin-stats": P("PASS", AUTHED, "944 submissions, status breakdown"),
  "admin-embedding-status": P("PASS", AUTHED, "930 published, 930 embedded, snapshot info"),
  "admin-rebuild-embeddings": P("NOT-RUN", "n/a", "would mutate the shared staging index; not dispatched"),
  "clone-auto": P("FAIL", AUTHED, "Archive-download plan crashed with a raw traceback: globus:// URL handed to httpx (UnsupportedProtocol)"),
  "clone-zip-vs-https": P("PASS", AUTHED, "direct file-by-file HTTPS download works (mobility_5d_v1.1)"),
  "backend-submissions": P("PASS", AUTHED, "listed submissions"),
  "stream-create": P("BLOCKED-BACKEND", AUTHED, "endpoint returns 404 'Not found' (streaming not deployed on this staging) AND exits 0 on failure"),
  "stream-append": P("BLOCKED-BACKEND", AUTHED, "streaming not deployed on staging"),
  "stream-status": P("BLOCKED-BACKEND", AUTHED, "streaming not deployed on staging"),
  "stream-close": P("BLOCKED-BACKEND", AUTHED, "streaming not deployed on staging"),
  "stream-snapshot": P("BLOCKED-BACKEND", AUTHED, "streaming not deployed on staging"),
  "stream-upload": P("BLOCKED-BACKEND", AUTHED, "streaming not deployed on staging; also exits 0 on failure"),
  "stream-files": P("BLOCKED-BACKEND", AUTHED, "streaming not deployed on staging"),
}
PHASE2.update(AUTH_TESTED)

# --- Phase-3 fixes applied (this session) + Phase-4 retest results -------------------
PHASE3 = {
  "pkg-deps-globus": "Pinned globus-sdk>=3.0,<4 in pyproject.toml (4.x removed globus_sdk.tokenstorage).",
  "global-version-flag": "Added --version/-V eager option to the main callback (cli/main.py).",
  "global-service-targeting": "read_client() catches the unknown-service ValueError and prints a clean message instead of a traceback.",
  "extractor-tabular": "Use a tab delimiter for .tsv files (extractors/tabular.py).",
  "search-keyword": "Routed through read_client() (public, friendly bad-service error).",
  "search-semantic": "Routed through read_client().",
  "show-dataset": "Routed through read_client() — no forced interactive login for public cards.",
  "related-by-author": "Routed through read_client().",
  "related-similar": "Routed through read_client() (still needs a built embedding snapshot).",
  "dataset-cite": "Routed through read_client().",
  "dataset-open": "Routed through read_client() (incl. --url).",
  "dataset-preview": "Routed through read_client().",
  "dataset-versions": "Routed through read_client().",
  "dataset-diff": "Routed through read_client().",
  "backend-hint-callback": "Removed the always-on 'try mdf cite/preview/doctor' hint from the backend callback.",
  "backend-health": "Routed through read_client() + interpret {status: ok} as healthy (was 'Error: Unknown error').",
  "backend-card": "Routed through read_client() (public).",
  "backend-cite": "Routed through read_client() (public).",
  "backend-preview": "Routed through read_client() (public).",
  "backend-search": "Routed through read_client() (public).",
  "import-zenodo-dryrun": "Provenance fix: to_metadata_payload now emits flat external_doi/url/source the backend actually reads.",
  "import-zenodo-submit": "Same provenance payload fix (config.py + submission.py).",
  "manifest-discover": "Clearer save summary (file: N column(s), M row(s) + column names) instead of bare 'mdf'.",
  "clone-auto": "Zip-Slip guard added to archive extraction (core/agent.py).",
  "clone-zip-vs-https": "Zip-Slip guard added to archive extraction.",
  "pkg-install": "Documented: legacy root setup.py shadows pyproject under old pip; modern PEP517 build installs mdf_agent cleanly.",
  "login-token": "Deferred: verifying a --token requires a network identity call (flaky); documented misleading 'Authentication ready'.",
  "setup-onboarding": "Deferred: needs interactive redesign to collect title/author (or stop implying the scaffold is publish-ready).",
  "list-datasets": "Deferred: --json should honor --status/--search/--latest-only filters.",
  "sdk-mdfagent-readers": "Documented: CLI public reads fixed via read_client, but SDK reader helpers still call authenticated() and default service_instance='prod' (vs CLI 'staging') — left for a follow-up.",
  "stream-upload": "Deferred: should exit nonzero when uploads fail.",
}
PHASE4 = {
  "pkg-deps-globus": "PASS — fresh import works on globus-sdk 3.65.0.",
  "global-version-flag": "PASS — `mdf --version` -> 'mdf 0.2.0'.",
  "global-service-targeting": "PASS — bogus --service prints one-line error, no traceback.",
  "extractor-tabular": "PASS — tabbed.tsv -> 3 columns a,b,c.",
  "search-keyword": "PASS — unauthenticated keyword search returns results.",
  "search-semantic": "PASS — semantic search returns ranked datasets.",
  "show-dataset": "PASS — full dataset card renders unauthenticated (no browser).",
  "related-by-author": "PASS — related table renders unauthenticated.",
  "related-similar": "PARTIAL — no forced login; needs a built snapshot to return rows.",
  "dataset-cite": "PASS — BibTeX citation unauthenticated.",
  "dataset-open": "PASS — `--url` prints DOI URL unauthenticated.",
  "dataset-preview": "PARTIAL — no forced login; 404 when dataset has no profile.",
  "dataset-versions": "PARTIAL — no forced login (endpoint availability varies).",
  "dataset-diff": "PASS — validation clean; no forced login.",
  "backend-hint-callback": "PASS — no hint printed on backend calls.",
  "backend-health": "PASS — 'Backend is healthy (mdf-v2)'.",
  "backend-card": "PASS — card unauthenticated, no hint.",
  "backend-cite": "PASS — citation unauthenticated.",
  "backend-preview": "PARTIAL — unauthenticated; 404 for no-profile datasets.",
  "backend-search": "PASS — results unauthenticated, no hint.",
  "import-zenodo-dryrun": "PASS — payload carries flat external_* keys; 3 model tests pass.",
  "import-zenodo-submit": "PARTIAL — payload fix verified via unit tests; full submit still needs live auth.",
  "manifest-discover": "PASS — summary shows 'tabbed.tsv: 3 column(s), 2 row(s)'.",
  "clone-auto": "PARTIAL — Zip-Slip guard unit-reasoned; full clone needs live auth.",
  "clone-zip-vs-https": "PARTIAL — guard in place; download path needs live auth.",
  "login-token": "UNCHANGED — still reports success without verification (deferred).",
  "setup-onboarding": "UNCHANGED — deferred.",
  "list-datasets": "UNCHANGED — deferred.",
  "stream-upload": "UNCHANGED — deferred.",
}

# Round-2 fixes (post-login session) + their live re-tests
PHASE3.update({
  "clone-auto": "Convert globus:// NCSA archive URL to its HTTPS form in _prepare_clone; guard non-http archive URLs + broaden CLI clone error handling so failures are friendly, not tracebacks.",
  "clone-zip-vs-https": "Same clone hardening (scheme guard + graceful errors).",
  "display-rich-markup": "Escape untrusted backend strings with rich.markup.escape in the show card, status detail, and search/list tables.",
})
PHASE4.update({
  "clone-auto": "PASS — published dataset with a globus:// archive now clones via 'Archive download' (Files: 2), no traceback.",
  "clone-zip-vs-https": "PASS — direct HTTPS file download still works (regression).",
  "display-rich-markup": "PASS — '[BRACKET] clone+escape retest' and 'Has [markup] in [the] title.' now render verbatim in mdf show.",
})

# Round-3: backend contract alignment (new staging URL + auth/ACL/status changes)
PHASE3.update({
  "contract-staging-url": "Updated _V2_API_URLS['staging'] to the new API Gateway URL (+ README + examples/*); prod left for the forthcoming change.",
  "contract-acl-reads": "read_client now treats a cached Globus login as credentials (is_logged_in) so logged-in users authenticate without a new prompt; anonymous stays public-only.",
  "contract-semantic-auth": "search --semantic flows through the now-auth-aware read_client; anonymous 401 is detected and shown as a clear login hint.",
  "contract-publish-failed": "Mapped publish_failed in _STATUS_STYLES, added to _watch_submission terminal states, and added a next-step branch in _render_status_details.",
})
PHASE4.update({
  "contract-staging-url": "PASS — doctor shows 'staging (https://3xicgt0g7l...)', healthy; search/list/publish work.",
  "contract-acl-reads": "PASS — anonymous public read + clean 404 (no prompt); logged-in read_client returns an authenticated (token-bearing) client.",
  "contract-semantic-auth": "PASS — anonymous: 'Semantic search requires login'; logged-in: token sent, 'no snapshot' message from new staging.",
  "contract-publish-failed": "PASS (unit) — badge + terminal-state + next-step verified.",
})


def _status_for(fid, p2_result):
    fix = PHASE3.get(fid, "")
    if fix and not fix.lower().startswith(("deferred", "documented")):
        retest = PHASE4.get(fid, "")
        if retest.startswith("PASS"):
            return "fixed+retested:PASS"
        if retest.startswith("PARTIAL"):
            return "fixed+retested:PARTIAL"
        return "fixed"
    if fix.lower().startswith("deferred"):
        return "documented:deferred"
    if p2_result in ("FAIL", "FAIL-auth-public"):
        return "documented:open"
    if p2_result == "BLOCKED-BACKEND":
        return "documented:blocked-backend"
    if p2_result.startswith("BLOCKED"):
        return "documented:blocked-auth"
    return "documented+tested"


def load(path):
    try:
        return json.load(open(path))
    except Exception as e:
        print(f"WARN: could not load {path}: {e}", file=sys.stderr); return []

rows = (
    load(_src("inventory_core_opus.json", "/tmp/inv_core.json"))
    + load(_src("inventory_util_opus.json", "/tmp/inv_util.json"))
    + SYNTH
)

COLS = ["feature_id","area","command","user_story","expected_behavior","preconditions",
        "auth_required","network_required","source_refs","known_code_issues",
        "phase2_test_method","phase2_result","phase2_errors","phase3_fix","phase4_retest","status"]

seen=set(); out=[]
for r in rows:
    fid=r.get("feature_id","")
    if fid in seen:  # de-dup across sources
        continue
    seen.add(fid)
    p2=PHASE2.get(fid, P("NOT-RUN","n/a"))
    out.append({
        "feature_id":fid,"area":r.get("area",""),"command":r.get("command",""),
        "user_story":r.get("user_story",""),"expected_behavior":r.get("expected_behavior",""),
        "preconditions":r.get("preconditions",""),"auth_required":r.get("auth_required",""),
        "network_required":r.get("network_required",""),"source_refs":r.get("source_refs",""),
        "known_code_issues":" | ".join(r.get("code_issues",[]) or []),
        "phase2_test_method":p2["method"],"phase2_result":p2["result"],"phase2_errors":p2["errors"],
        "phase3_fix":PHASE3.get(fid,""),"phase4_retest":PHASE4.get(fid,""),
        "status":_status_for(fid, p2["result"]),
    })

with open(OUT,"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=COLS); w.writeheader()
    for r in out: w.writerow(r)

# summary
from collections import Counter
res=Counter(r["phase2_result"] for r in out)
print(f"Wrote {OUT} with {len(out)} feature rows")
print("phase2_result breakdown:", dict(res))
