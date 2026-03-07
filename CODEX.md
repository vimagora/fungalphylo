# CODEX

Read this first when resuming work on `fungalphylo`.

Before making changes in a new session:

1. read `agent_context/project_master.md`
2. read `agent_context/project_live.md`
3. then inspect the current code before trusting older assumptions

## Project In One Minute

`fungalphylo` is a lightweight Python CLI for fungal phylogenomics intake and normalization:

`init -> ingest -> fetch-index -> autoselect -> review -> restore -> download -> stage -> busco-slurm`

The codebase already has good foundations:

- SQLite state
- raw file preservation
- structured JSONL logs
- canonical FASTA staging
- TSV-based human review

The main risks are not scale-related. They are semantic drift and incomplete restart contracts.

## Current Truths

- Trust the code over the README when they conflict.
- `stage` now writes snapshot-scoped artifacts under `staging/<staging_id>/`.
- `busco-slurm` consumes a chosen `staging_id` or the latest snapshot by default.
- `autoselect` now honors config-driven scoring weights and configurable ban patterns.
- `db` now enforces read-only SQL and opens SQLite in read-only mode.
- `restore --dry-run` and `download --dry-run` now build payloads without requiring JGI authentication.
- `download` now safely creates `unmatched_manifest.tsv` even when the kept-manifest directory does not yet exist.
- restore/download batch directories are now indexed in SQLite via `restore_requests` and `download_requests`.
- download failure paths for malformed non-zip responses and missing manifest files are now covered by tests and recorded as failed batches.
- compute commands beyond BUSCO are placeholders or absent.
- the pytest suite now covers snapshot creation/reuse, cache-only fetch ingest, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, restore/download request-ledger writes, restore dry-run/continue-on-error behavior, and download manifest mismatch/malformed-bundle handling.
- legacy `staged_files` has been removed from the schema; `staging_files` is the active staging artifact table.

## Files To Read First

1. `agent_context/project_master.md`
2. `agent_context/project_live.md`
3. `src/fungalphylo/cli/commands/stage.py`
4. `src/fungalphylo/core/paths.py`
5. `src/fungalphylo/cli/commands/download.py`
6. `src/fungalphylo/cli/commands/restore.py`
7. `src/fungalphylo/cli/commands/fetch_index.py`

## Main Architectural Question

The artifact model decision is made: immutable snapshot artifacts live under `staging/<staging_id>/...`.

The active implementation work is to make every relevant command and document honor that decision.

## High-Priority Known Issues

- some onboarding docs still contain historical pre-refactor notes
- placeholder command modules exist without functionality
- restore/download are now batch-tracked in SQLite, but the remote-side lifecycle is still not modeled beyond local batch outcomes
- `download` skip behavior still depends on raw-file presence or staged file IDs rather than checksum-aware raw state
- download still has no retry/backoff policy for transient HTTP or bundle-processing failures

## Working Principles For Changes

- keep the project lightweight
- prefer plain files + SQLite over new orchestration layers
- make restart behavior explicit and durable
- add tests before or alongside behavior changes
- keep CSC/Puhti usage in mind: deterministic paths, durable manifests, resumable batch steps

## Quick Verification

After edits, at minimum run:

```bash
python -m compileall src
```

Current focused tests:

```bash
.venv/bin/pytest tests/test_restore_download.py tests/test_core_utils.py tests/test_autoselect.py tests/test_db_command.py tests/test_stage_snapshots.py tests/test_fetch_index_cache.py
```
