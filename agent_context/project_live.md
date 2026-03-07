# Project Live: fungalphylo

This is the working status board for developers. It is intentionally short and operational.

## Current State

- Intake workflow is implemented through `stage`.
- BUSCO SLURM script generation exists.
- OrthoFinder/species-tree/family compute runs are not implemented.
- The current pytest suite covers staging snapshots, cache-ingest behavior, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, and raw path resolution.
- Code compiles with `python -m compileall src`.

## Recently Completed

- the implemented command restart contract is now documented in `docs/restart_contract.md`.
- `stage` now writes snapshot-scoped artifacts under `staging/<staging_id>/`.
- snapshot artifact metadata is now recorded in `staging_files`.
- stage artifact reuse now uses a cache key instead of the old mutable-output assumption.
- `busco-slurm` can target a chosen `staging_id` and defaults to the latest snapshot.
- the broken duplicate path helper in `src/fungalphylo/core/paths.py` was removed.
- README has been updated to the snapshot-first model.
- `fetch-index --ingest-from-cache` no longer requires JGI authentication.
- `restore` now handles non-HTTP per-payload failures under `--continue-on-error`.
- `restore` now retries transient `429`/`5xx`/timeout failures before marking a payload failed.
- `autoselect` now honors config-driven scoring weights and configurable ban patterns, with regression tests.
- `db` now enforces read-only SQL and opens SQLite in read-only mode, with regression tests.
- `restore --dry-run` and `download --dry-run` no longer require JGI authentication when only building payloads.
- `download` now safely writes `unmatched_manifest.tsv` even when the target directory does not already exist.
- restore/download request batches are now indexed in SQLite via `restore_requests` and `download_requests`, and `status` reads from that ledger.
- `download` now retries transient `429`/`5xx`/timeout failures and verifies raw-file `md5` when source metadata provides it.
- `status` now distinguishes checksum-mismatched raw files from missing raw files.
- malformed non-zip download responses and extracted bundles with no manifest are now covered by regression tests and recorded as failed download batches.
- pytest coverage now includes snapshot creation/reuse, cache-only fetch ingest, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, restore/download request-ledger behavior, restore partial-failure handling, and download manifest mismatch/malformed-bundle handling.
- legacy `staged_files` has been removed from the schema in favor of `staging_files`.
- `status` now reports latest snapshot details including artifact counts and reuse counts.

## What Is Working

- project initialization
- portal/file ingest from TSV/CSV/XLSX
- JGI file index fetch and cache
- candidate autoselection with explain TSV
- review export/apply loop
- restore payload generation and posting
- download bundle extraction into `raw/`
- staging into canonical per-portal FASTA files
- BUSCO sbatch script generation

## Immediate Missing Work

### Reliability

- keep request-ledger rows batch-scoped; use request directories and JSONL as the per-payload forensic record

### Restartability

- finish propagating the new immutable snapshot model through downstream commands and docs
- extend checksum + parameter aware skips where they matter beyond the current raw-file `md5` check

### Maintainability

- centralize repeated timestamp helper logic if it starts spreading further

## Known Technical Debt

- some onboarding docs still have historical notes that should now be rewritten
- `pydantic` is declared but not materially used
- `download` now verifies existing raw files against source `md5` when available; staged-snapshot skips intentionally remain based on approved source file IDs
- restore/download keep only batch rows in SQLite by design; detailed per-payload outcomes live in request directories and JSONL/files

## Suggested Work Order

1. Propagate `docs/restart_contract.md` into any remaining onboarding notes and command help where useful.
2. Only then implement additional compute steps.

## Definition Of “Good Enough” For The Next Milestone

- a developer can rerun any completed intake command without guessing what will happen
- staged outputs have an unambiguous lifecycle
- failures in network batches are resumable and inspectable
- the README and onboarding docs match the code
- at least a minimal regression test suite exists
