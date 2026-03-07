# Project Live: fungalphylo

This is the working status board for developers. It is intentionally short and operational.

## Current State

- Intake workflow is implemented through `stage`.
- BUSCO SLURM script generation exists.
- OrthoFinder/species-tree/family compute runs are not implemented.
- The current pytest suite covers staging snapshots, cache-ingest behavior, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, and raw path resolution.
- Code compiles with `python -m compileall src`.

## Recently Completed

- `stage` now writes snapshot-scoped artifacts under `staging/<staging_id>/`.
- snapshot artifact metadata is now recorded in `staging_files`.
- stage artifact reuse now uses a cache key instead of the old mutable-output assumption.
- `busco-slurm` can target a chosen `staging_id` and defaults to the latest snapshot.
- the broken duplicate path helper in `src/fungalphylo/core/paths.py` was removed.
- README has been updated to the snapshot-first model.
- `fetch-index --ingest-from-cache` no longer requires JGI authentication.
- `restore` now handles non-HTTP per-payload failures under `--continue-on-error`.
- `autoselect` now honors config-driven scoring weights and configurable ban patterns, with regression tests.
- `db` now enforces read-only SQL and opens SQLite in read-only mode, with regression tests.
- `restore --dry-run` and `download --dry-run` no longer require JGI authentication when only building payloads.
- `download` now safely writes `unmatched_manifest.tsv` even when the target directory does not already exist.
- restore/download request batches are now indexed in SQLite via `restore_requests` and `download_requests`, and `status` reads from that ledger.
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

- decide whether request-ledger rows need a child table for per-payload outcomes or whether JSONL files remain sufficient
- decide whether download should gain a minimal retry/backoff policy for transient HTTP failures

### Restartability

- decide and document the canonical restart model for each command
- finish propagating the new immutable snapshot model through downstream commands and docs
- move from existence/file-id based skips toward checksum + parameter aware skips where it matters
- decide whether restore/download request history should also be tracked in SQLite

### Maintainability

- remove or implement placeholder command modules
- centralize repeated timestamp helper logic if it starts spreading further

## Known Technical Debt

- some onboarding docs still have historical notes that should now be rewritten
- several empty modules imply features that do not yet exist
- `pydantic` is declared but not materially used
- `download` skip behavior is still based on file presence or staged file IDs rather than checksum-aware raw state
- restore/download request batches are queryable from SQLite, but per-payload outcomes still live only in JSONL/files
- download has no built-in retry/backoff semantics for transient remote errors

## Suggested Work Order

1. Decide whether the request ledger needs per-payload child rows or whether JSONL remains the right boundary.
2. Decide whether download should gain a minimal retry/backoff policy for transient HTTP failures.
3. Update the remaining onboarding docs and README details to match code exactly.
4. Only then implement additional compute steps.

## Definition Of “Good Enough” For The Next Milestone

- a developer can rerun any completed intake command without guessing what will happen
- staged outputs have an unambiguous lifecycle
- failures in network batches are resumable and inspectable
- the README and onboarding docs match the code
- at least a minimal regression test suite exists
