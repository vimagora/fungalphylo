# Project Live: fungalphylo

This is the working status board for developers. It is intentionally short and operational.

## Current State

- Intake workflow is implemented through `stage`.
- BUSCO SLURM script generation exists.
- BUSCO batch-summary import into SQLite exists via `busco ingest-results`.
- InterProScan runs can now be resumed in place with `interproscan-slurm --resume-run-id <run_id>`.
- OrthoFinder/species-tree/family compute runs are not implemented.
- The current pytest suite covers staging snapshots, cache-ingest behavior, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, and raw path resolution.
- Code compiles with `python -m compileall src`.
- A real-data validation run has now completed successfully from `init` through `stage`.

## Recently Completed

- the implemented command restart contract is now documented in `docs/restart_contract.md`.
- `stage` now writes snapshot-scoped artifacts under `staging/<staging_id>/`.
- snapshot artifact metadata is now recorded in `staging_files`.
- stage artifact reuse now uses a cache key instead of the old mutable-output assumption.
- `busco-slurm` can target a chosen `staging_id` and defaults to the latest snapshot.
- local development should only write SLURM scripts for review; optional submit paths should remain implemented but are not expected to be exercised without Puhti access
- `busco-slurm` now writes `runs/<run_id>/manifest.json` and records a `runs` row when it generates a script.
- `busco-slurm` now records the expected BUSCO batch root and `batch_summary.txt` path in the run manifest and uses `batch_summary.txt` as the batch-mode completion signal.
- `busco ingest-results` now imports BUSCO `batch_summary.txt` into SQLite `busco_results` as a manual post-run step after the user verifies completion.
- `interproscan-slurm` now writes a launcher script, worker sbatch script, controller script, per-proteome queue ledger, `runs/<run_id>/manifest.json`, and a `runs` row for launcher-based InterProScan execution on staged proteomes.
- `interproscan-slurm` now uses a true submit-and-poll controller: the launcher runs a controller script, the controller submits one worker job at a time with `sbatch --parsable`, records the child job ID in `queue.tsv`, polls that exact job via `squeue`/`sacct`, and only then advances to the next proteome.
- the generated InterProScan worker job now loads Puhti modules with `module load biokit` and `module load interproscan`; `interproscan.bin_dir` is optional and only prepended to `PATH` if explicitly configured.
- `interproscan-slurm` now supports `--limit` to include only the first `N` staged proteomes in the queue for debugging.
- `interproscan-slurm` now constrains the current Puhti wrapper path to a single explicit `TSV` output file per proteome.
- `interproscan-slurm` now supports `--resume-run-id <run_id>` so an existing run can refresh launcher/controller scripts, preserve `queue.tsv`, and resubmit cleanly after timeout/interruption.
- the generated InterProScan launcher now uses the same Python interpreter that wrote the run instead of a bare `python3`, fixing missing-dependency failures during manual `sbatch` resume.
- the broken duplicate path helper in `src/fungalphylo/core/paths.py` was removed.
- README has been updated to the snapshot-first model.
- `fetch-index --ingest-from-cache` no longer requires JGI authentication.
- `restore` now handles non-HTTP per-payload failures under `--continue-on-error`.
- `restore` now retries transient `429`/`5xx`/timeout failures before marking a payload failed.
- `autoselect` now honors config-driven scoring weights and configurable ban patterns, with regression tests.
- `db` now enforces read-only SQL and opens SQLite in read-only mode, with regression tests.
- `taxonomy apply` now updates first-class `portals.ncbi_taxon_id` values from a TSV/CSV/XLSX table, with regression tests and migration coverage for existing databases.
- `taxonomy fetch-ncbi` now downloads and extracts the NCBI `new_taxdump` archive into the project cache.
- `taxonomy busco-mockup` now renders a taxonomy-ordered HTML BUSCO QC report from the latest BUSCO run and local taxdump data.
- `taxonomy busco-mockup` now prefers imported `busco_results` rows or a BUSCO `batch_summary.txt` file over the older single-TSV assumption.
- `restore --dry-run` and `download --dry-run` no longer require JGI authentication when only building payloads.
- `download` now safely writes `unmatched_manifest.tsv` even when the target directory does not already exist.
- restore/download request batches are now indexed in SQLite via `restore_requests` and `download_requests`, and `status` reads from that ledger.
- `download` now retries transient `429`/`5xx`/timeout failures and verifies raw-file `md5` when source metadata provides it.
- `status` now distinguishes checksum-mismatched raw files from missing raw files.
- malformed non-zip download responses and extracted bundles with no manifest are now covered by regression tests and recorded as failed download batches.
- pytest coverage now includes snapshot creation/reuse, cache-only fetch ingest, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, restore/download request-ledger behavior, restore partial-failure handling, and download manifest mismatch/malformed-bundle handling.
- legacy `staged_files` has been removed from the schema in favor of `staging_files`.
- `status` now reports latest snapshot details including artifact counts and reuse counts.
- a real-data project ingested more than 2000 portal rows from a MycoCosm-derived table and fetched files for 1820 published portals.
- `autoselect` performed strongly on that run; among 150 reviewed portals (300 selected files), only one file needed manual correction.
- restore, download, stage, status, and helper commands all worked cleanly in that run after the `restore` bearer-token normalization fix.

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
- BUSCO batch summary import and taxonomy mockup generation
- InterProScan launcher/controller/worker/queue generation
- large real-data intake through `stage`
- InterProScan queue-ledger resume from an existing `run_id`

## Immediate Missing Work

### Reliability

- keep request-ledger rows batch-scoped; use request directories and JSONL as the per-payload forensic record

### Restartability

- validate the manual BUSCO result-import workflow on CSC/Puhti using the validated staged outputs when cluster access is available
- test the upgraded InterProScan controller on Puhti with real `cluster_interproscan` output and verify that one-proteome-at-a-time queueing avoids scheduler limit issues
- confirm the generated module-load path (`biokit`, `interproscan`) and the TSV-only output contract on Puhti
- monitor the current real-data InterProScan validation run and record whether `--resume-run-id` behaves cleanly after real scheduler timeouts

### Maintainability

- centralize repeated timestamp helper logic if it starts spreading further

## Known Technical Debt

- some onboarding docs still have historical notes that should now be rewritten
- `pydantic` is declared but not materially used
- `download` now verifies existing raw files against source `md5` when available; staged-snapshot skips intentionally remain based on approved source file IDs
- restore/download keep only batch rows in SQLite by design; detailed per-payload outcomes live in request directories and JSONL/files

## Suggested Work Order

1. Validate BUSCO script generation, cluster completion, and manual `busco ingest-results` on CSC/Puhti using validated staging outputs.
2. Finish the current real-data InterProScan run and record whether `--resume-run-id` handles timeout recovery cleanly.
3. Confirm the Puhti module-load path and worker-memory behavior, including `--limit` debug runs and GFF3 output.
4. Only then implement additional compute steps.

## Definition Of “Good Enough” For The Next Milestone

- a developer can rerun any completed intake command without guessing what will happen
- staged outputs have an unambiguous lifecycle
- failures in network batches are resumable and inspectable
- the README and onboarding docs match the code
- at least a minimal regression test suite exists
- the first downstream BUSCO handoff, including manual result import, works cleanly from a real staged snapshot
