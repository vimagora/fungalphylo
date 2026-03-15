# CODEX

Read this first when resuming work on `fungalphylo`.

## Session Onboarding

If you are a new agent session, do this in order before making changes:

1. Read `agent_context/project_master.md`.
   Use it for the durable technical overview: implemented workflow, architecture, reliability concerns, and the active backlog.
2. Read `agent_context/project_live.md`.
   Use it for the current working state: recently completed work, known debt, and the intended next work order.
3. Read `docs/restart_contract.md`.
   Use it as the source of truth for rerun, skip, and completion semantics of implemented commands.
4. Read the code for the commands you are likely to touch before trusting any older note or README text:
   - `src/fungalphylo/cli/commands/stage.py`
   - `src/fungalphylo/cli/commands/download.py`
   - `src/fungalphylo/cli/commands/restore.py`
   - `src/fungalphylo/cli/commands/fetch_index.py`
   - `src/fungalphylo/cli/commands/status.py`
   - `src/fungalphylo/core/paths.py`
   - `src/fungalphylo/db/schema.sql`
5. Inspect the current tests before changing behavior:
   - `tests/test_restore_download.py`
   - `tests/test_stage_snapshots.py`
   - `tests/test_fetch_index_cache.py`
   - `tests/test_autoselect.py`
   - `tests/test_db_command.py`
   - `tests/test_core_utils.py`

Behavior rules for a fresh session:

- Trust the current code over the README or older notes when they conflict.
- Treat `docs/restart_contract.md` as the operational contract for implemented commands.
- Treat `staging/<staging_id>/...` as the immutable artifact model.
- Assume restartability and durable local state matter more than adding new surface area.
- Before editing behavior, verify whether tests already describe that behavior.

If you need to understand where truth lives:

- workflow/architecture: `agent_context/project_master.md`
- current status and next work: `agent_context/project_live.md`
- rerun/skip semantics: `docs/restart_contract.md`
- filesystem layout: `src/fungalphylo/core/paths.py`
- durable state model: `src/fungalphylo/db/schema.sql`
- command behavior: `src/fungalphylo/cli/commands/`
- regression coverage: `tests/`

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
- `busco-slurm` now writes a run manifest under `runs/<run_id>/manifest.json` and records a `runs` ledger row when generating the SLURM script.
- `busco-slurm` now records the expected BUSCO batch root and `batch_summary.txt` path in the run manifest and uses `batch_summary.txt` as the batch-mode completion signal.
- `busco ingest-results` now imports BUSCO `batch_summary.txt` rows into SQLite `busco_results`; this is a manual post-run step after the user confirms the cluster job completed successfully.
- `interproscan-slurm` now writes a launcher script, worker sbatch script, controller script, per-proteome queue ledger, run manifest, and `runs` ledger row for launcher-based InterProScan execution on staged proteomes.
- `interproscan-slurm` now uses a true submit-and-poll controller that records child job IDs in `queue.tsv` and advances one proteome at a time.
- `interproscan-slurm` now loads Puhti modules (`biokit`, `interproscan`) inside the worker job before running `cluster_interproscan`; `interproscan.bin_dir` is optional.
- `interproscan-slurm` now supports `--limit` for debug-sized runs and, on the current Puhti wrapper path, constrains output to a single explicit `TSV` file per proteome.
- `interproscan-slurm` now supports `--resume-run-id <run_id>` to continue an existing run in place without rewriting `queue.tsv`; this is the safe resume path after launcher timeout/interrupts.
- the generated InterProScan launcher now uses the same Python interpreter that wrote the run, avoiding failures from a bare `python3` lacking project dependencies.
- In this local development environment, compute commands should default to writing SLURM scripts only; keep explicit submit support in code, but do not rely on live Puhti submission during development or tests.
- `autoselect` now honors config-driven scoring weights and configurable ban patterns.
- `db` now enforces read-only SQL and opens SQLite in read-only mode.
- `taxonomy apply` now updates first-class `portals.ncbi_taxon_id` values from a user-provided table.
- `taxonomy fetch-ncbi` now downloads the NCBI `new_taxdump` archive into the project cache.
- `taxonomy busco-mockup` now renders a taxonomy-ordered HTML BUSCO QC report from the latest BUSCO run plus local taxdump data.
- `taxonomy busco-mockup` now prefers imported `busco_results` rows or BUSCO `batch_summary.txt` over the older single-TSV assumption.
- `restore --dry-run` and `download --dry-run` now build payloads without requiring JGI authentication.
- `download` now safely creates `unmatched_manifest.tsv` even when the kept-manifest directory does not yet exist.
- `download` now retries transient `429`/`5xx`/timeout failures and verifies raw-file `md5` when source metadata provides it.
- restore/download batch directories are now indexed in SQLite via `restore_requests` and `download_requests`.
- download failure paths for malformed non-zip responses and missing manifest files are now covered by tests and recorded as failed batches.
- compute commands beyond BUSCO and InterProScan are absent.
- the pytest suite now covers snapshot creation/reuse, cache-only fetch ingest, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, restore/download request-ledger writes, restore dry-run/continue-on-error behavior, and download manifest mismatch/malformed-bundle handling.
- legacy `staged_files` has been removed from the schema; `staging_files` is the active staging artifact table.
- a real-data run has now validated the workflow from `init` through `stage` on a large MycoCosm-derived project.
- the validated run ingested more than 2000 portal rows, fetched files from 1820 published portals, and reviewed/selected 150 portals (300 files).
- `autoselect` performed strongly in that run, requiring only one file correction across the reviewed set.
- restore, download, stage, status, and helper commands all worked cleanly in the validated run after the `restore` bearer-token fix.

## Files To Read First

1. `agent_context/project_master.md`
2. `agent_context/project_live.md`
3. `docs/restart_contract.md`
4. `src/fungalphylo/cli/commands/stage.py`
5. `src/fungalphylo/core/paths.py`
6. `src/fungalphylo/cli/commands/download.py`
7. `src/fungalphylo/cli/commands/restore.py`
8. `src/fungalphylo/cli/commands/fetch_index.py`

## Main Architectural Question

The artifact model decision is made: immutable snapshot artifacts live under `staging/<staging_id>/...`.

The active implementation work is to make every relevant command and document honor that decision.

## High-Priority Known Issues

- some onboarding docs still contain historical pre-refactor notes
- restore/download are now batch-tracked in SQLite, but the remote-side lifecycle is still not modeled beyond local batch outcomes
- `download` now verifies raw-file `md5` when source metadata provides it, but staged-snapshot skips are still source-file-ID based
- the explicit restart contract now lives in `docs/restart_contract.md`; keep that file aligned with command behavior
- the next practical milestone is to validate the manual BUSCO result-import workflow and the upgraded InterProScan controller on CSC/Puhti
- a real-data InterProScan validation run on CSC/Puhti is currently in progress; preserve its `run_id` and `queue.tsv` if additional resume/debugging is needed

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
