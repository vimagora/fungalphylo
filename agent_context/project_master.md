# Project Master: fungalphylo

This document is based on reading the current codebase first and the prior onboarding note second. It is intended to be a durable technical orientation for developers working on reliability, restartability, lightweight operation, and maintainability.

## 1. Executive Summary

`fungalphylo` is a small Python CLI project for fungal phylogenomics data intake and normalization. The implemented workflow is:

1. `init`
2. `ingest`
3. `fetch-index`
4. `autoselect`
5. `review export` / manual edit / `review apply`
6. `restore`
7. `download`
8. `stage`
9. `busco-slurm`

The codebase is not large, but it already contains the right primitives for a durable pipeline:

- SQLite state for portals, files, approvals, stagings, and snapshot artifacts
- structured event/error logging
- raw download preservation
- manifest writing
- portal-level skip logic during staging
- human review through TSVs

The main problem is not missing architecture. It is drift:

- docs do not fully match code
- path semantics had drifted (`staging/` vs `staged/`); the code is now being moved to `staging/<staging_id>/` as the source of truth
- some restart behavior is real, but only at selected boundaries
- tests now exist, but coverage is still minimal

The project is viable, but before expanding compute steps it needs consolidation around a single execution model and a documented restart contract.

The current documented restart contract for implemented commands lives in `docs/restart_contract.md`.

A full real-data validation run has now completed successfully through `stage`, which materially reduces uncertainty around the intake side of the project.

## 2. What Exists Today

### Package layout

- `src/fungalphylo/cli/main.py`
  - registers the user-facing CLI
- `src/fungalphylo/cli/commands/`
  - implemented commands: `init`, `ingest`, `fetch_index`, `autoselect`, `review`, `restore`, `download`, `stage`, `status`, `idmap`, `busco_slurm`, `db`
- `src/fungalphylo/core/`
  - path handling, config, FASTA I/O, id mapping, manifests, events/errors, hashing, validation
- `src/fungalphylo/db/`
  - SQLite schema and a small query layer

### Actual workflow semantics

- `init`
  - creates project directories, `config.yaml`, `tools.yaml`, and SQLite DB
- `ingest`
  - reads TSV/CSV/XLSX portal tables
  - can infer `portal_id` from MycoCosm hyperlinks
  - can also ingest file rows when file columns are present
- `fetch-index`
  - fetches JGI Files search JSON per portal
  - caches raw JSON
  - upserts `portal_files`
  - stores `dataset_id` and `top_hit_id` on `portals`
- `autoselect`
  - ranks candidate proteome/CDS files and writes explainable review TSVs
- `review`
  - exports an editable TSV and applies user decisions into `approvals`
- `restore`
  - chunks restore payloads and writes request payloads to disk
- `download`
  - chunks download payloads
  - downloads ZIP bundles
  - extracts via manifest and moves source files into `raw/<portal>/<file_id>/<filename>`
- `stage`
  - normalizes approved raw FASTA inputs into canonical per-portal files
  - writes outputs under `staging/<staging_id>/proteomes/` and `staging/<staging_id>/cds/`
  - writes generated protein ID maps under `staging/<staging_id>/idmaps/generated/`
  - records snapshot-scoped artifact metadata in `staging_files`
  - reuses equivalent prior artifacts by cache key instead of regenerating them
- `busco-slurm`
  - writes a Puhti-oriented SLURM script to run BUSCO on a chosen `staging_id`

### Database shape

The schema is intentionally lean:

- `portals`
- `portal_files`
- `approvals`
- `stagings`
- `runs`
- `staging_files`
- `meta`

This is a good fit for a lightweight CLI project. The schema is not the current bottleneck.

## 3. Strengths

### Durable local state

SQLite is the correct choice here. It keeps the project self-contained, queryable, and easy to back up.

### Explainable human review

`autoselect` plus editable TSV review is a strong UX choice for real bioinformatics work, where full automation is rarely trusted.

### Raw artifact preservation

`download` keeps original files in `raw/` and defers normalization to `stage`. That is the right separation for auditability and future reprocessing.

### Structured logging

`logs/events.jsonl` and `logs/errors.jsonl` are simple and operationally useful. This is appropriate for HPC use where lightweight logs matter.

### Non-JGI header support

The `idmap` and `stage` path for non-JGI FASTA headers is already pragmatic and aligns with real-world messy datasets.

## 4. Code-Grounded Risks And Gaps

These are the main issues visible from the current implementation.

### 4.1 Path model drift

`src/fungalphylo/core/paths.py` previously mixed three concepts:

- immutable staging snapshots under `staging/`
- generated ID maps under `idmaps/generated/`

The duplicate `staged_proteome_dir` bug has been removed. The remaining task is to keep downstream commands and docs aligned with the snapshot-first model.

### 4.2 Snapshot-first staging is now the target model

The code now writes the normalized FASTA artifacts, generated ID maps, reports, manifest, and checksums under `staging/<staging_id>/...`.

The remaining work is to finish propagating that model through the rest of the workflow and documentation.

### 4.3 Restart behavior exists, but only in fragments

The project already has useful skip behavior:

- `fetch-index` can reuse cached JSON
- `download` can skip if raw files exist or if approved file IDs are already represented in staging snapshots
- `stage` can reuse equivalent artifacts by cache key across snapshots
- `markers.py` provides `STARTED`/`DONE` helpers for future run-level checkpointing

The restart contract is now documented in `docs/restart_contract.md`, but a few implementation boundaries remain open:

- `stage` reuse is now keyed by a staging artifact cache key derived from source checksum and parameters, but the cache contract still needs broader tests
- `download` now verifies existing raw files against source `md5` when that metadata is available, but otherwise falls back to path existence
- `download` now retries transient `429`/`5xx`/timeout failures before marking a payload failed
- `restore` now retries transient `429`/`5xx`/timeout failures before marking a payload failed
- `restore` and `download` now have batch-level request tracking in SQLite by design; per-payload detail remains in request directories and JSONL/files rather than child tables
- compute commands are mostly not implemented, so run markers are not yet part of the real execution path

Recent improvements in this area:

- `restore --dry-run` and `download --dry-run` now work as preflight/payload-building steps without requiring authentication
- `restore` continues posting later payloads under `--continue-on-error`, with tested per-payload error logging
- `download` records manifest mismatches in `unmatched_manifest.tsv`, and the directory-creation path for that report is now covered by tests
- restore/download batch summaries are now indexed in SQLite (`restore_requests`, `download_requests`) while filesystem request directories remain the source of truth
- malformed non-zip download responses and extracted bundles with no manifest are now covered by command-level tests and recorded as failed batches

### 4.4 CLI and docs do not fully agree

Examples:

- README is now closer to the current CLI, but some onboarding notes still lag behind
- README and some onboarding docs still need to be updated to the new snapshot-first behavior

This is manageable, but it creates onboarding confusion and makes maintenance harder.

### 4.5 Tests are now present but still minimal

A focused `tests/` directory now exists and covers:

- snapshot creation
- snapshot artifact reuse
- cache-only `fetch-index` ingest without a token
- autoselect scoring/config behavior
- db read-only enforcement
- FASTA parser/writer roundtrips
- ID map loading
- raw path resolution
- restore dry-run and continue-on-error behavior
- download manifest mismatch and malformed-bundle handling

This is a meaningful improvement, but long-running network and manifest edge cases still need more command-level coverage.

### 4.6 A few concrete command-level issues

- `download` intentionally decides staged-snapshot skips using approved source file IDs rather than checksum-aware raw state.
- `restore` and `download` now record batch-level request history in SQLite, while detailed per-payload outcomes live in request directories and JSONL/files.

### 4.7 Some dependencies and abstractions are ahead of reality

- `pydantic` is listed but not meaningfully used in the current code
- `runs` table and `markers.py` suggest a future cached compute layer that is not yet implemented
- README mentions Snakemake-backed compute, but there is no active compute integration beyond BUSCO script generation

This is not a flaw by itself, but it should be kept under control so the project stays lightweight.

## 5. Reliability Assessment

Current reliability is strongest in the intake and normalization phases, provided the user follows the intended workflow.

Most reliable areas:

- tabular ingest
- SQLite-backed approvals
- raw file retention
- deterministic FASTA rewriting for staged outputs
- the full intake path from `init` through `stage` has now been exercised successfully on a large real-data project

Real-data validation currently on record:

- more than 2000 portal rows ingested from a MycoCosm-derived source table
- files fetched for 1820 published portals
- 150 portals reviewed and selected, corresponding to 300 approved files
- only one manual file correction needed after `autoselect`
- restore, download, stage, status, and helper commands all worked cleanly after fixing restore token normalization

Least reliable areas:

- remaining long-running command semantics outside the newly-tested paths
- broader regression coverage
- downstream BUSCO script generation/submission has not yet received the same real-data validation
- residual drift between implementation and older onboarding notes

## 6. Maintainability Assessment

The codebase is small enough that maintainability can improve quickly if a few conventions are enforced:

- one canonical path model
- one canonical restart model
- one source of truth for workflow documentation
- tests around pure logic and representative command flows

Without that, new features will increase confusion faster than they add value.

## 7. Recommended Technical Direction

### Priority 1: complete the snapshot-first model

The artifact-model decision is made:

1. immutable snapshots under `staging/<staging_id>/...`
2. compute steps consume an explicit `staging_id`

The remaining question is whether any compatibility layer around the old `staged/` concept should remain.

### Priority 2: define restartability at each boundary

The baseline contract is now written down. The remaining work is to keep code, docs, and status surfaces aligned on it. For every command, preserve clarity on:

- what inputs define “same work”
- what durable record proves completion
- when the command skips
- when the command must rerun

For `restore` and `download`, keep SQLite at the batch-ledger level unless payload-level resume becomes a concrete requirement. The current intended boundary is:

- SQLite for discovery and high-level status
- request directories plus JSONL/files for payload-level evidence

This should stay aligned in both the docs and the code.

### Priority 3: add a minimal test suite

Start with pure and near-pure units:

- `classify_kind`
- `score_candidate`
- `load_id_map`
- `resolve_raw_path`
- FASTA parser/writer roundtrips
- stage header normalization on small fixtures

Then add one or two command-level integration tests with temp project directories and mocked JGI responses.

### Priority 4: tighten command behavior before adding new compute

Before implementing OrthoFinder/species-tree/family runs:

- expand regression coverage
- align the remaining onboarding docs

### Priority 5: keep the project lightweight

Avoid premature orchestration layers. The current combination of:

- Typer
- SQLite
- JSON/TSV manifests
- plain files

is enough for this phase. Extra frameworks should only be added if they reduce maintenance burden, not because they are “standard”.

## 8. Puhti / CSC Considerations

The current code is broadly compatible with Puhti-style usage:

- file-based state is local to a project directory
- SQLite is appropriate for single-user or low-contention workflows
- SLURM integration begins with `busco-slurm`
- logs and manifests are lightweight
- local development should write SLURM scripts for review by default; keep submit code paths available, but do not depend on live Puhti submission outside CSC

Operational recommendations for CSC environments:

- keep project directories under scratch/project space
- ensure long-running network steps write durable payloads and summaries before posting
- avoid hidden reliance on interactive prompts except where explicitly desired
- prefer deterministic output paths and checksum recording for all compute steps

## 9. Immediate Backlog

These are the most important short-term tasks.

1. Review `busco-slurm` against the current snapshot-first staging model and update any stale path or argument assumptions.
2. Add regression coverage for BUSCO script generation and the optional submit code path without requiring live Puhti access.
3. Test BUSCO script generation and submission on CSC/Puhti from a validated `staging_id` when cluster access is available.
3. Keep the documented command surface limited to implemented workflow steps until new compute commands actually exist.

## 10. Practical Guidance For New Developers

- Trust the code over the README when they disagree.
- Treat `stage` as the central command for current reproducibility work.
- Be cautious when changing path conventions; many commands assume exact layouts.
- Prefer adding tests around pure helper functions before touching network commands.
- Avoid broad refactors until the staging model is explicitly chosen.

## 11. Verification Performed

This assessment is based on:

- full repository file listing
- reading the current CLI, core, and DB modules
- reading the current `agent_context/project_master.md` after the code review
- running `python -m compileall src` successfully
- running focused pytest coverage for snapshot staging, cache-only fetch ingest, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, restore/download request-ledger behavior, restore partial-failure handling, and download manifest mismatch/malformed-bundle handling
