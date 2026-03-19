# Project Live: fungalphylo

This is the working status board for developers. It is intentionally short and operational.

## Current State

- Intake workflow is implemented and validated on real data through `stage`.
- BUSCO and InterProScan SLURM pipelines are fully validated on Puhti, including `--resume-run-id` for both.
- The `protsetphylo` sub-pipeline is implemented: `init` → `interproscan` → `select` → `build-fasta` → `align` → `tree`.
- `protsetphylo init`, `interproscan`, `select`, and `build-fasta` are validated on real data.
- `protsetphylo align` (MAFFT + trimAl) is currently running on Puhti for the `mfs_sugar` family (~7441 sequences).
- `protsetphylo tree` supports both FastTree (quick exploratory) and IQ-TREE (refined).
- The current pytest suite covers staging snapshots, cache-ingest behavior, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, and protsetphylo select/build-fasta/align/tree commands.
- Code compiles with `python -m compileall src`.

## Recently Completed

- BUSCO and InterProScan fully validated on Puhti with real data (150 proteomes). Both `--resume-run-id` features work correctly including InterProScan recovery of failed sequences.
- `protsetphylo init` and `protsetphylo interproscan` validated on real data (mfs_sugar family, PF00083).
- `protsetphylo select` validated on real data: 7441 proteins selected from project-wide InterProScan results.
- Fixed `resolve_staging_id` to exclude `__family__` sentinel rows — was causing `select` to resolve to wrong staging snapshot.
- `protsetphylo build-fasta` now preserves `combined.pre_dedup.faa` (pre-dedup FASTA) and `cluster_members.tsv` (cluster membership traceback) when using redundancy removal (MMseqs2 or CD-HIT).
- CD-HIT `.clstr` files are now converted to the same `cluster_members.tsv` format as MMseqs2 output.
- `tools.py` now supports `bin_dir` for all tools: mafft, trimal, iqtree, and new `fasttree` tool.
- `align.py` and `tree.py` now use `bin_dir` PATH exports when configured, falling back to `module load` only when no `bin_dir` is set.
- `tools.yaml` template updated to include mafft, trimal, fasttree, and iqtree entries with `bin_dir` support.
- the implemented command restart contract is now documented in `agent_context/restart_contract.md`.
- `stage` now writes snapshot-scoped artifacts under `staging/<staging_id>/`.
- snapshot artifact metadata is now recorded in `staging_files`.
- stage artifact reuse now uses a cache key instead of the old mutable-output assumption.
- `busco-slurm` can target a chosen `staging_id` and defaults to the latest snapshot.
- `busco-slurm` now writes `runs/<run_id>/manifest.json` and records a `runs` row when it generates a script.
- `busco ingest-results` now imports BUSCO `batch_summary.txt` into SQLite `busco_results` as a manual post-run step.
- `interproscan-slurm` now uses a true submit-and-poll controller with `--resume-run-id` support.
- `restore --dry-run` and `download --dry-run` no longer require JGI authentication when only building payloads.
- restore/download request batches are now indexed in SQLite via `restore_requests` and `download_requests`.
- pytest coverage includes snapshot creation/reuse, cache-only fetch ingest, autoselect scoring/config behavior, db read-only enforcement, FASTA roundtrips, ID map loading, raw path resolution, restore/download request-ledger behavior, restore partial-failure handling, download manifest mismatch/malformed-bundle handling, and protsetphylo command coverage.

## What Is Working

- project initialization
- portal/file ingest from TSV/CSV/XLSX
- JGI file index fetch and cache
- candidate autoselection with explain TSV
- review export/apply loop
- restore payload generation and posting
- download bundle extraction into `raw/`
- staging into canonical per-portal FASTA files
- BUSCO sbatch script generation, batch run, resume, and result import (validated on Puhti)
- BUSCO batch summary import and taxonomy mockup generation
- InterProScan launcher/controller/worker/queue generation and resume (validated on Puhti)
- large real-data intake through `stage`
- `protsetphylo init` — family initialization with characterized proteins and Pfam accessions
- `protsetphylo interproscan` — InterProScan on characterized proteins (validated on Puhti)
- `protsetphylo select` — protein selection from project proteomes by Pfam/e-value/architecture (validated)
- `protsetphylo build-fasta` — merge characterized + selected, optional MMseqs2/CD-HIT redundancy with traceback
- `protsetphylo align` — MAFFT + trimAl SLURM script generation (currently running on Puhti)
- `protsetphylo tree` — IQ-TREE or FastTree SLURM script generation

## Immediate Next Work

### Protsetphylo validation

- Check alignment results for mfs_sugar family when the SLURM job completes
- Run `protsetphylo tree --tree-method fasttree` for a quick exploratory tree of the full 7441-sequence family
- Inspect tree to identify subfamilies/clades, decide which to build refined IQ-TREE trees for
- Set up FastTree and IQ-TREE 3 binaries on Puhti (`bin_dir` in `tools.yaml`)

### Reliability

- keep request-ledger rows batch-scoped; use request directories and JSONL as the per-payload forensic record

### Maintainability

- centralize repeated timestamp helper logic if it starts spreading further

## Known Technical Debt

- some onboarding docs still have historical notes that should now be rewritten
- `download` now verifies existing raw files against source `md5` when available; staged-snapshot skips intentionally remain based on approved source file IDs
- restore/download keep only batch rows in SQLite by design; detailed per-payload outcomes live in request directories and JSONL/files

## Suggested Work Order

1. Check mfs_sugar alignment results and build a FastTree exploratory tree.
2. Inspect tree, identify subfamilies, and build refined IQ-TREE trees per subfamily.
3. Consider whether a `protsetphylo split-tree` or similar command would help automate subfamily extraction.
4. Validate the full protsetphylo pipeline end-to-end on a second gene family.
5. Only then consider OrthoFinder or species-tree commands if still needed.

## Definition Of "Good Enough" For The Next Milestone

- a developer can rerun any completed intake command without guessing what will happen
- staged outputs have an unambiguous lifecycle
- failures in network batches are resumable and inspectable
- the README and onboarding docs match the code
- at least a minimal regression test suite exists
- the full protsetphylo pipeline from `init` through `tree` works cleanly on at least one real gene family
