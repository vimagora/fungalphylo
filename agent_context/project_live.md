# Project Live: fungalphylo

This is the working status board for developers. It is intentionally short and operational.

## Current State

- Intake workflow is implemented and validated on real data through `stage`.
- BUSCO and InterProScan SLURM pipelines are fully validated on Puhti, including `--resume-run-id` for both.
- The `protsetphylo` sub-pipeline is implemented: `init` → `interproscan` → `select` → `build-fasta` → `align` → `tree`.
- `protsetphylo init`, `interproscan`, `select`, and `build-fasta` are validated on real data.
- `protsetphylo align` and `tree` generate SLURM scripts with configurable MAFFT/trimAl parameters.
- `protsetphylo tree` supports both FastTree (quick exploratory) and IQ-TREE (refined).
- `stage` now handles internal stop codons with three modes: `drop` (default), `warn`, `strip`.
- `select` integrates characterized proteins via BLAST (replace best hit or append) and writes standalone species FASTAs.
- `selected/` directory is the single source of truth for per-species FASTAs, ready for OrthoFinder input.
- `orthofinder-slurm` command implemented: supports full proteomes (`--staging-id`), family selections (`--family-id`), or custom dirs (`--input-dir`). `--og-only` uses `-M dendroblast` to skip MSA/gene trees. Validated on Puhti with 40 proteomes.
- All 111 tests pass. Code compiles with `python -m compileall src`.

## Recently Completed (2026-03-21 session)

- `orthofinder-slurm` command implemented with full input flexibility: `--staging-id` (default latest), `--family-id` (uses `selected/`), `--input-dir` (explicit).
- `--og-only` flag uses `-M dendroblast` to skip MSA/gene tree inference. Orthogroups are identical to MSA mode (MCL clustering happens before MSA step).
- OrthoFinder v3 validated on Puhti with 40 proteomes: DIAMOND + MCL orthogroup inference works. MSA-based gene trees (`--localpair --maxiterate 1000`) crash on large orthogroups — `--og-only` is the recommended path.
- `OrthoFinderTool` dataclass added to `core/tools.py` with `env_activate`, `command`, `msa_program`.
- Generated SLURM script handles Puhti environment: `module purge` → `StdEnv` → `python-data` → `source env_activate` → `module load <msa_program>`.
- Resume support via `--resume-run-id` passes `-b <WorkingDirectory>` to reuse DIAMOND results.
- 111 tests pass (11 for orthofinder-slurm).

## Previously Completed (2026-03-19 session)

- `stage` internal stop codon handling (`--internal-stop`: drop/warn/strip). Cache key schema v2.
- `protsetphylo select` rewritten with BLAST integration for characterized proteins.
- `selected/` as single source of truth for per-species FASTAs (OrthoFinder-ready).
- MAFFT/trimAl configurable parameters in `align`. `--input-fasta`/`--input-alignment` overrides.
- Cluster splitting in `build-fasta` with `cluster_summary.tsv`.

## Previously Completed

- BUSCO and InterProScan fully validated on Puhti with real data (150 proteomes). Both `--resume-run-id` features work correctly including InterProScan recovery of failed sequences.
- `protsetphylo init` and `protsetphylo interproscan` validated on real data (mfs_sugar family, PF00083).
- `protsetphylo select` validated on real data: 7441 proteins selected from project-wide InterProScan results.
- Fixed `resolve_staging_id` to exclude `__family__` sentinel rows.
- `protsetphylo build-fasta` preserves `combined.pre_dedup.faa` and `cluster_members.tsv` when using redundancy removal.
- CD-HIT `.clstr` files converted to the same `cluster_members.tsv` format as MMseqs2 output.
- `tools.py` supports `bin_dir` for all tools: mafft, trimal, iqtree, fasttree, blast.
- `align.py` and `tree.py` use `bin_dir` PATH exports when configured, falling back to `module load`.
- Immutable staging snapshots, artifact reuse by cache key, BUSCO/InterProScan SLURM generation.
- Restart contract documented for all implemented commands.
- pytest: 100 tests covering staging, cache-ingest, autoselect, db enforcement, FASTA I/O, ID maps, raw paths, restore/download, and protsetphylo commands.

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
- `protsetphylo align` — MAFFT + trimAl SLURM script generation
- `protsetphylo tree` — IQ-TREE or FastTree SLURM script generation
- `orthofinder-slurm` — OrthoFinder on full proteomes or family selections (validated on Puhti with `--og-only`)

## Immediate Next Work

### OrthoFinder production run

- Run `orthofinder-slurm --og-only` on full 150 proteomes via the pipeline command.
- Inspect orthogroups: single-copy orthogroups, gene counts per species.
- For gene families: pick orthogroups of interest, align + tree each with `protsetphylo align` + `tree`.

### Protsetphylo pipeline

- Re-run protsetphylo pipeline from scratch on mfs_sugar with BLAST-integrated select.
- Run OrthoFinder on `selected/` per-species FASTAs (`--family-id mfs_sugar --og-only`).

### Commit

- Commit all accumulated v0.2 changes after validation.

## Known Technical Debt

- some onboarding docs still have historical notes that should now be rewritten
- `download` now verifies existing raw files against source `md5` when available; staged-snapshot skips intentionally remain based on approved source file IDs
- restore/download keep only batch rows in SQLite by design; detailed per-payload outcomes live in request directories and JSONL/files

## Suggested Work Order

1. Run `orthofinder-slurm --og-only` on full 150 proteomes for species-level orthogroup inference.
2. Inspect orthogroups: single-copy orthogroups for species tree, multi-copy for gene family analysis.
3. Re-run protsetphylo pipeline on mfs_sugar from scratch, then run OrthoFinder on `selected/` with `--family-id`.
4. Pick orthogroups of interest (containing characterized proteins), align + tree each.
5. Validate pipeline end-to-end on a second gene family.
6. Commit all v0.2 changes.

## Definition Of "Good Enough" For The Next Milestone

- a developer can rerun any completed intake command without guessing what will happen
- staged outputs have an unambiguous lifecycle
- failures in network batches are resumable and inspectable
- the README and onboarding docs match the code
- at least a minimal regression test suite exists
- the full protsetphylo pipeline from `init` through `tree` works cleanly on at least one real gene family
