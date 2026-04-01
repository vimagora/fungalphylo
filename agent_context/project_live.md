# Project Live: fungalphylo

This is the working status board for developers. It is intentionally short and operational.

## Current State

- Intake workflow is implemented and validated on real data through `stage`.
- BUSCO and InterProScan SLURM pipelines are fully validated on Puhti, including `--resume-run-id` for both.
- The `protsetphylo` sub-pipeline is implemented: `init` → `interproscan` → `select` → `orthofinder-slurm` → `og-report` → `og-apply` → `place-standalone` → `phylo-slurm`.
- OrthoFinder 2.5.5 via Tykky container validated on Puhti with 149 proteomes (mfs_sugar family).
- `orthofinder-slurm` uses `env_path` (PATH export) instead of `env_activate` (source). Default: `-M msa`. `--og-only` adds `-os`.
- `filter-orthogroups` selects single-copy OGs (≥75% threshold).
- `phylo-slurm` uses cap-and-resubmit orchestrator + worker with step-level resume. Handles any number of OGs within Puhti array limits.
- `protsetphylo phylo-slurm` uses chained SLURM array jobs (align→trim→tree) with `--dependency=afterok:`, per-step resource allocation, MAFFT `--auto`, and `--iqtree-fast` option.
- `og-report` supports `--orientation horizontal|vertical` and uses `portal_id|protein_name` for characterized gene labels.
- `og-apply` parses include/merge decisions and writes OG FASTAs.
- `place-standalone` generates HMM-based placement SLURM script for standalone characterized genes.
- `protsetphylo init` supports `--force` to overwrite existing family.
- `select` writes standalone characterized genes to `selected/standalone/` and guards against BLAST replacement collisions.
- All 143 tests pass. Code compiles with `python -m compileall src`.

## Recently Completed (2026-04-01 session)

- Refactored `phylo-slurm` to cap-and-resubmit pattern: orchestrator script computes pending OGs (checks for `.treefile`), caps array to `--max-array-size` (380), submits worker. Worker has step-level resume (skips steps whose output exists). Solves Puhti's 400-job array limit for large OG sets (e.g., 2690 OGs).
- Created `protsetphylo phylo-slurm`: chained SLURM array jobs with `--dependency=afterok:` for align→trim→tree. Per-step resource allocation (align: 8cpu/4G/4h, trim: 1cpu/4G/15min, tree: 8cpu/4G/8h). MAFFT `--auto`, IQ-TREE `-m TEST`, optional `--iqtree-fast`. Same cap-and-resubmit orchestrator.
- Updated presentation slides with all current pipeline state + next steps.
- 143 tests pass (6 new for phylo-slurm, 5 new for protsetphylo phylo-slurm).

## Previously Completed (2026-03-30 session)

- Switched OrthoFinder from v3 to v2.5.5 via Tykky container (STRIDE bug in v3 crashes species tree inference).
- `orthofinder-slurm`: `env_activate` → `env_path` (PATH export), `-M dendroblast` → `-M msa -os` for `--og-only`, `-M msa` for full.
- `og-report`: characterized gene columns now use `portal_id|protein_name` when portal is available, else `short_name|protein_name`. Added `--orientation horizontal|vertical`.
- `protsetphylo init`: added `--force` flag to delete existing family directory and DB row before re-initializing.
- Fixed Zigrou/Zygrou typo was user data issue (170→171 characterized genes now all accounted for).
- All tests updated for new OrthoFinder behavior.

## Previously Completed (2026-03-18 session)

- `filter-orthogroups` command: reads `Orthogroups.GeneCount.tsv`, selects OGs with ≥75% single-copy species.
- `phylo-slurm` command: SLURM array job for MAFFT → trimAl → IQ-TREE per OG (380 max concurrent).
- `og-report` and `og-apply` commands for gene family OG inspection and selection.
- `place-standalone` command for HMM-based placement of standalone sequences into OGs.
- `select` standalone fix: characterized genes without portal_id go to `selected/standalone/`.
- BLAST replacement guard: `already_replaced` set prevents two characterized genes overwriting each other.
- OrthoFinder `-o` flag fix and memory scaling (4G/<60 proteomes, 8G/≥60).
- Presentation slides in `presentation/fungalphylo_workflow.md`.

## Previously Completed (2026-03-21 session)

- `orthofinder-slurm` command implemented with full input flexibility.
- OrthoFinder v3 validated on Puhti with 40 proteomes.

## Previously Completed (2026-03-19 session)

- `stage` internal stop codon handling. Cache key schema v2.
- `protsetphylo select` rewritten with BLAST integration.
- MAFFT/trimAl configurable parameters. Cluster splitting in `build-fasta`.

## What Is Working

- Full data intake track: init → ingest → fetch-index → autoselect → review → restore → download → stage
- BUSCO and InterProScan SLURM pipelines (validated on Puhti, resume works)
- OrthoFinder 2.5.5 via Tykky (validated on Puhti with 149 proteomes)
- `filter-orthogroups` for single-copy OG selection
- `phylo-slurm` for parallel gene tree inference (cap-and-resubmit, step-level resume)
- `protsetphylo phylo-slurm` for chained per-step array jobs on gene families
- Full `protsetphylo` pipeline: init → interproscan → select → orthofinder-slurm → place-standalone → og-report → og-apply → phylo-slurm
- Quick path: build-fasta → align → tree (MMseqs2/CD-HIT clustering)

## Immediate Next Work

### Production runs on Puhti

- mfs_sugar family: OrthoFinder 2.5.5 run completed, og-report generated, place-standalone validated. Ready for og-apply and protsetphylo phylo-slurm.
- Species tree: filter-orthogroups done (2690 OGs), ready for phylo-slurm with new cap-and-resubmit pattern.

### Commit

- Commit all accumulated v0.2 changes after validation.

## Known Technical Debt

- Some onboarding docs still have historical notes that should be rewritten.
- restore/download keep only batch rows in SQLite by design; detailed per-payload outcomes live in request directories and JSONL/files.

## Definition Of "Good Enough" For The Next Milestone

- a developer can rerun any completed intake command without guessing what will happen
- staged outputs have an unambiguous lifecycle
- failures in network batches are resumable and inspectable
- the README and onboarding docs match the code
- at least a minimal regression test suite exists
- the full protsetphylo pipeline from `init` through `tree` works cleanly on at least one real gene family
