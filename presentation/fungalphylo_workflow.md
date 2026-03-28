---
title: "fungalphylo: A Reproducible Fungal Phylogenomics Pipeline"
author: "Victor Manuel Gonzalez Ramos"
date: "2026"
---

# What is fungalphylo?

- Python CLI for fungal phylogenomics data intake and normalization
- Handles JGI/MycoCosm data through to species tree inference
- Built for CSC/Puhti HPC environment
- Key design: SQLite state tracking, immutable snapshots, SLURM script generation

---

# Pipeline Overview

Two main tracks:

**Data Intake**

```
init -> ingest -> fetch-index -> autoselect -> review -> restore -> download -> stage
```

**Compute**

```
busco-slurm -> interproscan-slurm -> orthofinder-slurm -> filter-orthogroups
```

---

# Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

- Requires Python 3.11+
- Dependencies: typer, pyyaml, openpyxl, requests, rich

---

# Project Structure

Every project is a self-contained directory (separate from code):

```
project/
  config.yaml              # Project configuration
  tools.yaml               # External tool paths
  db/fungalphylo.sqlite    # State database
  raw/                     # Downloaded originals
  staging/                 # Immutable normalized snapshots
  runs/                    # Compute runs
  review/                  # Human-editable TSVs
  logs/                    # Event and error logs
```

---

# Step 1: Initialize a Project

```bash
fungalphylo init /scratch/project_xxx/myproject
```

- Creates directory structure, config files, and SQLite database
- One project directory per analysis

---

# Step 2: Ingest Portal List

```bash
fungalphylo ingest --table mycocosm_portals.xlsx /path/to/project 
```

- Import a MycoCosm spreadsheet (XLSX)
- Expects hyperlinked `Name` and `Published` columns
- Populates the `portals` and `portal_files` tables

---

# Step 3: Fetch File Index from JGI

```bash
# All published portals
fungalphylo fetch-index --published-only /path/to/project

# Single portal
fungalphylo fetch-index --portal-id Dicsqu464_2 /path/to/project

# From cache only (no network)
fungalphylo fetch-index --ingest-from-cache /path/to/project 
```

- Queries JGI Files API for available files per portal
- Caches JSON responses locally for reruns

---

# Step 4: Auto-select Best Files

```bash
fungalphylo autoselect /path/to/project
```

- Heuristic scoring: prefers genome-group, filtered proteins/CDS, FASTA format
- Avoids deflines, promoters, alleles, GFF files
- Outputs explainable TSVs in `review/`

---

# Step 5: Review and Apply Selections

```bash
# Export editable TSV
fungalphylo review export --from-autoselect review/autoselect_<ts>.tsv /path/to/project  

# Edit the TSV: change file IDs, set approve=no to exclude

# Apply to database
fungalphylo review apply /path/to/project path/to/review_edit_<ts>.tsv
```

- Human-in-the-loop file selection
- Edit in any spreadsheet tool, then apply back

---

# Step 6: Restore Files from JGI

```bash
# Preview
fungalphylo restore --dry-run /path/to/project

# Send restore requests
fungalphylo restore --send-mail /path/to/project
```

- JGI files may be archived (tape storage)
- Restore requests can take hours/days
- Payloads chunked automatically for API limits

---

# Step 7: Download Approved Files

```bash
fungalphylo download /path/to/project

# Skip already-downloaded
fungalphylo download --skip-if-raw-present /path/to/project
```

- Downloads to `raw/<portal_id>/<file_id>/<filename>`
- Preserves original files untouched

---

# Step 8: Stage (Normalize)

```bash
# Preview
fungalphylo stage --dry-run /path/to/project

# Create snapshot
fungalphylo stage /path/to/project
```

- Canonical protein headers: `{portal_id}|{jgi_protein_id}`
- Immutable snapshot: `staging/<staging_id>/proteomes/` and `cds/`
- Each run creates a new snapshot ID

---

# Stage: Internal Stop Codons

| Mode | Behavior |
|------|----------|
| `drop` (default) | Remove sequence entirely |
| `warn` | Keep sequence, count in stats |
| `strip` | Remove internal `*`, keep sequence |

```bash
fungalphylo stage --internal-stop strip /path/to/project 
```

Trailing stop codons are always stripped.

---

# Compute: BUSCO (Quality Control)

```bash
# Generate SLURM script
fungalphylo busco-slurm /path/to/project

# Resume a timed-out run
fungalphylo busco-slurm  --resume-run-id <run_id> --time 48:00:00 --submit /path/to/project

# Import results
fungalphylo busco ingest-results --run-id <run_id> /path/to/project
```

- Assesses proteome completeness
- Use `--submit` only on systems with `sbatch`

---

# Compute: InterProScan (Domain Annotation)

```bash
# Generate launcher + worker scripts
fungalphylo interproscan-slurm --application PfamA /path/to/project 

# Resume after timeout
fungalphylo interproscan-slurm --resume-run-id <run_id> --submit /path/to/project
```

- Submit-and-poll controller (one worker at a time)
- Failed sequences retried automatically on resume

---

# Compute: OrthoFinder (Orthogroup Inference)

```bash
# Orthogroups only (recommended)
fungalphylo orthofinder-slurm --og-only --submit /path/to/project

# Full analysis with MSA-based gene trees (will crash 90% of the time)
fungalphylo orthofinder-slurm --submit /path/to/project

# Resume a timed-out run (reuses DIAMOND results)
fungalphylo orthofinder-slurm --resume-run-id <run_id> --submit /path/to/project
```

---

# OrthoFinder: Key Options

- **`--og-only`**: Uses `-M dendroblast`, skips MSA/gene trees (orthogroups are identical)
- Avoids OrthoFinder v3's aggressive MAFFT calls that crash on large OGs
- Memory scales: 4G/cpu (<60 proteomes), 8G/cpu (>=60 proteomes)
- Requires venv on Puhti -- configure `env_activate` in `tools.yaml`

---

# Filter Orthogroups

```bash
# Default: >=75% of species with exactly 1 copy
fungalphylo filter-orthogroups /path/to/project

# Custom threshold
fungalphylo filter-orthogroups --min-single-copy 0.80 /path/to/project 

# Explicit run
fungalphylo filter-orthogroups --run-id <run_id> /path/to/project 
```

- Selects orthogroups for phylogenomics (MAFFT -> trimAl -> IQ-TREE -> ASTRAL-Pro)
- Species with 0 or multiple copies are allowed (ASTRAL-Pro handles paralogs)
- Outputs: selected `.fa` files + `filter_summary.tsv`

---

# Gene Trees: phylo-slurm

SLURM array job: MAFFT -> trimAl -> IQ-TREE per orthogroup in parallel

```bash
# From filtered orthogroups
fungalphylo phylo-slurm --submit /path/to/project

# From explicit directory
fungalphylo phylo-slurm --input-dir /path/to/fastas --submit /path/to/project
```

- Defaults: `--retree 2 --maxiterate 1000`, `-gt 0.8 -cons 10`, `-m TEST -B 1000 -alrt 1000`
- 16 CPUs, 2G/cpu, 12h per task, max 380 concurrent (Puhti safe limit)
- Output: `runs/<run_id>/gene_trees/<OG_ID>/`

---

# Gene Family Pipeline: protsetphylo

For analyzing specific gene families (e.g., MFS sugar transporters):

```
protsetphylo init -> interproscan -> select -> orthofinder-slurm
    -> og-report -> og-apply -> phylo-slurm
```

Steps 1-3: Define family, annotate, select matching proteins

Steps 4-7: OrthoFinder -> inspect OGs -> select/merge -> gene trees

---

# protsetphylo: OG Report

After OrthoFinder on selected proteins:

```bash
fungalphylo protsetphylo og-report --family-id mfs_sugar \
  --run-id <of_run> /path/to/project
```

Outputs in `families/mfs_sugar/og_report/`:

- Characterized x OG matrix (TSV + HTML): which characterized genes are in which OGs
- Portal x OG matrix (TSV + HTML): gene counts per species, color-coded
- `og_decisions.txt`: editable template for include/merge decisions

---

# protsetphylo: OG Apply

Edit `og_decisions.txt`:

```
include: OG0000001,OG0000005
merge: OG0000002,OG0000003;OG0000004,OG0000006
```

Then apply:

```bash
fungalphylo protsetphylo og-apply --family-id mfs_sugar \
  --run-id <of_run> /path/to/project
```

- Copies included OGs as-is
- Merges specified groups into single FASTAs
- Output: `families/mfs_sugar/og_selected/` (ready for `phylo-slurm --input-dir`)

---

# Tool Configuration: tools.yaml

```yaml
busco:
  bin_dir: "/path/to/busco/bin"
  command: "busco"
orthofinder:
  env_activate: "/scratch/.../of3_env/bin/activate"
  command: "orthofinder"
  msa_program: "mafft"
mafft:
  bin_dir: ""
  command: "mafft"
trimal:
  bin_dir: "/path/to/trimal/bin"
  command: "trimal"
iqtree:
  bin_dir: ""
  command: "iqtree3"
```

---

# Useful Flags

| Flag | Available on | Effect |
|------|-------------|--------|
| `--dry-run` | stage, restore, download | Validate without side effects |
| `--submit` | all SLURM commands | Submit job after writing script |
| `--resume-run-id` | SLURM commands | Resume a timed-out run |
| `--og-only` | orthofinder-slurm | Skip MSA/gene trees |
| `--min-single-copy` | filter-orthogroups | Single-copy threshold |
| `--max-concurrent` | phylo-slurm | Max parallel array tasks (default: 380) |
| `--continue-on-error` | stage, restore, download | Don't stop on first failure |
| `--no-confirm` | SLURM commands | Skip account prompt |

---

# Diagnostics

```bash
# Project status summary
fungalphylo status /path/to/project

# Inspect failures
fungalphylo failures /path/to/project

# Database queries
fungalphylo db query /path/to/project "SELECT * FROM portals"
```

- `logs/events.jsonl` -- structured action log
- `logs/errors.jsonl` -- error details

---

# Design Principles

- **Immutable snapshots**: staging and runs never mutate prior directories
- **Restart contract**: every command has rerun/skip/completion semantics
- **Write-first**: SLURM scripts generated locally, submitted only with `--submit`
- **Artifact reuse**: equivalent artifacts shared across snapshots by cache key
- **Batch resilience**: `--continue-on-error` for large batch operations

---

# Full Pipeline Summary

```
init -> ingest -> fetch-index -> autoselect -> review
                                                  |
                              restore -> download -> stage
                                                       |
                    busco-slurm    interproscan-slurm   |
                                                        v
                              orthofinder-slurm (--og-only)
                                        |
                              filter-orthogroups
                                        |
                                   phylo-slurm
                          (MAFFT -> trimAl -> IQ-TREE x N)
                                        |
                                   ASTRAL-Pro
```

Gene family path: `protsetphylo init -> interproscan -> select -> orthofinder-slurm -> og-report -> og-apply -> phylo-slurm`
