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
busco-slurm -> interproscan-slurm -> orthofinder-slurm -> filter-orthogroups -> phylo-slurm
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
  families/                # Gene family analyses
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

# Resume a timed-out run (reuses DIAMOND results)
fungalphylo orthofinder-slurm --resume-run-id <run_id> --submit /path/to/project
```

- Uses OrthoFinder 2.5.5 via Tykky container on Puhti
- `--og-only` adds `-M msa -os` (stops after orthogroup sequences)
- Memory scales: 4G/cpu (<60 proteomes), 8G/cpu (>=60)

---

# OrthoFinder: Setup on Puhti

OrthoFinder 2.5.5 installed via Tykky container:

```yaml
# tools.yaml
orthofinder:
  env_path: "/scratch/project_xxx/software/of2_tykky/bin"
  command: "orthofinder"
  msa_program: "mafft"
```

- SLURM script prepends `env_path` to `PATH`
- `module load mafft` loaded automatically for MSA
- OrthoFinder 3 has STRIDE bugs with large datasets -- use v2.5.5

---

# Filter Orthogroups

```bash
# Default: >=75% of species with exactly 1 copy
fungalphylo filter-orthogroups /path/to/project

# Custom threshold
fungalphylo filter-orthogroups --min-single-copy 0.80 /path/to/project 
```

- Selects orthogroups for phylogenomics
- Species with 0 or multiple copies allowed (ASTRAL-Pro handles paralogs)
- Outputs: selected `.fa` files + `filter_summary.tsv`

---

# Gene Trees: phylo-slurm

Cap-and-resubmit SLURM array: MAFFT -> trimAl -> IQ-TREE per orthogroup

```bash
# Generate orchestrator + worker scripts
fungalphylo phylo-slurm /path/to/project

# Run on login node
bash runs/<run_id>/slurm/phylo_orchestrate.sh

# Rerun after completion for remaining OGs
bash runs/<run_id>/slurm/phylo_orchestrate.sh
```

---

# phylo-slurm: How It Works

**Orchestrator** (login node script):

- Checks which OGs already have a `.treefile`
- Writes pending list, caps array to 380 tasks (Puhti limit)
- Submits worker array job

**Worker** (array job, one task per OG):

- MAFFT `--retree 2 --maxiterate 1000` -> trimAl -> IQ-TREE `-m TEST`
- Skips steps whose output already exists (step-level resume)
- 16 CPUs, 2G/cpu, 12h per task

Rerun the orchestrator to process remaining OGs (e.g., 7 runs for 2690 OGs).

---

# Gene Family Pipeline: protsetphylo

For analyzing specific gene families (e.g., MFS sugar transporters):

```
protsetphylo init -> interproscan -> select -> orthofinder-slurm
  -> place-standalone -> og-report -> og-apply -> protsetphylo phylo-slurm
```

- Define family with characterized proteins + target Pfam domains
- Select matching proteins from project proteomes
- OrthoFinder groups them into orthogroups
- Inspect, curate, and build per-OG gene trees

---

# protsetphylo: Init + Select

```bash
# Initialize family with characterized proteins and Pfam targets
fungalphylo protsetphylo init --family-id mfs_sugar \
  --characterized proteins.tsv --pfam PF00083 /path/to/project

# Run InterProScan on characterized set
fungalphylo protsetphylo interproscan --family-id mfs_sugar \
  --account project_xxx --submit /path/to/project

# Select matching proteins from project proteomes
fungalphylo protsetphylo select --family-id mfs_sugar \
  --arch-mode flag /path/to/project
```

- E-value thresholds computed per Pfam from characterized set
- Characterized proteins with `portal_id` replace best BLAST hits
- Standalone proteins (no `portal_id`) separated for later HMM placement

---

# protsetphylo: Place Standalone

Characterized genes without `portal_id` (outgroups) placed into OGs via HMM:

```bash
fungalphylo protsetphylo place-standalone --family-id mfs_sugar \
  --run-id <of_run> --account project_xxx --submit /path/to/project
```

- Copies OG FASTAs to `og_placed/`, appends standalone genes
- Builds HMM per OG, searches standalone sequences, best hit wins
- Writes `placements.tsv` (used by og-report)
- Keeps OrthoFinder output immutable
- Skip if all characterized genes have portal IDs

---

# protsetphylo: OG Report

```bash
fungalphylo protsetphylo og-report --family-id mfs_sugar \
  --run-id <of_run> /path/to/project
```

Reports in `families/mfs_sugar/og_report/`:

- **Characterized x OG matrix** -- which characterized genes in which OGs
  - Uses `portal_id|protein_name` labels
  - Includes standalone genes from `placements.tsv`
- **Portal x OG matrix** -- gene counts per species (color-coded)
- **`og_decisions.txt`** -- editable template for OG selection

Options: `--orientation vertical`, `--no-placed`

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

- Reads from `og_placed/` by default (includes standalone placements)
- Use `--no-placed` to force `Orthogroup_Sequences/`
- Output: `families/mfs_sugar/og_selected/`

---

# protsetphylo: Phylo-SLURM

Chained SLURM array jobs with per-step resource allocation:

```bash
fungalphylo protsetphylo phylo-slurm --family-id mfs_sugar \
  --account project_xxx /path/to/project

# Then run:
bash runs/<run_id>/slurm/phylo_orchestrate.sh
```

Orchestrator chains three submissions with `--dependency=afterok:`:

| Step | Tool | Default resources |
|------|------|-------------------|
| align | MAFFT `--auto` | 8 CPUs, 4G/cpu, 4h |
| trim | trimAl | 1 CPU, 4G, 15min |
| tree | IQ-TREE `-m TEST` | 8 CPUs, 4G/cpu, 8h |

- `--iqtree-fast` for quicker tree inference on large OGs
- Per-step resources configurable: `--align-time`, `--tree-cpus`, etc.
- Same cap-and-resubmit pattern as species tree phylo-slurm

---

# protsetphylo: Quick Path (Optional)

For fast exploratory analysis without OrthoFinder:

```bash
# Build combined FASTA with clustering
fungalphylo protsetphylo build-fasta --family-id mfs_sugar \
  --redundancy-tool mmseqs2 --identity-threshold 0.3 /path/to/project

# Single alignment + tree
fungalphylo protsetphylo align --family-id mfs_sugar \
  --account project_xxx --submit /path/to/project

fungalphylo protsetphylo tree --family-id mfs_sugar \
  --tree-method iqtree --account project_xxx --submit /path/to/project
```

- Useful for initial exploration before committing to full OG analysis

---

# Tool Configuration: tools.yaml

```yaml
orthofinder:
  env_path: "/scratch/.../of2_tykky/bin"
  command: "orthofinder"
  msa_program: "mafft"
mafft:
  command: "mafft"
trimal:
  bin_dir: "/path/to/trimal/bin"
  command: "trimal"
iqtree:
  bin_dir: "/path/to/iqtree/bin"
  command: "iqtree2"
hmmer:
  hmmbuild_cmd: "hmmbuild"
  hmmsearch_cmd: "hmmsearch"
```

- `bin_dir`/`env_path` set: SLURM script adds `export PATH=...`
- Empty: script uses `module load <tool>` instead

---

# Useful Flags

| Flag | Available on | Effect |
|------|-------------|--------|
| `--dry-run` | stage, restore, download | Validate without side effects |
| `--submit` | all SLURM commands | Submit job after writing script |
| `--resume-run-id` | SLURM commands | Resume a timed-out run |
| `--og-only` | orthofinder-slurm | Stop after OG sequences (`-M msa -os`) |
| `--max-array-size` | phylo-slurm | Max tasks per submission (default: 380) |
| `--max-concurrent` | phylo-slurm | Max parallel tasks (default: 100) |
| `--iqtree-fast` | protsetphylo phylo-slurm | IQ-TREE fast mode |
| `--no-placed` | og-report, og-apply | Ignore standalone placements |
| `--force` | init, protsetphylo init | Overwrite existing project/family |
| `--continue-on-error` | stage, restore, download | Don't stop on first failure |

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
- **Cap-and-resubmit**: array jobs capped to Puhti limits, rerun for remaining
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

Gene family path:

```
protsetphylo init -> interproscan -> select -> orthofinder-slurm
  -> place-standalone -> og-report -> og-apply -> protsetphylo phylo-slurm
```

---

# Next Steps

- **ASTRAL-Pro integration**: concatenate gene trees into species tree
- **Automated tree QC**: detect long branches, rogue taxa, low-support clades
- **Batch phylo-slurm monitoring**: command to check progress across array batches
- **Gene tree reconciliation**: map gene trees to species tree for duplication/loss inference
- **Visualization helpers**: export annotated trees for iTOL or FigTree
- **Multi-family comparisons**: cross-family OG overlap and co-evolution analysis

---

# Thank You

```
pip install -e ".[dev]"
fungalphylo --help
```

Repository: github.com/vimagora/fungalphylo

Questions?
