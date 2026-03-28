# fungalphylo

A reproducible phylogenomics pipeline for fungal datasets. Handles everything from JGI data intake through staging, quality control, and gene family phylogenetics.

Built around:

- **SQLite** for state tracking and approvals
- **Immutable staging snapshots** for normalized inputs
- **SLURM script generation** for HPC compute (CSC/Puhti)
- **JGI Files API** for file discovery, restore, and download
- **TSV review loop** for human-in-the-loop file selection

---

## Quick Start

```bash
# Install
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Initialize a project (data directory, separate from this repo)
fungalphylo init /scratch/project_xxx/myproject
```

Requires Python **3.11+**. Dependencies: `typer`, `pyyaml`, `openpyxl`, `requests`, `rich`.

---

## Pipeline Overview

The pipeline has two main tracks:

### Data Intake Track
```
init → ingest → fetch-index → autoselect → review → restore → download → stage
```

### Compute Track (after staging)
```
busco-slurm            # Quality control
interproscan-slurm     # Domain annotation
orthofinder-slurm      # Orthogroup inference
filter-orthogroups     # Select single-copy orthogroups
phylo-slurm            # Parallel gene tree inference (MAFFT → trimAl → IQ-TREE)
protsetphylo           # Gene family phylogenomics
```

---

## Project Directory Layout

A fungalphylo **project directory** is separate from the code repo:

```
project/
  config.yaml                        # Project configuration
  tools.yaml                         # External tool paths (BUSCO, InterProScan, MAFFT, etc.)
  db/fungalphylo.sqlite              # State database
  raw/<portal_id>/<file_id>/         # Downloaded originals
  staging/<staging_id>/              # Immutable normalized snapshots
  runs/<run_id>/                     # Compute runs (BUSCO, IPR, alignment, tree)
  families/<family_id>/              # Gene family analyses
  review/                            # Human-editable TSVs
  cache/jgi_index_json/              # Cached JGI responses
  logs/events.jsonl                  # Action log
  logs/errors.jsonl                  # Error log
  restore_requests/<timestamp>/      # JGI restore batches
  download_requests/<timestamp>/     # JGI download batches
```

---

## Data Intake Workflow

### 1. Initialize a project

```bash
fungalphylo init /path/to/project --force
```

### 2. Ingest portal list

Import a MycoCosm spreadsheet (XLSX with hyperlinked `Name` and `Published` columns):

```bash
fungalphylo ingest /path/to/project --table mycocosm_portals.xlsx
```

### 3. Fetch file index from JGI

```bash
fungalphylo fetch-index /path/to/project
# Single portal
fungalphylo fetch-index /path/to/project --portal-id Dicsqu464_2
# From cached JSON only (no network)
fungalphylo fetch-index /path/to/project --ingest-from-cache
```

### 4. Auto-select best proteome + CDS

Heuristic scoring (configurable in `config.yaml`): prefers genome-group files, filtered proteins/CDS, FASTA format, newer files; avoids deflines, promoters, alleles, GFF.

```bash
fungalphylo autoselect /path/to/project
```

Outputs `review/autoselect_<ts>.tsv` and `review/autoselect_explain_<ts>.tsv`.

### 5. Review and apply selections

```bash
# Export editable TSV
fungalphylo review export /path/to/project --from-autoselect review/autoselect_<ts>.tsv

# Edit review_edit_<ts>.tsv: change file IDs, set approve=no to exclude portals

# Apply approvals to DB
fungalphylo review apply /path/to/project review/review_edit_<ts>.tsv
```

### 6. Restore files (can take hours/days)

```bash
fungalphylo restore /path/to/project --send-mail
# Preview without posting
fungalphylo restore /path/to/project --dry-run
```

Payloads are chunked to stay under backend character limits. Use `--continue-on-error` for large batches.

### 7. Download approved files

```bash
fungalphylo download /path/to/project
# Skip already-downloaded files
fungalphylo download /path/to/project --skip-if-raw-present
```

Downloads are saved to `raw/<portal_id>/<file_id>/<filename>`.

### 8. Stage (normalize + map)

Creates an immutable snapshot with canonical headers (`{portal_id}|{jgi_protein_id}`):

```bash
fungalphylo stage /path/to/project --dry-run   # Preview
fungalphylo stage /path/to/project              # Create snapshot
```

Outputs:
- `staging/<staging_id>/proteomes/<portal_id>.faa`
- `staging/<staging_id>/cds/<portal_id>.fna`
- `staging/<staging_id>/manifest.json`, `checksums.tsv`

Each run creates a new `staging_id`. Equivalent artifacts are reused by cache key.

#### Internal stop codons

Sequences with internal stop codons (asterisks not at the end) are handled by `--internal-stop`:

| Mode | Behavior |
|------|----------|
| `drop` (default) | Remove the sequence entirely |
| `warn` | Keep the sequence, count in stats |
| `strip` | Remove internal `*` characters, keep the sequence |

```bash
# Default: drop sequences with internal stops
fungalphylo stage /path/to/project

# Keep sequences but log warnings
fungalphylo stage /path/to/project --internal-stop warn

# Strip internal stop codons
fungalphylo stage /path/to/project --internal-stop strip
```

Trailing stop codons (`*` at sequence end) are always stripped regardless of mode.

#### Non-JGI headers

Some portals have non-standard FASTA headers. These require a per-portal ID mapping file (TSV with `canonical_protein_id`, `model_id`, `original_header`). Place in `idmaps/`.

---

## Compute: BUSCO

Quality control on staged proteomes:

```bash
# Generate SLURM script (latest staging by default)
fungalphylo busco-slurm /path/to/project --staging-id <staging_id>

# Resume a timed-out run with more time
fungalphylo busco-slurm /path/to/project --resume-run-id <run_id> --time 48:00:00 --submit

# Import results after completion
fungalphylo busco ingest-results /path/to/project --run-id <run_id>
```

Use `--submit` only on systems with `sbatch`. The generated script checks for prior completion and exits early if already done.

---

## Compute: InterProScan

Domain annotation on staged proteomes:

```bash
# Generate launcher + worker scripts
fungalphylo interproscan-slurm /path/to/project --application PfamA

# Resume after timeout
fungalphylo interproscan-slurm /path/to/project --resume-run-id <run_id> --submit

# Debug with subset
fungalphylo interproscan-slurm /path/to/project --limit 5
```

The launcher runs a submit-and-poll controller (one worker at a time to respect Puhti job limits). The worker loads `biokit` and `interproscan` modules. Failed sequences are automatically retried on resume.

---

## Compute: OrthoFinder

Orthogroup inference on staged proteomes or gene family selections:

```bash
# Orthogroups only (recommended — skips expensive MSA/gene trees)
fungalphylo orthofinder-slurm /path/to/project --og-only --submit

# Full analysis with MSA-based gene trees and species tree
fungalphylo orthofinder-slurm /path/to/project --submit

# Target a specific staging snapshot
fungalphylo orthofinder-slurm /path/to/project --og-only --staging-id <staging_id> --submit

# Run on a gene family's selected FASTAs instead of full proteomes
fungalphylo orthofinder-slurm /path/to/project --og-only --family-id mfs_sugar --submit

# Run on an explicit directory of .faa files
fungalphylo orthofinder-slurm /path/to/project --og-only --input-dir /path/to/fastas --submit

# Resume a timed-out run (reuses DIAMOND results)
fungalphylo orthofinder-slurm /path/to/project --resume-run-id <run_id> --submit

# Override MSA program (default: mafft; famsa may crash on some systems)
fungalphylo orthofinder-slurm /path/to/project --msa-program mafft --submit
```

**`--og-only`** uses `-M dendroblast` which skips MSA and gene tree inference. Orthogroup assignments are identical — MCL clustering happens before any MSA step. This avoids OrthoFinder v3's aggressive `--localpair --maxiterate 1000` MAFFT calls that can fail on large orthogroups.

OrthoFinder requires a virtual environment on Puhti. Configure in `tools.yaml`:

```yaml
orthofinder:
  env_activate: "/scratch/project_xxx/software/of3_env/bin/activate"
  command: "orthofinder"
  msa_program: "mafft"
```

The generated SLURM script handles `module purge` → `module load StdEnv` → `module load python-data` → `source env_activate` → `module load <msa_program>` automatically.

SLURM defaults: 48h, 16 CPUs, `small` partition. Memory scales with proteome count: 4G/cpu for <60 proteomes, 8G/cpu for ≥60. Output in `runs/<run_id>/orthofinder_results/`.

---

## Filter Orthogroups

After OrthoFinder completes, filter orthogroups by single-copy species occupancy for downstream phylogenomics (MAFFT → trimAl → IQ-TREE → ASTRAL-Pro):

```bash
# Filter from latest OrthoFinder run (default: ≥75% single-copy)
fungalphylo filter-orthogroups /path/to/project

# Explicit run ID
fungalphylo filter-orthogroups /path/to/project --run-id <run_id>

# Custom threshold (e.g., 80%)
fungalphylo filter-orthogroups /path/to/project --min-single-copy 0.80

# Point at OrthoFinder results directly
fungalphylo filter-orthogroups /path/to/project --results-dir /path/to/orthofinder_results

# Custom output directory
fungalphylo filter-orthogroups /path/to/project --output-dir /path/to/output
```

An orthogroup passes if at least `--min-single-copy` fraction of species have exactly 1 gene in the OG. Species with 0 or multiple copies are allowed — ASTRAL-Pro handles paralogs.

Outputs in `runs/<run_id>/filtered_orthogroups/`:
- `<OG_ID>.fa` — FASTA files for selected orthogroups
- `filter_summary.tsv` — per-OG stats (single-copy, multi-copy, missing counts)

---

## Compute: Gene Trees (`phylo-slurm`)

SLURM array job running MAFFT → trimAl → IQ-TREE per orthogroup in parallel:

```bash
# From filtered orthogroups (auto-detect)
fungalphylo phylo-slurm /path/to/project --submit

# From a specific OrthoFinder run
fungalphylo phylo-slurm /path/to/project --run-id <orthofinder_run_id> --submit

# From an explicit directory of .fa files
fungalphylo phylo-slurm /path/to/project --input-dir /path/to/fastas --submit

# Custom parameters
fungalphylo phylo-slurm /path/to/project \
  --mafft-maxiterate 500 --trimal-gt 0.5 \
  --iqtree-model LG+G4 --iqtree-bootstrap 2000 --submit
```

Default parameters per array task:
- **MAFFT**: `--retree 2 --maxiterate 1000`
- **trimAl**: `-gt 0.8 -cons 10`
- **IQ-TREE**: `-m TEST -B 1000 -alrt 1000`
- **SLURM**: 16 CPUs, 2G/cpu, 12h, max 380 concurrent tasks (Puhti safe limit)

Only MAFFT is loaded via `module load mafft`. trimAl and IQ-TREE3 use binaries configured via `bin_dir` in `tools.yaml`.

Output per OG in `runs/<run_id>/gene_trees/<OG_ID>/`:
- `<OG_ID>.aligned.faa` — MAFFT alignment
- `<OG_ID>.trimmed.faa` — trimAl output
- `<OG_ID>.treefile` — IQ-TREE gene tree

---

## Compute: Gene Family Phylogenomics (`protsetphylo`)

Analyze specific gene families (e.g., MFS sugar transporters) across your staged proteomes.

### Pipeline

```
protsetphylo init → interproscan → select → orthofinder-slurm → og-report → og-apply → phylo-slurm
                                      ↓
                              (optional quick path)
                          build-fasta → MMseqs2/CD-HIT → align → tree
```

The recommended path uses OrthoFinder on the per-species FASTAs from `select`, then `og-report` to inspect orthogroups containing characterized genes, `og-apply` to select/merge OGs, and `phylo-slurm` for parallel MAFFT → trimAl → IQ-TREE. The optional quick path uses `build-fasta` with clustering for fast exploratory analysis.

### Step-by-step

#### 1. Initialize a gene family

Provide a TSV of characterized proteins and target Pfam accessions:

```bash
fungalphylo protsetphylo init /path/to/project \
  --family-id mfs_sugar \
  --characterized characterized_proteins.tsv \
  --pfam PF00083
```

The characterized TSV must have columns: `portal_id`, `species`, `short_name`, `protein_name`, `sequence`. Optional: `protein_id`, `group_*`, `references`.

- `portal_id` can be blank for proteins not in your project (e.g., outgroup sequences from other species)
- `portal_id` set: the characterized protein will be BLASTed against that portal's selected proteins during `select` and will replace the best hit (or be appended if no hit)
- Multiple `--pfam` flags or `--pfam-list pfams.txt` for multi-domain families

Creates `families/<family_id>/` with preserved TSV, generated FASTA, and Pfam config.

#### 2. Run InterProScan on characterized proteins

```bash
fungalphylo protsetphylo interproscan /path/to/project \
  --family-id mfs_sugar \
  --account project_xxx \
  --submit
```

Generates a SLURM script to run InterProScan on the characterized FASTA. Results go to `families/<family_id>/characterized/interproscan/`.

#### 3. Select matching proteins from project proteomes

```bash
module load blast
fungalphylo protsetphylo select /path/to/project \
  --family-id mfs_sugar \
  --arch-mode flag
```

Selection logic:
- Computes e-value thresholds from the characterized set (worst score per Pfam = threshold)
- Scans project InterProScan results for proteins matching target Pfams within threshold
- `--arch-mode strict`: exclude proteins with non-matching domain architectures
- `--arch-mode flag` (default): include all, annotate match status in report
- `--arch-mode off`: skip architecture check

Characterized protein integration (requires BLAST on PATH):
- **With `portal_id`**: BLASTs the characterized protein against that portal's selected proteins. Best hit is replaced with the characterized sequence (header + sequence). If no hit, the characterized protein is appended.
- **Without `portal_id`**: Written as a standalone FASTA grouped by `short_name` (e.g., outgroup species)
- `--blast-evalue` controls the BLAST cutoff (default: 10.0)
- If BLAST is not available, characterized proteins are appended with a warning

Output: per-species FASTAs in `families/<family_id>/selected/` plus `selection_report.tsv`. This directory is ready to be used as input for OrthoFinder.

#### 4. OrthoFinder on selected proteins

```bash
fungalphylo orthofinder-slurm /path/to/project \
  --family-id mfs_sugar --og-only --submit
```

Runs OrthoFinder on `families/mfs_sugar/selected/` to identify orthogroups via MCL clustering.

#### 5. Inspect orthogroups with `og-report`

```bash
fungalphylo protsetphylo og-report /path/to/project \
  --family-id mfs_sugar --run-id <orthofinder_run_id>
```

Generates reports in `families/mfs_sugar/og_report/`:
- `characterized_og_matrix.tsv/.html` — which characterized genes are in which OGs
- `portal_og_matrix.tsv/.html` — gene counts per portal for OGs with characterized genes (color-coded)
- `og_decisions.txt` — editable template for selecting/merging OGs

#### 6. Apply OG decisions with `og-apply`

Edit `og_decisions.txt` to specify which OGs to keep and which to merge:

```
include: OG0000001,OG0000005
merge: OG0000002,OG0000003;OG0000004,OG0000006
```

The above keeps OG0000001 and OG0000005 as-is, merges OG0000002+OG0000003 into one FASTA, and merges OG0000004+OG0000006 into another.

```bash
fungalphylo protsetphylo og-apply /path/to/project \
  --family-id mfs_sugar --run-id <orthofinder_run_id>
```

Outputs FASTAs to `families/mfs_sugar/og_selected/`, ready for the next step.

#### 7. Gene tree inference with `phylo-slurm`

```bash
fungalphylo phylo-slurm /path/to/project \
  --input-dir /path/to/project/families/mfs_sugar/og_selected/ --submit
```

Runs a SLURM array job: MAFFT → trimAl → IQ-TREE per OG. Gene trees output in `runs/<run_id>/gene_trees/<OG_ID>/`.

#### Optional: Quick clustering with `build-fasta`

For fast exploratory analysis without OrthoFinder:

```bash
# Merge all selected into one FASTA (no clustering)
fungalphylo protsetphylo build-fasta /path/to/project \
  --family-id mfs_sugar

# Or with MMseqs2/CD-HIT clustering for subfamily splitting
module load mmseqs2
fungalphylo protsetphylo build-fasta /path/to/project \
  --family-id mfs_sugar \
  --redundancy-tool mmseqs2 \
  --identity-threshold 0.3
```

When using a redundancy tool, outputs include:
- `fasta/combined.faa` — representative sequences
- `fasta/combined.pre_dedup.faa` — all sequences before clustering
- `fasta/cluster_members.tsv` — cluster membership (representative → member)
- `fasta/clusters/` — per-cluster FASTAs
- `fasta/clusters/cluster_summary.tsv` — cluster sizes with `has_characterized` flag

#### 5. Align

```bash
fungalphylo protsetphylo align /path/to/project \
  --family-id mfs_sugar \
  --account project_xxx \
  --submit
```

Generates a SLURM script that runs MAFFT then trimAl. Configurable parameters:

| Option | Default | Description |
|--------|---------|-------------|
| `--mafft-retree` | 2 | Guide tree rebuilds |
| `--mafft-maxiterate` | 2 | Iterative refinement cycles (use 1000 for small sets) |
| `--trimal-gt` | 0.8 | Gap threshold (fraction of sequences required) |
| `--trimal-cons` | 10.0 | Minimum conservation percentage |
| `--input-fasta` | | Override input (e.g., a cluster FASTA) |

Outputs in `families/<family_id>/alignment/`.

#### 6. Build phylogenetic tree

```bash
# IQ-TREE (refined, slower)
fungalphylo protsetphylo tree /path/to/project \
  --family-id mfs_sugar \
  --tree-method iqtree \
  --account project_xxx \
  --submit

# FastTree (exploratory, fast)
fungalphylo protsetphylo tree /path/to/project \
  --family-id mfs_sugar \
  --tree-method fasttree \
  --account project_xxx \
  --submit
```

IQ-TREE defaults: `-m MFP -bb 1000 -nt AUTO`. Override with `--model` and `--bootstrap`. Use `--input-alignment` to point at a specific alignment (e.g., per-cluster). Output in `families/<family_id>/tree/`.

### Family directory structure

```
families/<family_id>/
  characterized/
    characterized.tsv          # Original input (preserved)
    characterized.faa          # Generated FASTA
    interproscan/              # IPR results on characterized
  config/
    pfams.txt                  # Target Pfam accessions
  selected/
    <portal_id>.faa            # Per-species FASTAs (OrthoFinder input)
    <short_name>.faa           # Standalone characterized species
    selection_report.tsv       # What was selected and why
  fasta/                       # Only if using build-fasta quick path
    combined.faa               # Merged/clustered sequences
    combined.pre_dedup.faa     # Pre-dedup backup (if clustering)
    cluster_members.tsv        # Cluster membership (if clustering)
    clusters/                  # Per-cluster FASTAs (if clustering)
      cluster_summary.tsv      # Cluster sizes + characterized flags
  alignment/
    combined.aln               # MAFFT output
    combined.trimmed.aln       # trimAl output
  tree/
    combined.treefile          # IQ-TREE or FastTree output
  manifest.json
```

---

## Taxonomy & QC Reports

```bash
# Fetch NCBI taxon IDs
fungalphylo taxonomy fetch-ncbi /path/to/project

# Export/edit/apply taxonomy mapping
fungalphylo taxonomy export /path/to/project --approved-only --out review/portal_taxonomy.tsv
fungalphylo taxonomy apply /path/to/project review/portal_taxonomy.tsv

# Generate BUSCO QC report ordered by taxonomy
fungalphylo busco ingest-results /path/to/project --run-id <run_id>
fungalphylo taxonomy busco-mockup /path/to/project --summary-rank family
```

---

## Tool Configuration

External tools are configured in `tools.yaml`:

```yaml
busco:
  bin_dir: "/path/to/busco/bin"   # optional
  command: "busco"
interproscan:
  bin_dir: ""                      # optional, modules loaded in job
  command: "cluster_interproscan"
mafft:
  bin_dir: ""                      # optional, module loaded in job
  command: "mafft"
trimal:
  bin_dir: "/path/to/trimal/bin"   # set if not in PATH/module
  command: "trimal"
iqtree:
  bin_dir: "/path/to/iqtree/bin"   # set if not in PATH/module
  command: "iqtree3"
fasttree:
  bin_dir: "/path/to/fasttree/bin" # set if not in PATH/module
  command: "fasttree"
blast:
  bin_dir: ""                      # optional, module loaded before select
  makeblastdb_cmd: "makeblastdb"
  blastp_cmd: "blastp"
orthofinder:
  env_activate: "/path/to/of3_env/bin/activate"  # venv activate script
  command: "orthofinder"
  msa_program: "mafft"             # famsa may crash on some systems
```

When `bin_dir` is set, generated SLURM scripts add `export PATH="<bin_dir>:$PATH"`. When empty, scripts use `module load <tool>` instead. On Puhti, most tools are available via `module load` (e.g., `module load blast` before running `protsetphylo select`).

---

## Diagnostics

```bash
# Project status summary
fungalphylo status /path/to/project

# Inspect failures (batches, staging errors, error log)
fungalphylo failures /path/to/project

# Database queries
fungalphylo db query /path/to/project "SELECT * FROM families"
```

---

## Useful Flags

| Flag | Available on | Effect |
|------|-------------|--------|
| `--dry-run` | stage, restore, download | Validate without side effects |
| `--continue-on-error` | stage, restore, download | Don't stop on first failure |
| `--submit` | busco-slurm, interproscan-slurm, orthofinder-slurm, phylo-slurm, protsetphylo | Submit SLURM job after writing |
| `--resume-run-id` | busco-slurm, interproscan-slurm, orthofinder-slurm | Resume a timed-out run |
| `--max-concurrent` | phylo-slurm | Max concurrent array tasks (default: 380) |
| `--og-only` | orthofinder-slurm | Use dendroblast (skip MSA/gene trees) |
| `--min-single-copy` | filter-orthogroups | Fraction of species with exactly 1 copy (default: 0.75) |
| `--staging-id` | most compute commands | Target a specific snapshot |
| `--no-confirm` | SLURM commands | Skip account confirmation prompt |

---

## Logging

- `logs/events.jsonl` — structured records for all major actions
- `logs/errors.jsonl` — error details for batch operations

---

## Development

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_protsetphylo_init.py -v

# Lint and format
ruff check .
ruff format .

# Type checking
mypy src/fungalphylo

# Quick compile check
python -m compileall src
```

Line length: 100 characters. See `pyproject.toml` for full config.

---

## Design Principles

- **Immutable snapshots**: staging and runs never mutate prior directories
- **Batch ledger boundary**: SQLite tracks batches; per-item detail lives in files
- **Artifact reuse**: equivalent artifacts shared across snapshots by cache key
- **Restart contract**: every command has documented rerun/skip/completion semantics (see `agent_context/restart_contract.md`)
- **Write-first**: SLURM scripts are generated locally, submitted only with `--submit`

---

## License

MIT (or update as needed).
