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
protsetphylo           # Gene family phylogenomics + visualization
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
fungalphylo init --force /path/to/project
```

### 2. Ingest portal list

Import a MycoCosm spreadsheet (XLSX with hyperlinked `Name` and `Published` columns):

```bash
fungalphylo ingest --table mycocosm_portals.xlsx /path/to/project
```

### 3. Fetch file index from JGI

```bash
fungalphylo fetch-index /path/to/project
# Single portal
fungalphylo fetch-index --portal-id Dicsqu464_2 /path/to/project
# From cached JSON only (no network)
fungalphylo fetch-index --ingest-from-cache /path/to/project
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
fungalphylo review export --from-autoselect review/autoselect_<ts>.tsv /path/to/project

# Edit review_edit_<ts>.tsv: change file IDs, set approve=no to exclude portals

# Apply approvals to DB
fungalphylo review apply review/review_edit_<ts>.tsv /path/to/project
```

### 6. Restore files (can take hours/days)

```bash
fungalphylo restore --send-mail /path/to/project
# Preview without posting
fungalphylo restore --dry-run /path/to/project
```

Payloads are chunked to stay under backend character limits. Use `--continue-on-error` for large batches.

### 7. Download approved files

```bash
fungalphylo download /path/to/project
# Skip already-downloaded files
fungalphylo download --skip-if-raw-present /path/to/project
```

Downloads are saved to `raw/<portal_id>/<file_id>/<filename>`.

### 8. Stage (normalize + map)

Creates an immutable snapshot with canonical headers (`{portal_id}|{jgi_protein_id}`):

```bash
fungalphylo stage --dry-run /path/to/project    # Preview
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
fungalphylo stage --internal-stop warn /path/to/project

# Strip internal stop codons
fungalphylo stage --internal-stop strip /path/to/project
```

Trailing stop codons (`*` at sequence end) are always stripped regardless of mode.

#### Non-JGI headers

Some portals have non-standard FASTA headers. These require a per-portal ID mapping file (TSV with `canonical_protein_id`, `model_id`, `original_header`). Place in `idmaps/`.

---

## Compute: BUSCO

Quality control on staged proteomes:

```bash
# Generate SLURM script (latest staging by default)
fungalphylo busco-slurm --staging-id <staging_id> /path/to/project

# Resume a timed-out run with more time
fungalphylo busco-slurm --resume-run-id <run_id> --time 48:00:00 --submit /path/to/project

# Import results after completion
fungalphylo busco ingest-results --run-id <run_id> /path/to/project
```

Use `--submit` only on systems with `sbatch`. The generated script checks for prior completion and exits early if already done.

---

## Compute: InterProScan

Domain annotation on staged proteomes:

```bash
# Generate launcher + worker scripts
fungalphylo interproscan-slurm --application PfamA /path/to/project

# Resume after timeout
fungalphylo interproscan-slurm --resume-run-id <run_id> --submit /path/to/project

# Debug with subset
fungalphylo interproscan-slurm --limit 5 /path/to/project
```

The launcher runs a submit-and-poll controller (one worker at a time to respect Puhti job limits). The worker loads `biokit` and `interproscan` modules. Failed sequences are automatically retried on resume.

---

## Compute: OrthoFinder

Orthogroup inference on staged proteomes or gene family selections:

```bash
# Orthogroups only (recommended — skips expensive MSA/gene trees)
fungalphylo orthofinder-slurm --og-only --submit /path/to/project

# Full analysis with MSA-based gene trees and species tree
fungalphylo orthofinder-slurm --submit /path/to/project

# Target a specific staging snapshot
fungalphylo orthofinder-slurm --og-only --staging-id <staging_id> --submit /path/to/project

# Run on a gene family's selected FASTAs instead of full proteomes
fungalphylo orthofinder-slurm --og-only --family-id mfs_sugar --submit /path/to/project

# Run on an explicit directory of .faa files
fungalphylo orthofinder-slurm --og-only --input-dir /path/to/fastas --submit /path/to/project

# Resume a timed-out run (reuses DIAMOND results)
fungalphylo orthofinder-slurm --resume-run-id <run_id> --submit /path/to/project

# Override MSA program (default: mafft; famsa may crash on some systems)
fungalphylo orthofinder-slurm --msa-program mafft --submit /path/to/project
```

**`--og-only`** adds `-M msa -os` which stops after writing orthogroup sequences, skipping gene tree and species tree inference. This is the recommended mode when you plan to build gene trees yourself with `phylo-slurm`.

OrthoFinder is installed via Tykky container on Puhti. Configure in `tools.yaml`:

```yaml
orthofinder:
  env_path: "/scratch/project_xxx/software/of2_tykky/bin"
  command: "orthofinder"
  msa_program: "mafft"
```

The generated SLURM script prepends `env_path` to `PATH` and loads `module load mafft` for MSA.

SLURM defaults: 48h, 16 CPUs, `small` partition. Memory scales with proteome count: 4G/cpu for <60 proteomes, 8G/cpu for ≥60. Output in `runs/<run_id>/orthofinder_results/`.

---

## Filter Orthogroups

After OrthoFinder completes, filter orthogroups by single-copy species occupancy for downstream phylogenomics (MAFFT → trimAl → IQ-TREE → ASTRAL-Pro):

```bash
# Filter from latest OrthoFinder run (default: ≥75% single-copy)
fungalphylo filter-orthogroups /path/to/project

# Explicit run ID
fungalphylo filter-orthogroups --run-id <run_id> /path/to/project

# Custom threshold (e.g., 80%)
fungalphylo filter-orthogroups --min-single-copy 0.80 /path/to/project
```

An orthogroup passes if at least `--min-single-copy` fraction of species have exactly 1 gene in the OG. Species with 0 or multiple copies are allowed — ASTRAL-Pro handles paralogs.

Outputs in `runs/<run_id>/filtered_orthogroups/`:
- `<OG_ID>.fa` — FASTA files for selected orthogroups
- `filter_summary.tsv` — per-OG stats (single-copy, multi-copy, missing counts)

---

## Compute: Gene Trees (`phylo-slurm`)

Cap-and-resubmit SLURM array job running MAFFT → trimAl → IQ-TREE per orthogroup. Generates two scripts: an **orchestrator** (run on the login node) that computes pending OGs and caps the array, and a **worker** (sbatch array job) with step-level resume.

```bash
# Generate scripts (auto-detect filtered orthogroups)
fungalphylo phylo-slurm /path/to/project

# Then run on login node:
bash runs/<run_id>/slurm/phylo_orchestrate.sh
# Rerun after completion to process remaining OGs

# Or generate + submit in one step
fungalphylo phylo-slurm --submit /path/to/project

# From a specific OrthoFinder run
fungalphylo phylo-slurm --run-id <orthofinder_run_id> /path/to/project

# Custom parameters
fungalphylo phylo-slurm \
  --mafft-maxiterate 500 --trimal-gt 0.5 \
  --iqtree-model LG+G4 --iqtree-bootstrap 2000 \
  --max-array-size 200 --max-concurrent 50 \
  /path/to/project
```

Default parameters per array task:
- **MAFFT**: `--retree 2 --maxiterate 1000`
- **trimAl**: `-gt 0.8 -cons 10`
- **IQ-TREE**: `-m TEST -B 1000 -alrt 1000`
- **SLURM**: 16 CPUs, 2G/cpu, 12h, max 380 tasks per submission, max 100 concurrent

**How it works**: The orchestrator checks which OGs already have a `.treefile`, writes a pending list, caps the array to `--max-array-size` (default 380, within Puhti's limits), and submits. The worker skips steps whose output already exists (step-level resume). After the batch completes, rerun the orchestrator to process remaining OGs. For 2690 OGs, you'd rerun ~7 times.

Only MAFFT is loaded via `module load mafft`. trimAl and IQ-TREE use binaries configured via `bin_dir` in `tools.yaml`.

Output per OG in `runs/<run_id>/gene_trees/<OG_ID>/`:
- `<OG_ID>.aligned.faa` — MAFFT alignment
- `<OG_ID>.trimmed.faa` — trimAl output
- `<OG_ID>.treefile` — IQ-TREE gene tree

---

## Species Tree with ASTRAL-Pro (`astral-slurm`)

After gene trees are computed, infer a species tree using ASTRAL-Pro. This command collects gene trees from a `phylo-slurm` run, builds a species mapping from tip labels, filters by minimum species count, and generates a SLURM script.

```bash
# Generate ASTRAL-Pro script (auto-detect latest phylo run)
fungalphylo astral-slurm --account project_123 /path/to/project

# From a specific phylo run
fungalphylo astral-slurm \
  --account project_123 \
  --run-id <phylo_run_id> \
  --output-run-id astral_run1 \
  /path/to/project

# Generate + submit
fungalphylo astral-slurm \
  --account project_123 --submit /path/to/project

# Custom parameters
fungalphylo astral-slurm \
  --account project_123 \
  --min-taxa 10 --delimiter "|" \
  --time 08:00:00 --mem 32G \
  /path/to/project
```

**How it works**: Tip labels in gene trees use the format `species|protein_id`. The command uses dendropy to rename tips to just the species name (stripping the protein ID) before writing the concatenated tree file. This means ASTRAL-Pro sees multi-labeled trees (paralogs appear as duplicate species names), which it handles natively — no mapping file needed. Trees with fewer than `--min-taxa` species (default 4) are filtered out. On Puhti, it loads `module load aster/1.23` and runs `astral-pro3`.

Key options:
- `--min-taxa` — Minimum number of species in a gene tree to include it (default: 4)
- `--delimiter` — Character separating species from gene ID in tip labels (default: `|`)
- `--astral-cmd` — ASTRAL executable (default: `astral-pro3`)
- `--extra-args` — Additional arguments passed to ASTRAL-Pro

Output in `runs/<run_id>/`:
- `slurm/gene_trees.nwk` — Concatenated gene trees with tips renamed to species
- `slurm/astral.sbatch` — SLURM script
- `species_tree.nwk` — Output species tree (after job completes)

SLURM defaults: 4h, 4 CPUs, 16G memory, `small` partition.

---

## Compute: Gene Family Phylogenomics (`protsetphylo`)

Analyze specific gene families (e.g., MFS sugar transporters) across your staged proteomes.

### Pipeline

```
init → interproscan → select → orthofinder-slurm → [place-standalone] → og-report → og-apply → phylo-slurm → [taxonomy] → tree-export → clade-mark
```

OrthoFinder clusters per-species FASTAs from `select` into orthogroups. If standalone characterized genes exist (no `portal_id`), `place-standalone` uses HMM profiles to assign them to OGs. Then `og-report` inspects which OGs contain characterized genes, `og-apply` selects/merges OGs, and `protsetphylo phylo-slurm` runs chained SLURM array jobs (MAFFT --auto → trimAl → IQ-TREE) per OG.

### Step-by-step

#### 1. Initialize a gene family

Provide a TSV of characterized proteins and target Pfam accessions:

```bash
fungalphylo protsetphylo init \
  --family-id mfs_sugar \
  --characterized characterized_proteins.tsv \
  --pfam PF00083 \
  /path/to/project
```

The characterized TSV must have columns: `portal_id`, `species`, `short_name`, `protein_name`, `sequence`. Optional: `protein_id`, `group_*`, `references`.

- `portal_id` can be blank for proteins not in your project (e.g., outgroup sequences from other species)
- `portal_id` set: the characterized protein will be BLASTed against that portal's selected proteins during `select` and will replace the best hit (or be appended if no hit)
- Multiple `--pfam` flags or `--pfam-list pfams.txt` for multi-domain families

Creates `families/<family_id>/` with preserved TSV, generated FASTA, and Pfam config.

#### 2. Run InterProScan on characterized proteins

```bash
fungalphylo protsetphylo interproscan \
  --family-id mfs_sugar \
  --submit /path/to/project
```

Generates a SLURM script to run InterProScan on the characterized FASTA. Results go to `families/<family_id>/characterized/interproscan/`.

#### 3. Select matching proteins from project proteomes

```bash
module load blast
fungalphylo protsetphylo select \
  --family-id mfs_sugar \
  --arch-mode flag \
  /path/to/project 
```

Selection logic:
- Computes e-value thresholds from the characterized set (worst score per Pfam = threshold)
- Scans project InterProScan results for proteins matching target Pfams within threshold
- `--arch-mode strict`: exclude proteins with non-matching domain architectures
- `--arch-mode flag` (default): include all, annotate match status in report
- `--arch-mode off`: skip architecture check

Characterized protein integration (requires BLAST on PATH):
- **With `portal_id`**: BLASTs the characterized protein against that portal's selected proteins. Best hit is replaced with the characterized sequence (header + sequence). If no hit, the characterized protein is appended.
- **Without `portal_id`**: Written to `families/<family_id>/selected/standalone/` (separated from portal FASTAs to avoid OrthoFinder issues with single-sequence species files)
- `--blast-evalue` controls the BLAST cutoff (default: 10.0)
- If BLAST is not available, characterized proteins are appended with a warning

Output: per-species FASTAs in `families/<family_id>/selected/` plus `selection_report.tsv`. Standalone characterized proteins go to `selected/standalone/`. The `selected/` directory (excluding `standalone/`) is ready for OrthoFinder.

#### 4. OrthoFinder on selected proteins

```bash
fungalphylo orthofinder-slurm \
  --family-id mfs_sugar --og-only --submit /path/to/project 
```

Runs OrthoFinder on `families/mfs_sugar/selected/` to identify orthogroups via MCL clustering.

#### 5. Place standalone characterized genes (optional)

If some characterized genes have no `portal_id` (e.g., outgroup species), they were excluded from OrthoFinder. Place them into OGs using HMM profiles **before** generating reports:

```bash
fungalphylo protsetphylo place-standalone \
  --family-id mfs_sugar --run-id <orthofinder_run_id> \
  --submit /path/to/project
```

Generates a SLURM script that:
- Copies all OG FASTAs from `Orthogroup_Sequences/` to `families/<family_id>/og_placed/`
- Aligns each OG with MAFFT, builds HMM profiles with `hmmbuild`
- Searches standalone sequences against all profiles with `hmmsearch`
- Appends each sequence to its best-matching OG in `og_placed/`
- Writes `placements.tsv` report

Requires HMMER (on Puhti: `module load biokit`). Skip this step if all characterized genes have portal IDs.

#### 6. Inspect orthogroups with `og-report`

```bash
fungalphylo protsetphylo og-report \
  --family-id mfs_sugar --run-id <orthofinder_run_id> \
  /path/to/project

# Vertical orientation (OGs as columns, portals/proteins as rows)
fungalphylo protsetphylo og-report \
  --family-id mfs_sugar --orientation vertical \
  /path/to/project

# Ignore placements (only show genes from Orthogroups.tsv)
fungalphylo protsetphylo og-report \
  --family-id mfs_sugar --no-placed \
  /path/to/project
```

Generates reports in `families/mfs_sugar/og_report/`:
- `characterized_og_matrix.tsv/.html` — which characterized genes are in which OGs (columns use `portal_id|protein_name` when portal is available). Includes standalone genes from `placements.tsv` if available.
- `portal_og_matrix.tsv/.html` — gene counts per portal for OGs with characterized genes (color-coded)
- `og_decisions.txt` — editable template for selecting/merging OGs

Use `--orientation horizontal` (default) for OGs as rows, or `--orientation vertical` for OGs as columns.

#### 7. Apply OG decisions with `og-apply`

Edit `og_decisions.txt` to specify which OGs to keep and which to merge:

```
include: OG0000001,OG0000005
merge: OG0000002,OG0000003;OG0000004,OG0000006
```

The above keeps OG0000001 and OG0000005 as-is, merges OG0000002+OG0000003 into one FASTA, and merges OG0000004+OG0000006 into another.

```bash
fungalphylo protsetphylo og-apply \
  --family-id mfs_sugar  \
  --run-id <orthofinder_run_id> \
  /path/to/project

# Force reading from Orthogroup_Sequences/ instead of og_placed/
fungalphylo protsetphylo og-apply \
  --family-id mfs_sugar \
  --run-id <orthofinder_run_id> \
  --no-placed /path/to/project
```

By default, `og-apply` reads from `og_placed/` (which includes standalone genes) if it exists, otherwise falls back to `Orthogroup_Sequences/`. Use `--no-placed` to force the fallback.

Outputs FASTAs to `families/mfs_sugar/og_selected/`, ready for the next step.

#### 8. Gene tree inference with `protsetphylo phylo-slurm`

Chained SLURM array jobs with per-step resource allocation, designed for large OGs (thousands of proteins):

```bash
# Generate scripts (auto-detects og_placed/ > og_selected/)
fungalphylo protsetphylo phylo-slurm \
  --family-id mfs_sugar /path/to/project

# Then run on login node:
bash runs/<run_id>/slurm/phylo_orchestrate.sh
# Rerun after completion to process remaining OGs

# Or generate + submit in one step
fungalphylo protsetphylo phylo-slurm \
  --family-id mfs_sugar --submit \
  /path/to/project

# With IQ-TREE fast mode for quicker inference
fungalphylo protsetphylo phylo-slurm \
  --family-id mfs_sugar --iqtree-fast \
  /path/to/project

# Custom per-step resources (e.g., for very large OGs)
fungalphylo protsetphylo phylo-slurm \
  --family-id mfs_sugar --align-time 06:00:00\
  --align-cpus 16 --tree-time 24:00:00\
  --tree-mem-per-cpu 8G /path/to/project
```

**How it works**: The orchestrator chains three array submissions with `--dependency=afterok:`, each with different SLURM resources:

| Step | Tool | Default resources |
|------|------|-------------------|
| `align` | MAFFT `--auto` | 8 CPUs, 4G/cpu, 4h |
| `trim` | trimAl | 1 CPU, 4G, 15min |
| `tree` | IQ-TREE `-m TEST` | 8 CPUs, 4G/cpu, 8h |

The worker script uses a `--step` flag to run only one phase per submission. Each step checks if its output already exists (step-level resume). SLURM holds subsequent steps until all tasks in the previous step complete. Same cap-and-resubmit orchestrator pattern as species tree `phylo-slurm`.

Key differences from species tree `phylo-slurm`:
- **MAFFT `--auto`** (adapts algorithm to OG size) instead of `--retree 2 --maxiterate 1000`
- **Chained submissions** with separate resource allocations per step
- **`--iqtree-fast`** option for faster tree inference on large OGs
- Input auto-detected from `og_placed/` (with standalone placements) or `og_selected/`

Output per OG in `runs/<run_id>/gene_trees/<OG_ID>/`.

#### 9. Taxonomy for gene families

Export a taxonomy template for portal + standalone species, fill in `ncbi_taxon_id`, and apply to resolve full lineages:

```bash
# Export template (portal species get ncbi_taxon_id pre-filled from DB)
fungalphylo taxonomy export --family-id mfs_sugar /path/to/project

# Edit families/mfs_sugar/config/taxonomy_template.tsv:
#   Fill in ncbi_taxon_id for standalone species (portals are already filled)

# Apply — resolves full lineages (phylum→species) from NCBI taxdump
fungalphylo taxonomy apply --family-id mfs_sugar /path/to/project \
  families/mfs_sugar/config/taxonomy_template.tsv

# Dry run to validate first
fungalphylo taxonomy apply --family-id mfs_sugar --dry-run /path/to/project \
  families/mfs_sugar/config/taxonomy_template.tsv
```

The apply step reads `ncbi_taxon_id` values, resolves lineages from the NCBI taxdump (run `taxonomy fetch-ncbi` first), and writes `families/<family_id>/config/taxonomy.tsv` with rank columns. This file is used by `tree-export` for color bars.

#### 10. Visualize gene trees with `tree-export`

Render gene trees as annotated PDF/SVG with node numbers, taxonomy color bars, group annotations, and characterized-gene landmarks:

```bash
# Basic export (auto-detect latest phylo run)
fungalphylo protsetphylo tree-export \
  --family-id mfs_sugar /path/to/project

# With taxonomy color bars (repeatable --tax-level)
fungalphylo protsetphylo tree-export \
  --family-id mfs_sugar \
  --tax-level order --tax-level family \
  /path/to/project

# From a specific phylo run
fungalphylo protsetphylo tree-export \
  --family-id mfs_sugar --run-id <phylo_run_id> \
  /path/to/project
```

**What it does**:
- Numbers internal nodes: IQ-TREE output `95/88` (UFBoot/SH-aLRT) becomes `95/88/N4` (appends node index)
- Renders each OG tree as PDF + SVG with toytree/toyplot
- Taxonomy color bars: one bar per `--tax-level` (e.g., order, family)
- `group_*` columns from characterized.tsv: single-value → color bars, multi-value (semicolons) → heatmaps
- Characterized gene tips highlighted in red
- Writes numbered Newick files for downstream `clade-mark`

Output in `families/<family_id>/tree_export/`:
- `trees/<OG_ID>.pdf` / `trees/svg/<OG_ID>.svg` — Rendered trees
- `numbered_newick/<OG_ID>.nwk` — Trees with node numbers in internal labels

#### 11. Mark clades with `clade-mark`

After reviewing the numbered trees, identify clades of interest and build a species × clade gene count matrix:

```bash
# Create a TSV with columns: clade_name, og_id, node_number
# (node_number from the numbered newick / rendered tree)

fungalphylo protsetphylo clade-mark \
  --family-id mfs_sugar \
  --clade-tsv clades.tsv \
  /path/to/project
```

Input TSV format:
```
clade_name	og_id	node_number
sugar_transporters_A	OG0000001	42
sugar_transporters_B	OG0000001	58
hexose_clade	OG0000005	23
```

**What it does**:
- For each entry, finds node N42 (etc.) in the numbered Newick, collects all descendant tips
- Builds a species × clade gene count matrix (how many genes per species per clade)
- Re-renders trees with clade highlights (colored subtrees)
- Generates an iTOL `DATASET_HEATMAP` annotation file for the species tree

Output in `families/<family_id>/clade_mark/`:
- `clade_count_matrix.tsv` — Species × clade gene count matrix
- `itol_clade_heatmap.txt` — iTOL annotation file (upload to species tree)
- `trees/<OG_ID>.pdf` / `trees/svg/<OG_ID>.svg` — Trees with highlighted clades

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
    standalone/                # Characterized genes without portal_id
      <short_name>.faa         # One per outgroup species
    selection_report.tsv       # What was selected and why
  og_placed/                   # OG FASTAs with standalone genes placed (from place-standalone)
    <OG_ID>.fa                 # Copies from Orthogroup_Sequences/ + appended standalone genes
  og_report/                   # OG inspection reports (reads placements.tsv if available)
    characterized_og_matrix.*  # Characterized genes x OGs (TSV + HTML)
    portal_og_matrix.*         # Portal gene counts x OGs (TSV + HTML)
    og_decisions.txt           # Editable include/merge template
  og_selected/                 # OG FASTAs after og-apply (reads from og_placed/ or Orthogroup_Sequences/)
    <OG_ID>.fa                 # Included OGs
    merge_<OG_ID>.fa           # Merged OG groups
  place_standalone/            # HMM placement working directory
    placements.tsv             # Placement report (read by og-report)
  tree_export/                 # Rendered gene trees (from tree-export)
    trees/<OG_ID>.pdf          # Per-OG PDFs with annotations
    trees/svg/<OG_ID>.svg      # Per-OG SVGs
    numbered_newick/<OG_ID>.nwk # Trees with N-numbered internal nodes
  clade_mark/                  # Clade analysis (from clade-mark)
    clade_count_matrix.tsv     # Species × clade gene count matrix
    itol_clade_heatmap.txt     # iTOL annotation for species tree
    trees/<OG_ID>.pdf          # Trees with highlighted clades
  manifest.json
```

---

## Taxonomy & QC Reports

```bash
# Fetch NCBI taxon IDs
fungalphylo taxonomy fetch-ncbi /path/to/project

# Export/edit/apply taxonomy mapping
fungalphylo taxonomy export --approved-only --out review/portal_taxonomy.tsv /path/to/project
fungalphylo taxonomy apply review/portal_taxonomy.tsv /path/to/project

# Generate BUSCO QC report ordered by taxonomy
fungalphylo busco ingest-results --run-id <run_id> /path/to/project
fungalphylo taxonomy busco-mockup --summary-rank family /path/to/project
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
  env_path: "/path/to/of2_tykky/bin"  # Tykky container bin directory
  command: "orthofinder"
  msa_program: "mafft"
hmmer:
  hmmbuild_cmd: "hmmbuild"         # On Puhti: module load biokit
  hmmsearch_cmd: "hmmsearch"
```

When `bin_dir` or `env_path` is set, generated SLURM scripts add `export PATH="<path>:$PATH"`. When empty, scripts use `module load <tool>` instead. On Puhti, most tools are available via `module load` (e.g., `module load blast` before running `protsetphylo select`).

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
| `--max-concurrent` | phylo-slurm, protsetphylo phylo-slurm | Max concurrent array tasks (default: 100) |
| `--max-array-size` | phylo-slurm, protsetphylo phylo-slurm | Max tasks per submission (default: 380) |
| `--iqtree-fast` | protsetphylo phylo-slurm | Use IQ-TREE `-fast` mode |
| `--align-time`, `--tree-time`, etc. | protsetphylo phylo-slurm | Per-step SLURM resource overrides |
| `--og-only` | orthofinder-slurm | Stop after orthogroup sequences (`-M msa -os`) |
| `--orientation` | protsetphylo og-report | Table orientation: horizontal or vertical |
| `--no-placed` | protsetphylo og-report, og-apply | Ignore og_placed/ and placements.tsv |
| `--force` | init, protsetphylo init | Overwrite existing project/family |
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
