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
busco-slurm          # Quality control
interproscan-slurm   # Domain annotation
protsetphylo         # Gene family phylogenomics (new)
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

## Compute: Gene Family Phylogenomics (`protsetphylo`)

Analyze specific gene families (e.g., MFS sugar transporters) across your staged proteomes.

### Pipeline

```
protsetphylo init → interproscan → select → build-fasta → align → tree
```

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

- `portal_id` can be blank for proteins not in your project (e.g., outgroup sequences)
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

Outputs per-portal FASTAs and `selection_report.tsv` in `families/<family_id>/selected/`.

#### 4. Build merged FASTA

```bash
fungalphylo protsetphylo build-fasta /path/to/project \
  --family-id mfs_sugar
```

Merges characterized and selected proteins:
- Characterized proteins with `portal_id` + `protein_id` replace their selected counterparts
- Characterized without `portal_id` are included as standalone sequences
- Optional `--redundancy-tool cdhit|mmseqs2 --identity-threshold 0.95`

Outputs `families/<family_id>/fasta/combined.faa` plus per-portal FASTAs.

#### 5. Align

```bash
fungalphylo protsetphylo align /path/to/project \
  --family-id mfs_sugar \
  --account project_xxx \
  --submit
```

Generates a SLURM script that runs MAFFT (`--auto`) then trimAl (`-automated1`). Outputs in `families/<family_id>/alignment/`.

#### 6. Build phylogenetic tree

```bash
fungalphylo protsetphylo tree /path/to/project \
  --family-id mfs_sugar \
  --tree-method iqtree \
  --account project_xxx \
  --submit

# Or with FastTree
fungalphylo protsetphylo tree /path/to/project \
  --family-id mfs_sugar \
  --tree-method fasttree \
  --account project_xxx \
  --submit
```

IQ-TREE defaults: `-m MFP -bb 1000 -nt AUTO`. Override with `--model` and `--bootstrap`. Output in `families/<family_id>/tree/`.

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
    <portal_id>.faa            # Selected proteins per portal
    selection_report.tsv       # What was selected and why
  fasta/
    <portal_id>.faa            # Merged per-portal
    combined.faa               # All sequences for alignment
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
  bin_dir: "/path/to/busco/bin"  # optional
  command: "busco"
interproscan:
  bin_dir: ""                     # optional, modules loaded in job
  command: "cluster_interproscan"
mafft:
  command: "mafft"                # module loaded in job
trimal:
  command: "trimal"
iqtree:
  command: "iqtree2"              # module loaded in job
```

On Puhti, most tools are available via `module load` (handled in generated SLURM scripts).

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
| `--submit` | busco-slurm, interproscan-slurm, protsetphylo | Submit SLURM job after writing |
| `--resume-run-id` | busco-slurm, interproscan-slurm | Resume a timed-out run |
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
