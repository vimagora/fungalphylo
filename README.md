# fungalphylo

A reproducible, user-friendly phylogenomics pipeline for fungal datasets built around:

- **SQLite** for state/approvals (what exists + what’s approved)
- **Immutable staging snapshots** (`staging_id`) for normalized inputs
- **Snakemake** for compute steps (later)
- **JGI Files API** for file discovery, restore, and download
- **TSV review loop** for human-in-the-loop selection

This repo is designed so **download is I/O only** (keeps raw artifacts), while **stage** performs normalization (IDs, filtering, mapping manifests).

---

## Requirements

- Python **3.11+**
- Packages (installed via `pyproject.toml`):
  - `typer`, `pydantic`, `pyyaml`, `openpyxl`, `requests`, `rich`

---

## Install

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

pip install -U pip
pip install -e ".[dev]"
```

---

## Authentication (JGI)

Set your JGI session token as an environment variable.

### Linux/macOS
```bash
export JGI_TOKEN="/api/sessions/<your_token_here>"
```

### Windows PowerShell
```powershell
$env:JGI_TOKEN="/api/sessions/<your_token_here>"
```

You can also pass `--token` to commands that support it.

---

## Project layout (created by `init`)

A fungalphylo **project directory** is separate from the repo (code). Example:

```
project/
  config.yaml
  db/fungalphylo.sqlite
  raw/                         # downloaded originals (backup)
  staging/<staging_id>/        # normalized staged FASTA + maps + manifest
  runs/<run_id>/               # compute runs (later)
  review/                      # human-editable TSVs
  cache/jgi_index_json/        # cached JGI search responses
  logs/events.jsonl
  logs/errors.jsonl
  restore_requests/<timestamp>/
  download_requests/<timestamp>/
```

---

## Typical workflow (end-to-end)

### 1) Initialize a project

```bash
fungalphylo init /path/to/project --force
```

### 2) Ingest MycoCosm portal list (XLSX)

Your MycoCosm spreadsheet contains hyperlinks in `Name` (portal URL) and `Published` (paper URL).

```bash
fungalphylo ingest /path/to/project --table mycocosm_portals.xlsx
```

This populates `portals` including:
- `portal_id` inferred from the `Name` hyperlink
- `is_published`, `published_text`, `published_url`

### 3) Fetch file index from JGI (portal_files)

```bash
fungalphylo fetch-index /path/to/project
# or limit to portals
fungalphylo fetch-index /path/to/project --portal-id Dicsqu464_2
```

This:
- caches raw JSON: `cache/jgi_index_json/<portal_id>.json`
- upserts file rows into `portal_files`
- stores `dataset_id` and `top_hit_id` on `portals` (used for restore/download)

### 4) Autoselect best proteome + CDS (explainable)

Heuristics include (current defaults):
- prefer `data_group=genome`
- prefer `jat_label`: proteins_filtered/cds_filtered (fallback transcripts_filtered)
- avoid `deflines`, `promoter`, `alleles`
- avoid gff
- prefer `file_format=fasta`
- prefer newer files

```bash
fungalphylo autoselect /path/to/project
```

Outputs in `review/`:
- `autoselect_<ts>.tsv` (one row/portal, chosen IDs)
- `autoselect_explain_<ts>.tsv` (top-N candidates + scoring breakdown)

### 5) Review / override selections and apply approvals

Export an editable TSV from the autoselect output:

```bash
fungalphylo review export /path/to/project --from-autoselect review/autoselect_<ts>.tsv
```

Edit the resulting `review_edit_<ts>.tsv`:
- change `proteome_file_id` or `cds_file_id`
- set `approve=no` to exclude a portal

Apply to DB approvals:

```bash
fungalphylo review apply /path/to/project review/review_edit_<ts>.tsv
```

### 6) Request restore (can take hours/days)

Restore is separate from download. By default, it requests restore for **all approved files** and emails when ready.

```bash
fungalphylo restore /path/to/project --send-mail
```

This writes payloads + responses under:
`restore_requests/<timestamp>/`

**Payloads are chunked** to stay under the backend character limit (default 3500, hard limit 4094).

### 7) Download approved files into raw cache

```bash
fungalphylo download /path/to/project
```

This:
- posts chunked download payloads to `https://files-download.jgi.doe.gov/download_files/`
- saves bundles + extracted files under `download_requests/<timestamp>/`
- copies matched files into:
  `raw/<portal_id>/<file_id>/<original_filename>`
- keeps unmatched extracted files under:
  `download_requests/<timestamp>/unmatched_files/`

### 8) Stage (normalize + map + manifest)

Staging creates an immutable `staging_id` and normalized FASTA files with canonical headers:
`{portal_id}|{jgi_protein_id}`

```bash
fungalphylo stage /path/to/project --dry-run
fungalphylo stage /path/to/project
```

Outputs:
- `staging/<staging_id>/proteomes/<portal_id>.faa.gz`
- `staging/<staging_id>/cds/<portal_id>.fna.gz`
- `staging/<staging_id>/protein_id_map.tsv.gz`
- `staging/<staging_id>/manifest.json`
- `staging/<staging_id>/checksums.tsv`
- `staging/<staging_id>/reports/*` (for non-JGI header cases)

#### Non-JGI header portals
Some portals have non-JGI FASTA headers (uniform within that proteome). These require an ID mapping file.

Recommended universal per-portal mapping file (TSV):
- `canonical_protein_id`
- `model_id`
- `original_header`
- `transcript_id` (optional)

Mapping lookup is **primary by exact original_header** (most reliable), with optional fallback via `model_id` if present.

---

## Logging

- `logs/events.jsonl` — one-line JSON records for major actions
- `logs/errors.jsonl` — structured error records for long batch steps (fetch/download)

---

## Helpful flags

- `--dry-run` on staging/download/restore to build payloads and validate inputs without executing.
- `--continue-on-error` (where supported) to process large batches without stopping.

---

## Next planned steps (compute)

Once staging is stable, compute steps (OrthoFinder, InterProScan, species tree, family runs) will be added as Snakemake-backed runs in `runs/<run_id>/` with caching and STARTED/DONE markers.

---

## License

MIT (or update as needed).
