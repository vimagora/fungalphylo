# fungalphylo

A reproducible, user-friendly phylogenomics pipeline for fungal datasets built around:

- **SQLite** for state/approvals (what exists + what’s approved)
- **Immutable staging snapshots** (`staging_id`) for normalized inputs
- **Snakemake** for compute steps (later)
- **JGI Files API** for file discovery, restore, and download
- **TSV review loop** for human-in-the-loop selection

The explicit rerun/completion contract for implemented commands is documented in `docs/restart_contract.md`.

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
  tools.yaml
  db/fungalphylo.sqlite
  raw/                         # downloaded originals (backup)
  staging/<staging_id>/        # immutable normalized snapshot
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

If you already have cache files and only want to ingest them into the DB:

```bash
fungalphylo fetch-index /path/to/project --ingest-from-cache
```

### 4) Autoselect best proteome + CDS (explainable)

Heuristics include (current defaults):
- prefer `data_group=genome`
- prefer `jat_label`: proteins_filtered/cds_filtered (fallback transcripts_filtered)
- avoid `deflines`, `promoter`, `alleles`
- avoid gff
- prefer `file_format=fasta`
- prefer newer files

These defaults are now configurable in `config.yaml` under `autoselect.weights` and `autoselect.ban_patterns`.

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

### 5b) Curate NCBI taxon IDs

If you want a curated taxonomy mapping for later completeness-by-taxon analysis, apply a TSV with `portal_id` and `ncbi_taxon_id`:

```bash
fungalphylo taxonomy fetch-ncbi /path/to/project
fungalphylo taxonomy export /path/to/project --approved-only --out review/portal_taxonomy.tsv
fungalphylo taxonomy apply /path/to/project review/portal_taxonomy.tsv
fungalphylo taxonomy busco-mockup /path/to/project --summary-rank family
```

Use `--approved-only` with `taxonomy export` to build a template for just the approved portals. Use `--dry-run` with `taxonomy apply` to validate the table without writing. Blank `ncbi_taxon_id` values clear an existing assignment by default.

`taxonomy busco-mockup` reads the latest BUSCO run by default, expects a single summary TSV under `runs/<run_id>/busco_results/`, resolves lineage from the downloaded NCBI taxdump, and writes an HTML taxonomy-ordered QC report under `runs/<run_id>/reports/`. It supports `--summary-rank family|order` and highlights low-quality taxa below `--low-quality-threshold` (default `85`).

### 6) Request restore (can take hours/days)

Restore is separate from download. By default, it requests restore for **all approved files** and emails when ready.

```bash
fungalphylo restore /path/to/project --send-mail
```

This writes payloads + responses under:
`restore_requests/<timestamp>/`

**Payloads are chunked** to stay under the backend character limit (default 3500, hard limit 4094).

Restore restart behavior:
- `--dry-run` writes the payloads without requiring a token or posting requests
- normal runs always write payload JSON before any POST attempt
- `--retries` and `--retry-backoff-seconds` retry transient `429`/`5xx`/timeout restore failures before the payload is marked failed
- `--continue-on-error` logs per-payload failures to `logs/errors.jsonl` and continues posting the remaining payloads
- reruns create a new `restore_requests/<timestamp>/` directory rather than mutating an old request batch
- each restore batch is also indexed in SQLite `restore_requests` so `status` can show the latest request state without scanning directories
- SQLite stores only the batch ledger; inspect `payload_*.json`, `responses.jsonl`, and `logs/errors.jsonl` for payload-level detail

### 7) Download approved files into raw cache

```bash
fungalphylo download /path/to/project
```

This:
- posts chunked download payloads to `https://files-download.jgi.doe.gov/download_files/`
- saves bundles + extracted files under `download_requests/<timestamp>/`
- moves matched files into:
  `raw/<portal_id>/<file_id>/<original_filename>`
- writes unmatched or missing manifest rows to:
  `download_requests/<timestamp>/bundles/unmatched_manifest.tsv`

Download restart behavior:
- `--dry-run` writes payloads without requiring a token or downloading bundles
- normal runs always write payload JSON before any POST attempt and finish with `summary.json`
- `--skip-if-raw-present` skips approved files already present at the configured raw path and verifies md5 when source metadata provides one
- `--overwrite-staged` disables the default skip for approved source file IDs already represented in any staging snapshot
- `--retries` and `--retry-backoff-seconds` retry transient `429`/`5xx`/timeout download failures before the payload is marked failed
- `--continue-on-error` logs per-payload failures to `logs/errors.jsonl` and continues with the remaining payloads
- reruns create a new `download_requests/<timestamp>/` directory rather than mutating an old request batch
- each download batch is also indexed in SQLite `download_requests` so `status` can show the latest batch outcome and counts quickly
- malformed non-zip responses and extracted bundles with no manifest are treated as per-payload failures, logged to `logs/errors.jsonl`, and reflected in the batch ledger/status
- SQLite stores only the batch ledger; inspect the request directory, manifests, `summary.json`, and `logs/errors.jsonl` for payload-level detail

### 8) Stage (normalize + map + manifest)

Staging creates an immutable `staging_id` and normalized FASTA files with canonical headers:
`{portal_id}|{jgi_protein_id}`

```bash
fungalphylo stage /path/to/project --dry-run
fungalphylo stage /path/to/project
```

Current outputs:
- `staging/<staging_id>/proteomes/<portal_id>.faa`
- `staging/<staging_id>/cds/<portal_id>.fna`
- `staging/<staging_id>/idmaps/generated/<portal_id>.protein_id_map.tsv`
- `staging/<staging_id>/manifest.json`
- `staging/<staging_id>/checksums.tsv`
- `staging/<staging_id>/reports/*` (for non-JGI header cases)

Staging is snapshot-first:
- each successful run creates a new `staging_id`
- downstream compute should target a chosen `staging_id`
- equivalent artifacts are reused internally by cache key instead of being regenerated unnecessarily

Stage restart behavior:
- `--dry-run` validates inputs and reports the intended generate/reuse action without writing a snapshot
- normal runs always create a fresh `staging_id`
- `--overwrite` disables cache-key based artifact reuse across snapshots
- failures are written to `staging/<staging_id>/reports/failed_portals.tsv` when `--continue-on-error` is active

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

For BUSCO on Puhti / SLURM:

```bash
fungalphylo busco-slurm /path/to/project --staging-id <staging_id>
```

If `--staging-id` is omitted, the latest staging snapshot is used.

Use `--submit` only on systems that actually have `sbatch` access. In local development, the intended workflow is to write the SLURM script, review it, and submit it later on Puhti.

Each BUSCO script generation also writes `runs/<run_id>/manifest.json` and records a `runs` row in SQLite.

For InterProScan on Puhti / SLURM:

```bash
fungalphylo interproscan-slurm /path/to/project --staging-id <staging_id> --application pfam
```

Repeat `--application` to request multiple InterProScan databases. The default application is `pfam`. Repeat `--format` to request multiple output formats; the default is `tsv`, and `tsv` is currently required for downstream parsing.

The command writes a launcher script, a worker script, a per-proteome queue ledger, and a run manifest under `runs/<run_id>/`. The launcher-based design walks staged proteomes one at a time so the next `cluster_interproscan` call is not started until the previous proteome finishes, which avoids exhausting concurrent-job limits on Puhti when InterProScan submits its own cluster work internally.

Use `--submit` only on systems that actually have `sbatch` access. In local development, the intended workflow is to write the scripts, review them, and submit them later on Puhti.

---

## Next planned steps (compute)

Once staging is stable, compute steps (OrthoFinder, InterProScan, species tree, family runs) will be added as Snakemake-backed runs in `runs/<run_id>/` with caching and STARTED/DONE markers.

---

## License

MIT (or update as needed).
