# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

fungalphylo is a Python CLI for fungal phylogenomics data intake and normalization. The pipeline:
`init → ingest → fetch-index → autoselect → review → restore → download → stage → busco-slurm / interproscan-slurm`

It uses SQLite for state, immutable staging snapshots under `staging/<staging_id>/`, raw file preservation in `raw/`, and SLURM script generation for HPC compute on CSC/Puhti. Project directories (data) are separate from the repo (code).

## Build & Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Requires Python 3.11+.

## Testing

```bash
# Run all tests
pytest

# Run a single test file
pytest tests/test_stage_snapshots.py

# Run a specific test
pytest tests/test_stage_snapshots.py::test_name -v

# Focused suite (the key regression tests)
.venv/bin/pytest tests/test_restore_download.py tests/test_core_utils.py tests/test_autoselect.py tests/test_db_command.py tests/test_stage_snapshots.py tests/test_fetch_index_cache.py
```

Tests use `typer.testing.CliRunner` for CLI integration tests, `tmp_path` fixtures for isolated project directories, and `monkeypatch` for mocking external calls (sbatch, requests, JGI API).

## Linting & Formatting

```bash
ruff check .        # Lint (E, F, I, B, UP rules)
ruff format .       # Format
black --check .     # Check formatting
black .             # Format
```

Line length: 100 characters. Target: Python 3.11. Config in `pyproject.toml`.

## Quick Compile Check

```bash
python -m compileall src
```

## Architecture

### Package Layout

- `src/fungalphylo/cli/main.py` — Typer app; registers all commands
- `src/fungalphylo/cli/commands/` — One module per command (~15 commands)
- `src/fungalphylo/core/` — Shared utilities: FASTA I/O, path handling, config, hashing, events/errors, manifests, ID mapping, validation
- `src/fungalphylo/db/` — SQLite schema (`schema.sql`), connection helper, query layer

### Key Design Decisions

- **Immutable snapshots**: `staging/<staging_id>/` directories are immutable. Each `stage` run creates a new snapshot. Downstream compute targets an explicit `staging_id`.
- **Batch ledger boundary**: SQLite stores one row per restore/download batch. Per-payload detail lives in request directories and JSONL logs, not in child tables.
- **Artifact reuse**: `stage` reuses equivalent artifacts across snapshots by cache key (source checksum + parameters) rather than regenerating.
- **Restart contract**: Every implemented command has explicit rerun/skip/completion semantics documented in `agent_context/restart_contract.md`. Reruns create new immutable directories rather than mutating prior runs.
- **Compute script generation**: `busco-slurm` and `interproscan-slurm` write SLURM scripts locally. Actual `sbatch` submission only happens on Puhti with `--submit`.

### Data Flow

1. `ingest` reads tabular portal metadata → SQLite `portals` + `portal_files`
2. `fetch-index` fetches JGI file listings → cached JSON + SQLite `portal_files`
3. `autoselect` scores candidates → explainable TSVs in `review/`
4. `review export/apply` → human edits → SQLite `approvals`
5. `restore` + `download` → raw files in `raw/<portal_id>/<file_id>/`
6. `stage` → normalized FASTA in `staging/<staging_id>/proteomes/` and `cds/`
7. `busco-slurm` / `interproscan-slurm` → SLURM scripts + run manifests in `runs/<run_id>/`

### Core Modules Worth Knowing

- `core/paths.py` — `ProjectPaths` class defining the entire project directory structure
- `core/fasta.py` — FASTA reading/writing with gzip support
- `core/idmap.py` — Protein ID mapping for non-JGI headers
- `core/events.py` / `core/errors.py` — Structured JSONL logging
- `db/schema.sql` — The full SQLite schema (portals, files, approvals, stagings, staging_files, runs, busco_results, restore/download_requests, meta)

## Conventions

- Trust the code over the README or older notes when they conflict.
- Commands follow the pattern: `fungalphylo <command> /path/to/project [options]`
- Protein header canonicalization: `{portal_id}|{jgi_protein_id}`
- Timestamp-based IDs for snapshots (`staging_id`) and compute runs (`run_id`)
- `--dry-run` validates without side effects; `--continue-on-error` for batch resilience
- `fungalphylo failures <project>` inspects failed batches, staging failures, and error log entries
- Keep the project lightweight: Typer + SQLite + JSON/TSV manifests + plain files. Avoid premature abstractions.
- Add tests before or alongside behavior changes.
- CSC/Puhti considerations: deterministic paths, durable manifests, resumable batch steps, no interactive prompts.

## Context Documents

- `agent_context/project_master.md` — Durable technical overview, architecture, risks, backlog
- `agent_context/project_live.md` — Current working state, recent completions, next work order
- `agent_context/restart_contract.md` — Rerun/skip/completion semantics for all implemented commands
