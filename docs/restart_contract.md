# Restart Contract

This document defines the intended rerun and completion semantics for the implemented intake commands in `fungalphylo`.

Scope:

- `fetch-index`
- `restore`
- `download`
- `stage`
- `busco-slurm`
- `busco ingest-results`

Principles:

- commands write durable filesystem artifacts before or alongside side effects
- reruns create new immutable batch or snapshot directories instead of mutating prior runs
- SQLite stores a compact ledger for discovery and status, but request directories and manifests remain the detailed source of truth
- when docs and code diverge, code wins until the docs are updated

Ledger boundary:

- SQLite is intentionally batch-scoped for `restore` and `download`
- one SQLite row represents one immutable request batch
- do not add per-payload child tables unless the workflow later gains a real payload-level resume/replay requirement
- detailed per-payload evidence belongs in the batch directory and JSONL logs, not in SQLite

## Shared Terms

Same work:
- the same logical input set and parameters for a command

Completion proof:
- the durable local record that a command finished successfully enough to be considered complete

Partial completion:
- some work completed and was recorded, but at least one unit failed

Skip behavior:
- conditions under which a command intentionally avoids repeating work

## `fetch-index`

Same work:
- a portal set plus the raw cached JSON or remote JGI index responses for those portals

Completion proof:
- cached JSON at `cache/jgi_index_json/<portal_id>.json`
- corresponding `portal_files` rows in SQLite
- updated `portals.dataset_id` and `portals.top_hit_id`

Partial completion:
- some portal cache files or DB upserts exist and others failed; see `logs/errors.jsonl`

Skip behavior:
- `--ingest-from-cache` skips network access entirely and treats cache files as the source of truth
- there is no global done marker; reruns simply refresh or re-ingest the selected portal set

Rerun contract:
- rerunning is safe and idempotent at the DB level because file rows are upserted
- if cache exists and you want deterministic replay, use `--ingest-from-cache`
- if remote content may have changed, rerun without `--ingest-from-cache` to refresh cache and DB state

## `restore`

Same work:
- the current approved file set, optionally filtered by `--portal-id`, chunked by `--max-chars`, with the chosen `--send-mail`, `--retries`, and `--retry-backoff-seconds` settings

Completion proof:
- request directory `restore_requests/<request_id>/`
- `payload_*.json` files for every planned payload
- `restore_requests` ledger row in SQLite
- for non-dry-run success paths, `responses.jsonl` with one line per posted payload

Partial completion:
- SQLite status `partial`
- some payloads posted successfully and others failed
- successful payload responses remain in `responses.jsonl`
- per-payload errors recorded in `logs/errors.jsonl`

Skip behavior:
- none; `restore` always creates a new request batch
- there is no dedupe against earlier restore batches or remote-side state

Rerun contract:
- `--dry-run` is the preflight mode; it builds payloads and records a `planned` batch without requiring JGI auth
- a normal rerun always writes a fresh `restore_requests/<request_id>/` directory and a new SQLite ledger row
- reruns do not resume a prior partially posted batch in place
- transient `429`/`5xx`/timeout failures are retried within a payload before the payload is marked failed
- if `--continue-on-error` is enabled, later payloads continue after a failure; otherwise the command exits on the first failure

Operator guidance:
- treat each request directory as immutable history
- use the newest batch as the active operational record unless you are explicitly auditing an older attempt
- use SQLite to discover the latest batch and high-level status, then inspect `responses.jsonl`, `payload_*.json`, and `logs/errors.jsonl` for payload-level detail

## `download`

Same work:
- the current approved file set, optionally filtered by `--portal-id`, chunked by `--max-chars`, with the chosen `--retain`, `--skip-if-raw-present`, and `--overwrite-staged` options

Completion proof:
- request directory `download_requests/<request_id>/`
- `payload_*.json` files for every planned payload
- `summary.json` for non-dry-run executions
- `download_requests` ledger row in SQLite
- matched raw files moved into `raw/<portal_id>/<file_id>/<filename>`

Partial completion:
- SQLite status `partial`
- some payloads downloaded and processed successfully and others failed
- per-payload payload definitions remain in `payload_*.json`
- manifest mismatches or move failures recorded in `download_requests/<request_id>/bundles/unmatched_manifest.tsv`
- per-payload failures recorded in `logs/errors.jsonl`

Skip behavior:
- if `--skip-if-raw-present` is set, approved files with an existing raw path are skipped; when source `md5` metadata is available, the raw file must match that checksum to be skipped
- unless `--overwrite-staged` is set, approved files already represented in any `staging_files` row are skipped by approved source file ID across any staging snapshot
- if all approved files are skipped, the command prints `Nothing to download...` and exits without creating a new request batch

Rerun contract:
- `--dry-run` builds payloads and records a `planned` batch without requiring JGI auth
- a normal rerun always creates a fresh `download_requests/<request_id>/` directory and a new SQLite ledger row
- reruns do not resume an older batch in place
- payload success is local-batch scoped; there is currently no per-payload resume ledger in SQLite
- transient `429`/`5xx`/timeout failures are retried within a payload before the payload is marked failed
- if `--continue-on-error` is enabled, later payloads continue after a failure; otherwise the command exits on the first failure

Known boundary:
- checksum-aware raw skipping currently depends on source `md5` metadata being present; otherwise `--skip-if-raw-present` falls back to path existence
- staged-snapshot skip logic intentionally uses approved source file IDs rather than checksum-aware raw state

## `stage`

Same work:
- the current approved portals, optionally filtered by `--portal-id`, plus staging parameters:
  `min_aa`, `max_aa`, `probe_n`, `id_map`, `id_map_cds`, and `overwrite`
- for each artifact, sameness is defined by the artifact cache key derived from source checksum and relevant parameters

Completion proof:
- snapshot directory `staging/<staging_id>/`
- `manifest.json`
- `checksums.tsv`
- normalized FASTA outputs under `proteomes/` and optional `cds/`
- generated protein ID maps under `idmaps/generated/`
- `stagings` and `staging_files` rows in SQLite

Partial completion:
- snapshot manifest contains `failures`
- `reports/failed_portals.tsv` exists when portal-level failures occurred under `--continue-on-error`
- successful portal artifacts remain recorded in the snapshot

Skip behavior:
- there is no whole-command skip; each non-dry-run execution creates a new `staging_id`
- artifact-level reuse occurs by cache key unless `--overwrite` is set
- reuse links or copies prior snapshot artifacts into the new snapshot instead of regenerating them

Rerun contract:
- `--dry-run` validates inputs and reports intended generate/reuse actions without writing a snapshot or SQLite rows
- a normal rerun always creates a fresh immutable snapshot directory
- cache-key-equivalent artifacts may be reused from an older snapshot, but the new snapshot still gets its own manifest and ledger rows
- if `--continue-on-error` is enabled, portal failures are logged and other portals continue; otherwise the command exits on the first portal failure

Operator guidance:
- downstream compute should target an explicit `staging_id`
- do not treat `staging/` as a mutable working directory; each child directory is immutable history

## `busco-slurm`

Same work:
- a chosen `staging_id` or the latest snapshot if none is given, plus the command options used to render the script

Completion proof:
- the generated SLURM script on disk
- `runs/<run_id>/manifest.json`
- `runs` ledger row in SQLite

Skip behavior:
- none at the workflow level; rerunning regenerates the script

Rerun contract:
- always safe to rerun
- operational identity comes from the referenced `staging_id`, not from mutable staged paths

Operator guidance:
- in local development environments without Puhti access, use `busco-slurm` to write the script for review and manual transfer
- keep the optional submit path implemented for real CSC usage, but do not make normal development or tests depend on successful `sbatch`
- batch-mode completion is represented by `runs/<run_id>/busco_results/<batch_root>/batch_summary.txt`

## `busco ingest-results`

Same work:
- a chosen BUSCO `run_id` plus its completed `batch_summary.txt`

Completion proof:
- one row per portal in SQLite `busco_results`
- the BUSCO run manifest and batch-summary file remain on disk

Partial completion:
- some BUSCO rows may be imported and others missing if the batch summary is incomplete or the filesystem outputs are only partially present

Skip behavior:
- none; rerunning replaces existing `busco_results` rows for the same `run_id`

Rerun contract:
- this is a manual post-run import step after the operator confirms that the cluster-side BUSCO run completed successfully
- rerunning is safe and idempotent at the run-summary level because existing rows for that `run_id` are replaced before import
- detailed BUSCO outputs remain on disk under `runs/<run_id>/busco_results/...`; SQLite stores only the per-portal summary layer

Operator guidance:
- do not treat BUSCO import as an automatic poller or completion detector
- first verify that the cluster-side run has produced `batch_summary.txt`
- then run `busco ingest-results` manually

## `interproscan-slurm`

Same work:
- a chosen `staging_id` or the latest snapshot if none is given, plus the command options used to render the launcher, controller, worker, and queue

Completion proof:
- generated launcher script, worker sbatch script, controller script, and `queue.tsv`
- `runs/<run_id>/manifest.json`
- `runs` ledger row in SQLite

Skip behavior:
- none at the workflow level; rerunning regenerates the run scaffolding

Rerun contract:
- always safe to rerun
- operational identity comes from the referenced `staging_id` and `run_id`, not from mutable staged paths
- `--limit` intentionally changes the queued proteome set for debug-sized runs
- for the current Puhti `cluster_interproscan` wrapper path, the generated worker uses one explicit `-o` output file and therefore supports only a single `TSV` format

Operator guidance:
- the launcher now runs a submit-and-poll controller that submits one worker at a time with `sbatch --parsable`, records the child job ID in `queue.tsv`, polls that exact job with `squeue`/`sacct`, and only then advances
- on Puhti, the worker script loads `biokit` and `interproscan` modules before running `cluster_interproscan`
- local development should still treat this as a write-first command unless explicit `--submit` behavior is being tested on CSC

## Status Interpretation

`status` summarizes the latest restore and download ledger rows and the latest staging snapshot.

Use it to answer:

- what was the latest batch or snapshot
- whether the latest restore/download run is `planned`, `running`, `completed`, `partial`, or `failed`
- whether approved raw files are present, checksum-mismatched, or missing at the configured raw layout

Do not use `status` as the only forensic source for failures. For details, inspect:

- `restore_requests/<request_id>/`
- `download_requests/<request_id>/`
- `staging/<staging_id>/manifest.json`
- `logs/errors.jsonl`
