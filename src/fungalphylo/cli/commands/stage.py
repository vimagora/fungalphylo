from __future__ import annotations

import csv
import json
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import typer
from rich.progress import (
    Progress,
    BarColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TextColumn,
    MofNCompleteColumn,
)

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.hash import hash_json, sha256_file, write_checksums_tsv
from fungalphylo.core.ids import new_staging_id
from fungalphylo.core.idmap import PortalIdMap, load_id_map, resolve_id_map_file
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.core.validate import validate_fasta_headers_are_canonical
from fungalphylo.db.db import connect, init_db
from fungalphylo.db.queries import fetch_approvals_with_files

app = typer.Typer(help="Stage approved proteomes/CDS into immutable staging snapshots.")

PORTAL_WIDTH = 18
JGI_PIPE_RE = re.compile(r"^jgi\|([^|]+)\|(\d+)\|([^|\s]+)")
NA_TOKENS = {"", "na", "n/a", "#n/d", "#na", "null", "none", "nan"}

def is_na_value(v: str | None) -> bool:
    if v is None:
        return True
    return v.strip().lower() in NA_TOKENS


def strip_trailing_stop_aa(seq: str) -> str:
    # remove trailing stop(s) only
    return seq.rstrip("*")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_default_idmap(project_dir: Path, cfg: dict, explicit: Optional[Path]) -> Optional[Path]:
    if explicit is not None:
        return explicit.expanduser().resolve()
    default_rel = cfg.get("staging", {}).get("default_idmaps_dir", "idmaps")
    candidate = (project_dir / default_rel).expanduser().resolve()
    return candidate if candidate.exists() else None


def parse_jgi_pipe_header(header: str) -> tuple[str, str, str]:
    first = header.split()[0]
    m = JGI_PIPE_RE.match(first)
    if not m:
        raise ValueError(f"Not a JGI pipe header: {header!r}")
    return m.group(1), m.group(2), m.group(3)


def extract_model_token(header: str) -> str:
    h = header.strip()
    return re.split(r"[\s|]+", h, maxsplit=1)[0]


def detect_header_mode(fasta_path: Path, probe_n: int = 25) -> str:
    n = 0
    jgi_hits = 0
    for rec in iter_fasta(fasta_path):
        n += 1
        try:
            parse_jgi_pipe_header(rec.header)
            jgi_hits += 1
        except ValueError:
            pass
        if n >= probe_n:
            break
    if n == 0:
        raise ValueError(f"Empty FASTA: {fasta_path}")
    return "jgi_pipe" if jgi_hits == n else "non_jgi"


def write_sample_headers(path: Path, out_txt: Path, n: int = 20) -> None:
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for i, rec in enumerate(iter_fasta(path)):
        lines.append(">" + rec.header)
        if i + 1 >= n:
            break
    out_txt.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _lookup_non_jgi_canon(rec_header: str, idmap: PortalIdMap) -> tuple[Optional[str], Optional[str]]:
    if rec_header in idmap.header_to_canon:
        return idmap.header_to_canon[rec_header], None
    token = extract_model_token(rec_header)
    if token in idmap.model_to_canon:
        return idmap.model_to_canon[token], token
    return None, token


def stage_proteome_jgi(
    *,
    in_path: Path,
    out_path: Path,
    portal_id: str,
    min_len: int,
    max_len: int,
    map_writer: csv.writer,
) -> tuple[dict, dict]:
    stats = defaultdict(int)
    model_to_canon: dict[str, str] = {}
    out_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        p_in, protein_num, model_id = parse_jgi_pipe_header(rec.header)
        if p_in != portal_id:
            stats["portal_mismatch"] += 1

        seq = strip_trailing_stop_aa(rec.sequence)
        L = len(seq)
        if L < min_len:
            stats["dropped_too_short"] += 1
            continue
        if L > max_len:
            stats["dropped_too_long"] += 1
            continue

        canon = f"{portal_id}|{protein_num}"
        model_to_canon[model_id] = canon
        map_writer.writerow([canon, rec.header, L, model_id])
        out_records.append(FastaRecord(header=canon, sequence=seq))
        stats["kept"] += 1

    write_fasta(out_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats), model_to_canon


def stage_cds_jgi(
    *,
    in_path: Path,
    out_path: Path,
    portal_id: str,
    model_to_canon: dict[str, str],
) -> dict:
    stats = defaultdict(int)
    out_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        try:
            p_in, _tx_num, model_id = parse_jgi_pipe_header(rec.header)
        except ValueError:
            stats["dropped_non_jgi_header"] += 1
            continue

        if p_in != portal_id:
            stats["portal_mismatch"] += 1

        canon = model_to_canon.get(model_id)
        if canon is None:
            stats["dropped_no_protein_match"] += 1
            continue

        out_records.append(FastaRecord(header=canon, sequence=rec.sequence))
        stats["kept"] += 1

    write_fasta(out_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats)


def stage_proteome_non_jgi(
    *,
    in_path: Path,
    out_path: Path,
    portal_id: str,
    min_len: int,
    max_len: int,
    idmap: PortalIdMap,
    map_writer: csv.writer,
) -> tuple[dict, dict]:
    stats = defaultdict(int)
    token_to_canon: dict[str, str] = {}
    out_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        canon, token = _lookup_non_jgi_canon(rec.header, idmap)
        if canon is None:
            stats["dropped_missing_in_idmap"] += 1
            continue

        if is_na_value(canon):
            stats["dropped_na_mapping"] += 1
            continue

        seq = strip_trailing_stop_aa(rec.sequence)
        L = len(seq)
        if L < min_len:
            stats["dropped_too_short"] += 1
            continue
        if L > max_len:
            stats["dropped_too_long"] += 1
            continue

        map_writer.writerow([canon, rec.header, L, token or ""])
        out_records.append(FastaRecord(header=canon, sequence=seq))
        stats["kept"] += 1
        if token:
            token_to_canon[token] = canon

    write_fasta(out_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats), token_to_canon


def stage_cds_non_jgi(
    *,
    in_path: Path,
    out_path: Path,
    token_to_canon: dict[str, str],
    idmap_cds: Optional[PortalIdMap],
) -> dict:
    stats = defaultdict(int)
    out_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        canon: Optional[str] = None
        if idmap_cds and rec.header in idmap_cds.header_to_canon:
            canon = idmap_cds.header_to_canon[rec.header]
        else:
            token = extract_model_token(rec.header)
            canon = token_to_canon.get(token)

        if canon is None or is_na_value(canon):
            stats["dropped_no_protein_match"] += 1
            continue

        out_records.append(FastaRecord(header=canon, sequence=rec.sequence))
        stats["kept"] += 1

    write_fasta(out_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats)


def load_token_to_canon_map(map_path: Path) -> dict[str, str]:
    token_to_canon: dict[str, str] = {}
    with map_path.open("r", encoding="utf-8", newline="") as mf:
        reader = csv.DictReader(mf, delimiter="\t")
        for row in reader:
            token = (row.get("model_id_or_token") or "").strip()
            canon = (row.get("canonical_protein_id") or "").strip()
            if token and canon:
                token_to_canon[token] = canon
    return token_to_canon


def artifact_cache_key(payload: dict) -> str:
    return hash_json(payload)


def link_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def find_reusable_artifact(conn, *, kind: str, cache_key: str):
    row = conn.execute(
        """
        SELECT staging_id, artifact_path, artifact_sha256, source_file_id
        FROM staging_files
        WHERE kind=? AND artifact_cache_key=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (kind, cache_key),
    ).fetchone()
    return row


def insert_staging_file(
    rows: list[dict],
    *,
    staging_id: str,
    portal_id: str,
    kind: str,
    source_file_id: str,
    raw_sha256: str,
    artifact_path: str,
    artifact_sha256: str,
    artifact_cache_key: str,
    reused_from_staging_id: Optional[str],
    params: dict,
) -> None:
    rows.append(
        {
            "staging_id": staging_id,
            "portal_id": portal_id,
            "kind": kind,
            "source_file_id": source_file_id,
            "raw_sha256": raw_sha256,
            "artifact_path": artifact_path,
            "artifact_sha256": artifact_sha256,
            "artifact_cache_key": artifact_cache_key,
            "reused_from_staging_id": reused_from_staging_id,
            "created_at": _now_iso(),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        }
    )


def write_snapshot_checksums(snapshot_dir: Path, project_dir: Path, out_path: Path) -> None:
    rows: list[tuple[str, str]] = []
    for path in sorted(snapshot_dir.rglob("*")):
        if not path.is_file():
            continue
        if path == out_path:
            continue
        rows.append((str(path.relative_to(project_dir)), sha256_file(path)))
    write_checksums_tsv(rows, out_path)


@app.callback(invoke_without_command=True)
def stage_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Stage only specific portal IDs."),
    min_aa: Optional[int] = typer.Option(None, "--min-aa", help="Override staging.min_aa."),
    max_aa: Optional[int] = typer.Option(None, "--max-aa", help="Override staging.max_aa."),
    probe_n: int = typer.Option(25, "--probe-n", help="Headers to probe when detecting JGI header mode."),
    id_map: Optional[Path] = typer.Option(None, "--id-map", help="Mapping for non-JGI portals (dir or TSV)."),
    id_map_cds: Optional[Path] = typer.Option(None, "--id-map-cds", help="Optional CDS mapping for non-JGI portals."),
    overwrite: bool = typer.Option(False, "--overwrite", help="Force regeneration instead of reusing equivalent artifacts."),
    continue_on_error: bool = typer.Option(True, "--continue-on-error/--fail-fast", help="Continue after portal errors."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preflight only (no writes)."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    base_cfg = load_yaml(paths.config_yaml)
    overrides = {"staging": {}}
    if min_aa is not None:
        overrides["staging"]["min_aa"] = min_aa
    if max_aa is not None:
        overrides["staging"]["max_aa"] = max_aa
    cfg = resolve_config(project_config=base_cfg, cli_overrides=overrides)

    stg_cfg = cfg["staging"]
    raw_layout = stg_cfg["raw_layout"]
    min_len = int(stg_cfg["min_aa"])
    max_len = int(stg_cfg["max_aa"])

    resolved_id_map = resolve_default_idmap(project_dir, cfg, id_map)
    resolved_id_map_cds = resolve_default_idmap(project_dir, cfg, id_map_cds) or resolved_id_map

    errors_log = paths.errors_log

    conn = connect(paths.db_path)
    try:
        approvals = fetch_approvals_with_files(conn, portal_ids=portal_id)
        if not approvals:
            raise typer.BadParameter("No approved portals found. Run `review apply` first.")

        staging_id = new_staging_id()
        snapshot_dir = paths.staging_dir(staging_id)
        proteomes_dir = paths.staging_proteomes_dir(staging_id)
        cds_dir = paths.staging_cds_dir(staging_id)
        reports_dir = paths.staging_reports_dir(staging_id)
        generated_idmaps_dir = paths.staging_generated_idmaps_dir(staging_id)

        if not dry_run:
            for path in (snapshot_dir, proteomes_dir, cds_dir, reports_dir, generated_idmaps_dir):
                path.mkdir(parents=True, exist_ok=True)

        actions: List[dict] = []
        failures: List[dict] = []
        staging_rows: list[dict] = []

        with Progress(
            TextColumn("Portal:"),
            TextColumn("{task.fields[p]:<18}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Staging", total=len(approvals), p="-" * PORTAL_WIDTH)

            for a in approvals:
                pid = a["portal_id"]
                progress.update(task, p=(pid[:PORTAL_WIDTH]).ljust(PORTAL_WIDTH))

                try:
                    prot_file_id = a["proteome_file_id"]
                    prot_filename = a["proteome_filename"]
                    raw_prot = resolve_raw_path(
                        project_dir,
                        raw_layout=raw_layout,
                        portal_id=pid,
                        file_id=prot_file_id,
                        filename=prot_filename,
                    )
                    if not raw_prot.exists():
                        raise FileNotFoundError(f"Missing raw proteome: {raw_prot}")

                    prot_mode = detect_header_mode(raw_prot, probe_n=probe_n)
                    prot_raw_sha256 = sha256_file(raw_prot)

                    prot_idmap_path: Optional[Path] = None
                    prot_idmap_sha256: Optional[str] = None
                    if prot_mode != "jgi_pipe":
                        if resolved_id_map is None:
                            if dry_run:
                                typer.echo(f"[dry-run] {pid}: proteome_mode=non_jgi idmap=missing")
                                continue
                            sample = reports_dir / f"sample_headers_{pid}_proteome.txt"
                            write_sample_headers(raw_prot, sample, n=20)
                            raise RuntimeError(f"{pid}: non-JGI headers require idmap. See {sample}")
                        prot_idmap_path = resolve_id_map_file(resolved_id_map, pid, kind="proteome")
                        prot_idmap_sha256 = sha256_file(prot_idmap_path)

                    prot_cache_payload = {
                        "schema_version": 1,
                        "kind": "proteome",
                        "portal_id": pid,
                        "source_file_id": prot_file_id,
                        "raw_sha256": prot_raw_sha256,
                        "min_aa": min_len,
                        "max_aa": max_len,
                        "probe_n": probe_n,
                        "header_mode": prot_mode,
                        "id_map_sha256": prot_idmap_sha256,
                    }
                    prot_cache_key = artifact_cache_key(prot_cache_payload)
                    prot_reusable = None if overwrite else find_reusable_artifact(conn, kind="proteome", cache_key=prot_cache_key)

                    out_prot = proteomes_dir / f"{pid}.faa"
                    map_out = paths.staging_generated_protein_id_map(staging_id, pid)
                    model_or_token_to_canon: dict[str, str] = {}
                    prot_action = "staged"
                    prot_params = {
                        "mode": prot_mode,
                        "min_aa": min_len,
                        "max_aa": max_len,
                        "id_map": str(prot_idmap_path) if prot_idmap_path else None,
                    }

                    if dry_run:
                        reuse_state = "reuse" if prot_reusable else "generate"
                        typer.echo(f"[dry-run] {pid}: proteome_mode={prot_mode} proteome_action={reuse_state}")
                        continue

                    if prot_reusable:
                        prev_prot = project_dir / prot_reusable["artifact_path"]
                        prev_map = paths.staging_generated_protein_id_map(prot_reusable["staging_id"], pid)
                        if prev_prot.exists() and prev_map.exists():
                            link_or_copy(prev_prot, out_prot)
                            link_or_copy(prev_map, map_out)
                            model_or_token_to_canon = load_token_to_canon_map(map_out)
                            prot_action = "reused"
                        else:
                            prot_reusable = None

                    if prot_reusable is None:
                        with map_out.open("w", encoding="utf-8", newline="") as mf:
                            mw = csv.writer(mf, delimiter="\t")
                            mw.writerow(["canonical_protein_id", "original_header", "length_aa", "model_id_or_token"])

                            if prot_mode == "jgi_pipe":
                                prot_stats, model_or_token_to_canon = stage_proteome_jgi(
                                    in_path=raw_prot,
                                    out_path=out_prot,
                                    portal_id=pid,
                                    min_len=min_len,
                                    max_len=max_len,
                                    map_writer=mw,
                                )
                            else:
                                pmap = load_id_map(resolved_id_map, pid, kind="proteome")
                                prot_stats, model_or_token_to_canon = stage_proteome_non_jgi(
                                    in_path=raw_prot,
                                    out_path=out_prot,
                                    portal_id=pid,
                                    min_len=min_len,
                                    max_len=max_len,
                                    idmap=pmap,
                                    map_writer=mw,
                                )
                                missing = prot_stats.get("dropped_missing_in_idmap", 0)
                                if missing:
                                    actions.append(
                                        {
                                            "portal_id": pid,
                                            "kind": "proteome",
                                            "action": "warn",
                                            "reason": f"dropped_missing_in_idmap={missing}",
                                        }
                                    )

                    prot_artifact_sha256 = sha256_file(out_prot)
                    insert_staging_file(
                        staging_rows,
                        staging_id=staging_id,
                        portal_id=pid,
                        kind="proteome",
                        source_file_id=prot_file_id,
                        raw_sha256=prot_raw_sha256,
                        artifact_path=str(out_prot.relative_to(project_dir)),
                        artifact_sha256=prot_artifact_sha256,
                        artifact_cache_key=prot_cache_key,
                        reused_from_staging_id=(prot_reusable["staging_id"] if prot_reusable else None),
                        params=prot_params,
                    )
                    actions.append(
                        {
                            "portal_id": pid,
                            "kind": "proteome",
                            "action": prot_action,
                            "file_id": prot_file_id,
                            "out": str(out_prot.relative_to(project_dir)),
                        }
                    )

                    if a["cds_file_id"] and a["cds_filename"]:
                        cds_file_id = a["cds_file_id"]
                        cds_filename = a["cds_filename"]
                        raw_cds = resolve_raw_path(
                            project_dir,
                            raw_layout=raw_layout,
                            portal_id=pid,
                            file_id=cds_file_id,
                            filename=cds_filename,
                        )
                        if not raw_cds.exists():
                            raise FileNotFoundError(f"Missing raw CDS/transcript: {raw_cds}")

                        cds_raw_sha256 = sha256_file(raw_cds)
                        cds_idmap_path: Optional[Path] = None
                        cds_idmap_sha256: Optional[str] = None
                        cds_map_obj: Optional[PortalIdMap] = None

                        if prot_mode != "jgi_pipe" and resolved_id_map_cds is not None:
                            try:
                                cds_idmap_path = resolve_id_map_file(resolved_id_map_cds, pid, kind="cds")
                                cds_idmap_sha256 = sha256_file(cds_idmap_path)
                                cds_map_obj = load_id_map(resolved_id_map_cds, pid, kind="cds")
                            except Exception:
                                cds_idmap_path = None
                                cds_idmap_sha256 = None
                                cds_map_obj = None

                        cds_cache_payload = {
                            "schema_version": 1,
                            "kind": "cds",
                            "portal_id": pid,
                            "source_file_id": cds_file_id,
                            "raw_sha256": cds_raw_sha256,
                            "proteome_cache_key": prot_cache_key,
                            "proteome_artifact_sha256": prot_artifact_sha256,
                            "header_mode": prot_mode,
                            "id_map_cds_sha256": cds_idmap_sha256,
                        }
                        cds_cache_key = artifact_cache_key(cds_cache_payload)
                        cds_reusable = None if overwrite else find_reusable_artifact(conn, kind="cds", cache_key=cds_cache_key)

                        out_cds = cds_dir / f"{pid}.fna"
                        cds_mode = "jgi_pipe(model->protein)" if prot_mode == "jgi_pipe" else "non_jgi(header/token->canon)"

                        if cds_reusable:
                            prev_cds = project_dir / cds_reusable["artifact_path"]
                            if prev_cds.exists():
                                link_or_copy(prev_cds, out_cds)
                                cds_action = "reused"
                            else:
                                cds_reusable = None
                                cds_action = "staged"
                        else:
                            cds_action = "staged"

                        if cds_reusable is None:
                            if prot_mode == "jgi_pipe":
                                _ = stage_cds_jgi(
                                    in_path=raw_cds,
                                    out_path=out_cds,
                                    portal_id=pid,
                                    model_to_canon=model_or_token_to_canon,
                                )
                            else:
                                _ = stage_cds_non_jgi(
                                    in_path=raw_cds,
                                    out_path=out_cds,
                                    token_to_canon=model_or_token_to_canon,
                                    idmap_cds=cds_map_obj,
                                )

                        cds_artifact_sha256 = sha256_file(out_cds)
                        insert_staging_file(
                            staging_rows,
                            staging_id=staging_id,
                            portal_id=pid,
                            kind="cds",
                            source_file_id=cds_file_id,
                            raw_sha256=cds_raw_sha256,
                            artifact_path=str(out_cds.relative_to(project_dir)),
                            artifact_sha256=cds_artifact_sha256,
                            artifact_cache_key=cds_cache_key,
                            reused_from_staging_id=(cds_reusable["staging_id"] if cds_reusable else None),
                            params={"mode": cds_mode, "id_map_cds": str(cds_idmap_path) if cds_idmap_path else None},
                        )
                        actions.append(
                            {
                                "portal_id": pid,
                                "kind": "cds",
                                "action": cds_action,
                                "file_id": cds_file_id,
                                "out": str(out_cds.relative_to(project_dir)),
                            }
                        )

                except Exception as e:
                    log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, **exception_record(e)})
                    failures.append({"portal_id": pid, "reason": f"{type(e).__name__}: {e}"})
                    if not continue_on_error:
                        raise
                finally:
                    progress.advance(task)

        if failures:
            failed_report = reports_dir / "failed_portals.tsv"
            with failed_report.open("w", encoding="utf-8", newline="") as f:
                w = csv.writer(f, delimiter="\t")
                w.writerow(["portal_id", "reason"])
                for r in failures:
                    w.writerow([r["portal_id"], r["reason"]])

        if dry_run:
            log_event(
                project_dir,
                {
                    "ts": _now_iso(),
                    "event": "stage",
                    "staging_id": staging_id,
                    "dry_run": True,
                    "n_actions": len(actions),
                    "n_failures": len(failures),
                },
            )
            typer.echo("Dry-run complete (no snapshot written).")
            return

        manifest = {
            "staging_id": staging_id,
            "created_at": _now_iso(),
            "thresholds": {"min_aa": min_len, "max_aa": max_len},
            "raw_layout": raw_layout,
            "probe_n": probe_n,
            "id_map": str(resolved_id_map) if resolved_id_map else None,
            "id_map_cds": str(resolved_id_map_cds) if resolved_id_map_cds else None,
            "overwrite": overwrite,
            "actions": actions,
            "failures": failures,
            "outputs": {
                "proteomes_dir": str(proteomes_dir.relative_to(project_dir)),
                "cds_dir": str(cds_dir.relative_to(project_dir)),
                "idmaps_generated_dir": str(generated_idmaps_dir.relative_to(project_dir)),
                "reports_dir": str(reports_dir.relative_to(project_dir)),
            },
        }
        write_manifest(paths.staging_manifest(staging_id), manifest)
        write_snapshot_checksums(snapshot_dir, project_dir, paths.staging_checksums(staging_id))

        if not dry_run:
            manifest_rel = str(paths.staging_manifest(staging_id).relative_to(project_dir))
            manifest_sha256 = sha256_file(paths.staging_manifest(staging_id))
            conn.execute(
                """
                INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256)
                VALUES(?,?,?,?)
                """,
                (staging_id, _now_iso(), manifest_rel, manifest_sha256),
            )
            for row in staging_rows:
                conn.execute(
                    """
                    INSERT INTO staging_files(
                      staging_id, portal_id, kind, source_file_id, raw_sha256,
                      artifact_path, artifact_sha256, artifact_cache_key,
                      reused_from_staging_id, created_at, params_json
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["staging_id"],
                        row["portal_id"],
                        row["kind"],
                        row["source_file_id"],
                        row["raw_sha256"],
                        row["artifact_path"],
                        row["artifact_sha256"],
                        row["artifact_cache_key"],
                        row["reused_from_staging_id"],
                        row["created_at"],
                        row["params_json"],
                    ),
                )
            conn.commit()

        log_event(
            project_dir,
            {"ts": _now_iso(), "event": "stage", "staging_id": staging_id, "n_actions": len(actions), "n_failures": len(failures)},
        )

        typer.echo(f"Stage run recorded: {staging_id}")
        typer.echo(f"Manifest: {paths.staging_manifest(staging_id)}")
        typer.echo(f"Proteomes: {proteomes_dir}")
        if failures:
            typer.echo(f"Failures: {len(failures)} (see staging/{staging_id}/reports)")
    finally:
        conn.close()
