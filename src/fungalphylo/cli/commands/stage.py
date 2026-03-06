from __future__ import annotations

import csv
import json
import re
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
from fungalphylo.core.hash import sha256_file
from fungalphylo.core.ids import new_staging_id
from fungalphylo.core.idmap import load_id_map, PortalIdMap
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.core.validate import validate_fasta_headers_are_canonical
from fungalphylo.db.db import connect
from fungalphylo.db.queries import fetch_approvals_with_files

app = typer.Typer(help="Stage approved proteomes/CDS into shared staged/ outputs with DB-backed skip logic.")

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


def staged_status(conn, portal_id: str, kind: str) -> Optional[str]:
    row = conn.execute(
        "SELECT file_id FROM staged_files WHERE portal_id=? AND kind=?",
        (portal_id, kind),
    ).fetchone()
    return row["file_id"] if row else None


def upsert_staged(
    conn,
    *,
    portal_id: str,
    kind: str,
    file_id: str,
    raw_sha256: str,
    staged_path: str,
    staged_sha256: str,
    params: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO staged_files(portal_id, kind, file_id, raw_sha256, staged_path, staged_sha256, created_at, params_json)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(portal_id, kind) DO UPDATE SET
          file_id=excluded.file_id,
          raw_sha256=excluded.raw_sha256,
          staged_path=excluded.staged_path,
          staged_sha256=excluded.staged_sha256,
          created_at=excluded.created_at,
          params_json=excluded.params_json
        """,
        (
            portal_id,
            kind,
            file_id,
            raw_sha256,
            staged_path,
            staged_sha256,
            _now_iso(),
            json.dumps(params, ensure_ascii=False, sort_keys=True),
        ),
    )


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
    overwrite: bool = typer.Option(False, "--overwrite", help="Allow overwriting staged/<portal>.faa/.fna if file_id changed."),
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

    staged_prot_dir = project_dir / "staged" / "proteomes"
    staged_cds_dir = project_dir / "staged" / "cds"
    idmaps_gen_dir = project_dir / "idmaps" / "generated"
    staged_prot_dir.mkdir(parents=True, exist_ok=True)
    staged_cds_dir.mkdir(parents=True, exist_ok=True)
    idmaps_gen_dir.mkdir(parents=True, exist_ok=True)

    errors_log = paths.errors_log

    conn = connect(paths.db_path)
    try:
        approvals = fetch_approvals_with_files(conn, portal_ids=portal_id)
    finally:
        conn.close()

    if not approvals:
        raise typer.BadParameter("No approved portals found. Run `review apply` first.")

    staging_id = new_staging_id()
    run_dir = paths.staging_dir(staging_id)
    reports_dir = run_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    actions: List[dict] = []
    failures: List[dict] = []

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

                conn = connect(paths.db_path)
                try:
                    existing = staged_status(conn, pid, "proteome")
                finally:
                    conn.close()

                if existing == prot_file_id and not overwrite:
                    actions.append({"portal_id": pid, "kind": "proteome", "action": "skipped", "reason": "already_staged_same_file_id"})
                    stage_prot = False
                elif existing is not None and existing != prot_file_id and not overwrite:
                    raise RuntimeError(
                        f"{pid}: proteome already staged from file_id={existing}. Current approval is {prot_file_id}. Use --overwrite."
                    )
                else:
                    stage_prot = True

                if dry_run:
                    needs_map = (prot_mode != "jgi_pipe")
                    map_status = "n/a"
                    if needs_map:
                        if resolved_id_map is None:
                            map_status = "missing"
                        else:
                            try:
                                _ = load_id_map(resolved_id_map, pid)
                                map_status = "ok"
                            except Exception as e:
                                map_status = f"invalid:{type(e).__name__}"
                    typer.echo(f"[dry-run] {pid}: proteome_mode={prot_mode} stage_proteome={stage_prot} idmap={map_status}")
                    progress.advance(task)
                    continue

                out_prot = staged_prot_dir / f"{pid}.faa"
                map_out = idmaps_gen_dir / f"{pid}.{prot_file_id}.protein_id_map.tsv"

                model_or_token_to_canon: dict[str, str] = {}

                if stage_prot:
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
                            mode_note = "jgi_pipe"
                        else:
                            if resolved_id_map is None:
                                sample = reports_dir / f"sample_headers_{pid}_proteome.txt"
                                write_sample_headers(raw_prot, sample, n=20)
                                raise RuntimeError(f"{pid}: non-JGI headers require idmap. See {sample}")

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
                                actions.append({"portal_id": pid, "kind": "proteome", "action": "warn", "reason": f"dropped_missing_in_idmap={missing}"})
                            mode_note = "non_jgi+idmap"

                    conn = connect(paths.db_path)
                    try:
                        upsert_staged(
                            conn,
                            portal_id=pid,
                            kind="proteome",
                            file_id=prot_file_id,
                            raw_sha256=sha256_file(raw_prot),
                            staged_path=str(out_prot.relative_to(project_dir)),
                            staged_sha256=sha256_file(out_prot),
                            params={"min_aa": min_len, "max_aa": max_len, "mode": mode_note, "id_map": str(resolved_id_map) if resolved_id_map else None},
                        )
                        conn.commit()
                    finally:
                        conn.close()

                    actions.append({"portal_id": pid, "kind": "proteome", "action": "staged", "file_id": prot_file_id, "out": str(out_prot)})

                else:
                    if map_out.exists():
                        with map_out.open("r", encoding="utf-8", newline="") as mf:
                            rr = csv.DictReader(mf, delimiter="\t")
                            for row in rr:
                                tok = (row.get("model_id_or_token") or "").strip()
                                canon = (row.get("canonical_protein_id") or "").strip()
                                if tok and canon:
                                    model_or_token_to_canon[tok] = canon

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

                    conn = connect(paths.db_path)
                    try:
                        existing_cds = staged_status(conn, pid, "cds")
                    finally:
                        conn.close()

                    if existing_cds == cds_file_id and not overwrite:
                        actions.append({"portal_id": pid, "kind": "cds", "action": "skipped", "reason": "already_staged_same_file_id"})
                    elif existing_cds is not None and existing_cds != cds_file_id and not overwrite:
                        raise RuntimeError(f"{pid}: cds already staged from file_id={existing_cds}. Current approval is {cds_file_id}. Use --overwrite.")
                    else:
                        out_cds = staged_cds_dir / f"{pid}.fna"
                        if prot_mode == "jgi_pipe":
                            _ = stage_cds_jgi(
                                in_path=raw_cds,
                                out_path=out_cds,
                                portal_id=pid,
                                model_to_canon=model_or_token_to_canon,
                            )
                            cds_mode = "jgi_pipe(model→protein)"
                        else:
                            cds_map_obj = None
                            if resolved_id_map_cds is not None:
                                try:
                                    cds_map_obj = load_id_map(resolved_id_map_cds, pid, kind="cds")
                                except Exception:
                                    cds_map_obj = None

                            _ = stage_cds_non_jgi(
                                in_path=raw_cds,
                                out_path=out_cds,
                                token_to_canon=model_or_token_to_canon,
                                idmap_cds=cds_map_obj,
                            )
                            cds_mode = "non_jgi(header/token→canon)"

                        conn = connect(paths.db_path)
                        try:
                            upsert_staged(
                                conn,
                                portal_id=pid,
                                kind="cds",
                                file_id=cds_file_id,
                                raw_sha256=sha256_file(raw_cds),
                                staged_path=str(out_cds.relative_to(project_dir)),
                                staged_sha256=sha256_file(out_cds),
                                params={"mode": cds_mode, "id_map_cds": str(resolved_id_map_cds) if resolved_id_map_cds else None},
                            )
                            conn.commit()
                        finally:
                            conn.close()

                        actions.append({"portal_id": pid, "kind": "cds", "action": "staged", "file_id": cds_file_id, "out": str(out_cds)})

            except Exception as e:
                log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, **exception_record(e)})
                failures.append({"portal_id": pid, "reason": f"{type(e).__name__}: {e}"})
                if not continue_on_error:
                    raise
            finally:
                progress.advance(task)

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
            "staged_proteomes_dir": str(staged_prot_dir.relative_to(project_dir)),
            "staged_cds_dir": str(staged_cds_dir.relative_to(project_dir)),
            "idmaps_generated_dir": str(idmaps_gen_dir.relative_to(project_dir)),
        },
    }
    write_manifest(paths.staging_manifest(staging_id), manifest)

    if failures:
        failed_report = (run_dir / "reports" / "failed_portals.tsv")
        failed_report.parent.mkdir(parents=True, exist_ok=True)
        with failed_report.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["portal_id", "reason"])
            for r in failures:
                w.writerow([r["portal_id"], r["reason"]])

    log_event(project_dir, {"ts": _now_iso(), "event": "stage", "staging_id": staging_id, "n_actions": len(actions), "n_failures": len(failures)})

    typer.echo(f"Stage run recorded: {staging_id}")
    typer.echo(f"Manifest: {paths.staging_manifest(staging_id)}")
    if failures:
        typer.echo(f"⚠ Failures: {len(failures)} (see staging/{staging_id}/reports)")