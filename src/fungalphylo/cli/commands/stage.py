from __future__ import annotations

import csv
import gzip
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import typer
from rich.progress import (
    Progress,
    BarColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    TextColumn,
    MofNCompleteColumn,
)

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.hash import sha256_file, write_checksums_tsv
from fungalphylo.core.ids import new_staging_id
from fungalphylo.core.idmap import load_id_map, PortalIdMap
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.core.validate import validate_fasta_headers_are_canonical
from fungalphylo.db.db import connect
from fungalphylo.db.queries import fetch_approvals_with_files

app = typer.Typer(help="Create an immutable staging snapshot from approved downloads.")

PORTAL_WIDTH = 18
JGI_PIPE_RE = re.compile(r"^jgi\|([^|]+)\|(\d+)\|([^|\s]+)")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_default_idmap(project_dir: Path, cfg: dict, explicit: Optional[Path]) -> Optional[Path]:
    """
    If user provided --id-map, use it.
    Else, if <project_dir>/<staging.default_idmaps_dir> exists, use that.
    Else return None.
    """
    if explicit is not None:
        return explicit.expanduser().resolve()

    default_rel = cfg.get("staging", {}).get("default_idmaps_dir", "idmaps")
    candidate = (project_dir / default_rel).expanduser().resolve()
    return candidate if candidate.exists() else None


def parse_jgi_pipe_header(header: str) -> tuple[str, str, str]:
    """
    Returns: (portal_in_header, numeric_id, model_id)
    Proteins:  jgi|PORTAL|PROTEIN_NUM|MODEL
    CDS/Tx:    jgi|PORTAL|TRANSCRIPT_NUM|MODEL   (numeric differs; MODEL links)
    """
    first = header.split()[0]
    m = JGI_PIPE_RE.match(first)
    if not m:
        raise ValueError(f"Not a JGI pipe header: {header!r}")
    return m.group(1), m.group(2), m.group(3)


def extract_model_token(header: str) -> str:
    """
    For non-JGI headers, take the first token before whitespace or '|'
    Example: 'PTRG_00001 | ...' -> 'PTRG_00001'
    """
    h = header.strip()
    token = re.split(r"[\s|]+", h, maxsplit=1)[0]
    return token


def detect_header_mode(fasta_path: Path, probe_n: int = 25) -> str:
    """
    Returns 'jgi_pipe' only if ALL of the first probe_n headers match JGI pipe.
    Otherwise returns 'non_jgi' (proteome-level behavior).
    """
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


def stage_proteome_jgi(
    *,
    in_path: Path,
    out_path: Path,
    portal_id: str,
    min_len: int,
    max_len: int,
    mapping_writer: csv.writer,
) -> tuple[dict, dict]:
    """
    Stage protein FASTA with canonical headers {portal_id}|{protein_num}.
    Returns: (stats, model_id -> canonical_protein_id)
    """
    stats = defaultdict(int)
    model_to_canon: dict[str, str] = {}
    staged_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        p_in, protein_num, model_id = parse_jgi_pipe_header(rec.header)
        if p_in != portal_id:
            stats["portal_mismatch"] += 1

        L = len(rec.sequence)
        if L < min_len:
            stats["dropped_too_short"] += 1
            continue
        if L > max_len:
            stats["dropped_too_long"] += 1
            continue

        canon = f"{portal_id}|{protein_num}"
        model_to_canon[model_id] = canon
        mapping_writer.writerow([canon, rec.header, L, model_id])

        staged_records.append(FastaRecord(header=canon, sequence=rec.sequence))
        stats["kept"] += 1

    write_fasta(staged_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats), model_to_canon


def stage_cds_jgi(
    *,
    in_path: Path,
    out_path: Path,
    portal_id: str,
    model_to_canon: dict[str, str],
) -> dict:
    """
    Stage CDS/transcript FASTA by mapping model_id -> canonical protein ID.
    """
    stats = defaultdict(int)
    staged_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        try:
            p_in, _transcript_num, model_id = parse_jgi_pipe_header(rec.header)
        except ValueError:
            stats["dropped_non_jgi_header"] += 1
            continue

        if p_in != portal_id:
            stats["portal_mismatch"] += 1

        canon = model_to_canon.get(model_id)
        if canon is None:
            stats["dropped_no_protein_match"] += 1
            continue

        staged_records.append(FastaRecord(header=canon, sequence=rec.sequence))
        stats["kept"] += 1

    write_fasta(staged_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats)


def _lookup_non_jgi_canon(rec_header: str, idmap: PortalIdMap) -> tuple[Optional[str], Optional[str]]:
    """
    Primary: exact original_header match (rec_header).
    Fallback: model token match (if the map has model_to_canon).
    Returns (canonical_id, model_id_or_token_for_map_tsv)
    """
    if rec_header in idmap.header_to_canon:
        return idmap.header_to_canon[rec_header], None

    token = extract_model_token(rec_header)
    if token in idmap.model_to_canon:
        return idmap.model_to_canon[token], token

    return None, token


def stage_proteome_non_jgi(
    *,
    in_path: Path,
    out_path: Path,
    portal_id: str,
    min_len: int,
    max_len: int,
    mapping_writer: csv.writer,
    idmap: PortalIdMap,
) -> tuple[dict, dict]:
    """
    Stage non-JGI proteome using idmap (header→canon primary).
    Returns: (stats, token->canon map for possible CDS token matching)
    """
    stats = defaultdict(int)
    token_to_canon: dict[str, str] = {}
    staged_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        canon, token = _lookup_non_jgi_canon(rec.header, idmap)
        if canon is None:
            stats["missing_in_id_map"] += 1
            continue

        L = len(rec.sequence)
        if L < min_len:
            stats["dropped_too_short"] += 1
            continue
        if L > max_len:
            stats["dropped_too_long"] += 1
            continue

        mapping_writer.writerow([canon, rec.header, L, token or ""])
        staged_records.append(FastaRecord(header=canon, sequence=rec.sequence))
        stats["kept"] += 1

        if token:
            token_to_canon[token] = canon

    write_fasta(staged_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats), token_to_canon


def stage_cds_non_jgi(
    *,
    in_path: Path,
    out_path: Path,
    token_to_canon: dict[str, str],
    idmap_cds: Optional[PortalIdMap],
) -> dict:
    """
    For non-JGI CDS, attempt:
      1) exact header lookup via idmap_cds.header_to_canon (if provided)
      2) token lookup via token_to_canon
    """
    stats = defaultdict(int)
    staged_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1

        canon: Optional[str] = None
        if idmap_cds and rec.header in idmap_cds.header_to_canon:
            canon = idmap_cds.header_to_canon[rec.header]
        else:
            token = extract_model_token(rec.header)
            canon = token_to_canon.get(token)

        if canon is None:
            stats["dropped_no_protein_match"] += 1
            continue

        staged_records.append(FastaRecord(header=canon, sequence=rec.sequence))
        stats["kept"] += 1

    write_fasta(staged_records, out_path)
    validate_fasta_headers_are_canonical(out_path)
    return dict(stats)


@app.callback(invoke_without_command=True)
def stage_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory created by fungalphylo init."),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Stage only specific portal IDs."),
    min_aa: Optional[int] = typer.Option(None, help="Override staging.min_aa."),
    max_aa: Optional[int] = typer.Option(None, help="Override staging.max_aa."),
    probe_n: int = typer.Option(25, "--probe-n", help="Headers to probe when detecting JGI header mode."),
    id_map: Optional[Path] = typer.Option(
        None,
        "--id-map",
        help="Mapping for non-JGI portals (dir of <portal>.tsv OR combined TSV).",
    ),
    id_map_cds: Optional[Path] = typer.Option(
        None,
        "--id-map-cds",
        help="Optional separate mapping for non-JGI CDS headers (same formats as --id-map).",
    ),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error", help="Stage what you can; log failures and continue."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preflight only: detect issues, do not write outputs."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required when calling `fungalphylo stage`.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    cfg = resolve_config(project_config=load_yaml(paths.config_yaml), cli_overrides={"staging": {}})
    if min_aa is not None:
        cfg = resolve_config(project_config=load_yaml(paths.config_yaml), cli_overrides={"staging": {"min_aa": min_aa}})
    if max_aa is not None:
        # merge carefully with possibly-set min_aa
        ov = {"staging": {}}
        if min_aa is not None:
            ov["staging"]["min_aa"] = min_aa
        ov["staging"]["max_aa"] = max_aa
        cfg = resolve_config(project_config=load_yaml(paths.config_yaml), cli_overrides=ov)

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
    finally:
        conn.close()

    if not approvals:
        raise typer.BadParameter("No approved portals found. Run `review apply` first.")

    staging_id = new_staging_id()
    stg_dir = paths.staging_dir(staging_id)
    prot_dir = paths.staging_proteomes_dir(staging_id)
    cds_dir = paths.staging_cds_dir(staging_id)
    reports_dir = stg_dir / "reports"
    failed_report = reports_dir / "failed_portals.tsv"

    if dry_run:
        typer.echo(f"[dry-run] Would create staging: {staging_id}")
    else:
        prot_dir.mkdir(parents=True, exist_ok=True)
        cds_dir.mkdir(parents=True, exist_ok=True)
        reports_dir.mkdir(parents=True, exist_ok=True)

    map_path = paths.staging_protein_id_map(staging_id)
    per_portal: Dict[str, Dict] = {}
    checksum_rows: List[Tuple[str, str]] = []
    failures: list[tuple[str, str, str]] = []

    map_f = None
    map_writer = None
    if not dry_run:
        map_path.parent.mkdir(parents=True, exist_ok=True)
        map_f = gzip.open(map_path, "wt", encoding="utf-8", newline="")
        map_writer = csv.writer(map_f, delimiter="\t")
        map_writer.writerow(["canonical_protein_id", "original_header", "length_aa", "model_id_or_token"])

    # Progress bar across portals
    with Progress(
        TextColumn("Portal:"),
        TextColumn("{task.fields[portal]:<18}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Staging", total=len(approvals), portal="-" * PORTAL_WIDTH)

        try:
            for a in approvals:
                pid = a["portal_id"]
                progress.update(task, portal=(pid[:PORTAL_WIDTH]).ljust(PORTAL_WIDTH))

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
                        msg = (
                            f"Missing raw proteome file: {raw_prot} "
                            f"(raw_layout={raw_layout!r})"
                        )
                        failures.append((pid, msg, ""))
                        log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, "stage": "raw_missing", "path": str(raw_prot)})
                        if not continue_on_error:
                            raise FileNotFoundError(msg)
                        continue

                    prot_mode = detect_header_mode(raw_prot, probe_n=probe_n)

                    # DRY-RUN preflight output
                    if dry_run:
                        needs_map = (prot_mode != "jgi_pipe")
                        map_status = "n/a"
                        if needs_map:
                            if resolved_id_map is None:
                                map_status = "missing (no idmaps dir and no --id-map)"
                            else:
                                try:
                                    _ = load_id_map(resolved_id_map, pid)
                                    map_status = f"ok ({resolved_id_map})"
                                except Exception as e:
                                    map_status = f"invalid ({resolved_id_map}): {type(e).__name__}: {e}"

                        typer.echo(f"[dry-run] {pid}: proteome_mode={prot_mode} needs_idmap={needs_map} idmap={map_status}")

                        # Check CDS/transcript raw file presence if approved
                        if a["cds_file_id"] and a["cds_filename"]:
                            raw_cds = resolve_raw_path(
                                project_dir,
                                raw_layout=raw_layout,
                                portal_id=pid,
                                file_id=a["cds_file_id"],
                                filename=a["cds_filename"],
                            )
                            cds_exists = raw_cds.exists()
                            typer.echo(f"[dry-run] {pid}: raw_cds_exists={cds_exists} path={raw_cds}")
                        else:
                            typer.echo(f"[dry-run] {pid}: no approved CDS/transcript")
                        continue

                    assert map_writer is not None

                    out_prot = prot_dir / f"{pid}.faa.gz"
                    portal_entry: Dict[str, object] = {
                        "portal_id": pid,
                        "proteome": {
                            "file_id": prot_file_id,
                            "filename": prot_filename,
                            "raw_path": str(raw_prot.relative_to(project_dir)),
                            "raw_sha256": sha256_file(raw_prot),
                            "staged_path": str(out_prot.relative_to(project_dir)),
                        },
                        "cds": None,
                    }

                    # --- Proteome staging ---
                    token_to_canon: dict[str, str] = {}
                    if prot_mode == "jgi_pipe":
                        prot_stats, model_to_canon = stage_proteome_jgi(
                            in_path=raw_prot,
                            out_path=out_prot,
                            portal_id=pid,
                            min_len=min_len,
                            max_len=max_len,
                            mapping_writer=map_writer,
                        )
                        portal_entry["proteome"]["mode"] = "jgi_pipe"  # type: ignore[index]
                        portal_entry["proteome"]["staged_sha256"] = sha256_file(out_prot)  # type: ignore[index]
                        portal_entry["proteome"]["stats"] = prot_stats  # type: ignore[index]
                        token_to_canon = model_to_canon  # model_id -> canon works as token map
                    else:
                        if resolved_id_map is None:
                            sample = reports_dir / f"sample_headers_{pid}_proteome.txt"
                            write_sample_headers(raw_prot, sample, n=20)
                            reason = "non-JGI proteome headers; provide idmap (default: project/idmaps/<portal>.tsv)"
                            failures.append((pid, reason, str(sample.relative_to(project_dir))))
                            log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, "stage": "idmap_missing", "sample": str(sample)})
                            if not continue_on_error:
                                raise RuntimeError(f"{pid}: {reason}. See {sample}")
                            continue

                        pmap = load_id_map(resolved_id_map, pid)
                        prot_stats, token_to_canon = stage_proteome_non_jgi(
                            in_path=raw_prot,
                            out_path=out_prot,
                            portal_id=pid,
                            min_len=min_len,
                            max_len=max_len,
                            mapping_writer=map_writer,
                            idmap=pmap,
                        )

                        # Non-JGI completeness: must cover all headers
                        if prot_stats.get("missing_in_id_map", 0) > 0:
                            sample = reports_dir / f"sample_headers_{pid}_proteome.txt"
                            write_sample_headers(raw_prot, sample, n=20)
                            reason = f"id-map incomplete for proteome (missing {prot_stats['missing_in_id_map']}). Provide full map."
                            failures.append((pid, reason, str(sample.relative_to(project_dir))))
                            log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, "stage": "idmap_incomplete", "missing": prot_stats["missing_in_id_map"]})
                            try:
                                out_prot.unlink(missing_ok=True)
                            except Exception:
                                pass
                            if not continue_on_error:
                                raise RuntimeError(f"{pid}: {reason}. See {sample}")
                            continue

                        portal_entry["proteome"]["mode"] = "non_jgi+idmap(header→canon)"  # type: ignore[index]
                        portal_entry["proteome"]["staged_sha256"] = sha256_file(out_prot)  # type: ignore[index]
                        portal_entry["proteome"]["stats"] = prot_stats  # type: ignore[index]

                    # --- Optional CDS/transcript staging ---
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
                            reason = f"missing raw CDS/transcript: {raw_cds}"
                            failures.append((pid, reason, ""))
                            log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, "stage": "raw_missing_cds", "path": str(raw_cds)})
                            if not continue_on_error:
                                raise FileNotFoundError(reason)
                        else:
                            out_cds = cds_dir / f"{pid}.fna.gz"
                            cds_entry: Dict[str, object] = {
                                "file_id": cds_file_id,
                                "filename": cds_filename,
                                "raw_path": str(raw_cds.relative_to(project_dir)),
                                "raw_sha256": sha256_file(raw_cds),
                                "staged_path": str(out_cds.relative_to(project_dir)),
                            }

                            if prot_mode == "jgi_pipe":
                                cds_stats = stage_cds_jgi(
                                    in_path=raw_cds,
                                    out_path=out_cds,
                                    portal_id=pid,
                                    model_to_canon=token_to_canon,  # model_id -> canon
                                )
                                cds_entry["mode"] = "jgi_pipe(model_id→protein)"
                            else:
                                cds_map_obj = None
                                if resolved_id_map_cds is not None:
                                    try:
                                        cds_map_obj = load_id_map(resolved_id_map_cds, pid)
                                    except Exception:
                                        cds_map_obj = None

                                cds_stats = stage_cds_non_jgi(
                                    in_path=raw_cds,
                                    out_path=out_cds,
                                    token_to_canon=token_to_canon,
                                    idmap_cds=cds_map_obj,
                                )

                                # If we dropped entries and no cds map provided, likely tokens differ.
                                if cds_stats.get("dropped_no_protein_match", 0) > 0 and resolved_id_map_cds is None:
                                    sample = reports_dir / f"sample_headers_{pid}_cds.txt"
                                    write_sample_headers(raw_cds, sample, n=20)
                                    reason = (
                                        f"CDS tokens didn't match proteome mapping (dropped {cds_stats['dropped_no_protein_match']}). "
                                        f"Provide --id-map-cds for exact CDS header mapping."
                                    )
                                    failures.append((pid, reason, str(sample.relative_to(project_dir))))
                                    log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, "stage": "cds_mapping_needed", "dropped": cds_stats["dropped_no_protein_match"]})
                                    try:
                                        out_cds.unlink(missing_ok=True)
                                    except Exception:
                                        pass
                                    if not continue_on_error:
                                        raise RuntimeError(f"{pid}: {reason}. See {sample}")
                                    cds_entry = None
                                else:
                                    cds_entry["mode"] = "non_jgi(header/token→canon)"

                            if cds_entry:
                                cds_entry["staged_sha256"] = sha256_file(out_cds)
                                cds_entry["stats"] = cds_stats
                                portal_entry["cds"] = cds_entry

                    per_portal[pid] = portal_entry

                except Exception as e:
                    # Portal-level exception handling
                    log_error_jsonl(
                        errors_log,
                        {
                            "event": "stage_error",
                            "portal_id": pid,
                            "stage": "portal_exception",
                            **exception_record(e),
                        },
                    )
                    failures.append((pid, f"{type(e).__name__}: {e}", ""))
                    if not continue_on_error:
                        raise

                finally:
                    progress.advance(task)

        finally:
            if map_f is not None:
                map_f.close()

    if dry_run:
        typer.echo("[dry-run] Done.")
        return

    # Write failures report
    if failures:
        reports_dir.mkdir(parents=True, exist_ok=True)
        with failed_report.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["portal_id", "reason", "sample_headers_path"])
            for pid, reason, sample in failures:
                w.writerow([pid, reason, sample])

    # checksums
    checksum_rows.append((str(map_path.relative_to(project_dir)), sha256_file(map_path)))
    for pid, info in per_portal.items():
        prot_rel = info["proteome"]["staged_path"]  # type: ignore[index]
        checksum_rows.append((prot_rel, sha256_file(project_dir / prot_rel)))
        cds_info = info.get("cds")
        if cds_info and isinstance(cds_info, dict) and cds_info.get("staged_path"):
            cds_rel = cds_info["staged_path"]  # type: ignore[index]
            checksum_rows.append((cds_rel, sha256_file(project_dir / cds_rel)))

    manifest = {
        "staging_id": staging_id,
        "created_at": _now_iso(),
        "protein_id_scheme": cfg["staging"]["protein_id_scheme"],
        "thresholds": {"min_aa": min_len, "max_aa": max_len},
        "raw_layout": raw_layout,
        "probe_n": probe_n,
        "id_map": str(resolved_id_map) if resolved_id_map else None,
        "id_map_cds": str(resolved_id_map_cds) if resolved_id_map_cds else None,
        "portals": per_portal,
        "failures_report": str(failed_report.relative_to(project_dir)) if failures else None,
        "artifacts": {
            "protein_id_map": str(map_path.relative_to(project_dir)),
            "checksums_tsv": str(paths.staging_checksums(staging_id).relative_to(project_dir)),
            "reports_dir": str(reports_dir.relative_to(project_dir)),
        },
    }

    manifest_path = paths.staging_manifest(staging_id)
    write_manifest(manifest_path, manifest)

    manifest_sha = sha256_file(manifest_path)
    checksum_rows.append((str(manifest_path.relative_to(project_dir)), manifest_sha))

    checksums_path = paths.staging_checksums(staging_id)
    write_checksums_tsv(checksum_rows, checksums_path)

    # Register staging in DB
    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) VALUES(?,?,?,?)",
            (staging_id, _now_iso(), str(manifest_path.relative_to(project_dir)), manifest_sha),
        )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": _now_iso(),
            "event": "stage",
            "staging_id": staging_id,
            "n_portals": len(per_portal),
            "n_failures": len(failures),
            "min_aa": min_len,
            "max_aa": max_len,
            "manifest_path": str(manifest_path),
        },
    )

    typer.echo(f"Staging complete: {staging_id}")
    typer.echo(f"Manifest: {manifest_path}")
    typer.echo(f"Proteomes: {prot_dir}")
    typer.echo(f"Protein ID map: {map_path}")
    if failures:
        typer.echo(f"⚠ Some portals failed staging. See: {failed_report}")