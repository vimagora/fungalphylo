from __future__ import annotations

import csv
import json
import os
import re
import shutil
from collections import defaultdict
from pathlib import Path

from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.hash import hash_json, sha256_file, write_checksums_tsv
from fungalphylo.core.idmap import PortalIdMap
from fungalphylo.core.ids import now_iso
from fungalphylo.core.validate import validate_fasta_headers_are_canonical

JGI_PIPE_RE = re.compile(r"^jgi\|([^|]+)\|(\d+)\|([^|\s]+)")
NA_TOKENS = {"", "na", "n/a", "#n/d", "#na", "null", "none", "nan"}


def is_na_value(v: str | None) -> bool:
    if v is None:
        return True
    return v.strip().lower() in NA_TOKENS


def strip_trailing_stop_aa(seq: str) -> str:
    return seq.rstrip("*")


def has_internal_stop(seq: str) -> bool:
    """Check if a sequence has internal stop codons (asterisks not at the end)."""
    stripped = seq.rstrip("*")
    return "*" in stripped


def resolve_default_idmap(project_dir: Path, cfg: dict, explicit: Path | None) -> Path | None:
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


def _lookup_non_jgi_canon(
    rec_header: str, idmap: PortalIdMap
) -> tuple[str | None, str | None]:
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
    internal_stop: str = "drop",
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

        if has_internal_stop(seq):
            stats["internal_stop"] += 1
            if internal_stop == "drop":
                stats["dropped_internal_stop"] += 1
                continue
            elif internal_stop == "strip":
                seq = seq.replace("*", "")
            # "warn" — keep as-is, just counted above

        length = len(seq)
        if length < min_len:
            stats["dropped_too_short"] += 1
            continue
        if length > max_len:
            stats["dropped_too_long"] += 1
            continue

        canon = f"{portal_id}|{protein_num}"
        model_to_canon[model_id] = canon
        map_writer.writerow([canon, rec.header, length, model_id])
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
    internal_stop: str = "drop",
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

        if has_internal_stop(seq):
            stats["internal_stop"] += 1
            if internal_stop == "drop":
                stats["dropped_internal_stop"] += 1
                continue
            elif internal_stop == "strip":
                seq = seq.replace("*", "")
            # "warn" — keep as-is, just counted above

        length = len(seq)
        if length < min_len:
            stats["dropped_too_short"] += 1
            continue
        if length > max_len:
            stats["dropped_too_long"] += 1
            continue

        map_writer.writerow([canon, rec.header, length, token or ""])
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
    idmap_cds: PortalIdMap | None,
) -> dict:
    stats = defaultdict(int)
    out_records: list[FastaRecord] = []

    for rec in iter_fasta(in_path):
        stats["records_total"] += 1
        canon: str | None = None
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
    reused_from_staging_id: str | None,
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
            "created_at": now_iso(),
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
