from __future__ import annotations

import csv
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, write_fasta
from fungalphylo.core.hash import hash_json
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect, init_db

PFAM_RE = re.compile(r"^PF\d{5}$")
SAFE_FAMILY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
REQUIRED_COLUMNS = {"portal_id", "species", "short_name", "protein_name", "sequence"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_header_part(text: str) -> str:
    """Replace whitespace with underscores, strip leading/trailing underscores."""
    return re.sub(r"\s+", "_", text.strip()).strip("_")


def _validate_pfam(accession: str) -> str:
    accession = accession.strip()
    if not PFAM_RE.match(accession):
        raise typer.BadParameter(f"Invalid Pfam accession: {accession!r} (expected PFxxxxx)")
    return accession


def _read_pfam_list(path: Path) -> list[str]:
    accessions: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            accessions.append(_validate_pfam(stripped))
    return accessions


def _read_characterized_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            raise typer.BadParameter(f"Characterized TSV is empty: {path}")
        fieldnames = set(reader.fieldnames)
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            raise typer.BadParameter(
                f"Characterized TSV missing required columns: {', '.join(sorted(missing))}. "
                f"Found: {', '.join(sorted(fieldnames))}"
            )
        rows = list(reader)
    if not rows:
        raise typer.BadParameter(f"Characterized TSV has no data rows: {path}")
    return rows


def _rows_to_fasta(rows: list[dict[str, str]]) -> list[FastaRecord]:
    records: list[FastaRecord] = []
    seen_headers: set[str] = set()
    for i, row in enumerate(rows, start=1):
        short_name = _sanitize_header_part(row["short_name"])
        protein_name = _sanitize_header_part(row["protein_name"])
        if not short_name or not protein_name:
            raise typer.BadParameter(
                f"Row {i}: short_name or protein_name is empty after sanitization"
            )
        header = f"{short_name}|{protein_name}"
        if header in seen_headers:
            raise typer.BadParameter(f"Row {i}: duplicate header {header!r}")
        seen_headers.add(header)

        seq = row["sequence"].strip()
        if not seq:
            raise typer.BadParameter(f"Row {i}: sequence is empty for {header!r}")
        records.append(FastaRecord(header=header, sequence=seq))
    return records


def init_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Safe identifier for the gene family"),
    characterized: Path = typer.Option(
        ..., "--characterized", help="TSV with characterized proteins (portal_id, species, short_name, protein_name, sequence)"
    ),
    pfam: Optional[list[str]] = typer.Option(None, "--pfam", help="Target Pfam accession (repeatable)"),
    pfam_list: Optional[Path] = typer.Option(None, "--pfam-list", help="File with one Pfam accession per line"),
) -> None:
    """Initialize a gene family for phylogenomic analysis."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    # Validate family_id
    if not SAFE_FAMILY_ID_RE.match(family_id):
        raise typer.BadParameter(
            f"Invalid family_id: {family_id!r}. "
            "Must start with alphanumeric and contain only [A-Za-z0-9_.-]"
        )

    # Check family doesn't already exist
    family_dir = paths.family_dir(family_id)
    if family_dir.exists():
        raise typer.BadParameter(f"Family directory already exists: {family_dir}")

    # Collect Pfam accessions
    pfam_accessions: list[str] = []
    if pfam:
        for p in pfam:
            pfam_accessions.append(_validate_pfam(p))
    if pfam_list:
        if not pfam_list.exists():
            raise typer.BadParameter(f"Pfam list file not found: {pfam_list}")
        pfam_accessions.extend(_read_pfam_list(pfam_list))
    # Deduplicate while preserving order
    pfam_accessions = list(dict.fromkeys(pfam_accessions))
    if not pfam_accessions:
        raise typer.BadParameter("At least one Pfam accession is required (--pfam or --pfam-list)")

    # Validate and parse characterized TSV
    characterized = characterized.expanduser().resolve()
    if not characterized.exists():
        raise typer.BadParameter(f"Characterized TSV not found: {characterized}")
    rows = _read_characterized_tsv(characterized)
    fasta_records = _rows_to_fasta(rows)

    # Create directory structure
    char_dir = paths.family_characterized_dir(family_id)
    config_dir = paths.family_config_dir(family_id)
    for d in [char_dir, config_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Copy TSV
    tsv_dest = char_dir / "characterized.tsv"
    shutil.copy2(characterized, tsv_dest)

    # Write FASTA
    fasta_dest = char_dir / "characterized.faa"
    write_fasta(fasta_records, fasta_dest)

    # Write pfams.txt
    pfams_path = config_dir / "pfams.txt"
    pfams_path.write_text("\n".join(pfam_accessions) + "\n", encoding="utf-8")

    # Write manifest
    created_at = _now_iso()
    manifest_data = {
        "family_id": family_id,
        "created_at": created_at,
        "pfams": pfam_accessions,
        "characterized_tsv": str(tsv_dest.relative_to(project_dir)),
        "characterized_fasta": str(fasta_dest.relative_to(project_dir)),
        "n_characterized": len(fasta_records),
    }
    manifest_path = paths.family_manifest(family_id)
    write_manifest(manifest_path, manifest_data)
    manifest_sha256 = hash_json(manifest_data)

    # Insert DB row
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO families(family_id, created_at, pfams, characterized_tsv,
                                 characterized_fasta, manifest_path, manifest_sha256)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                family_id,
                created_at,
                json.dumps(pfam_accessions),
                str(tsv_dest.relative_to(project_dir)),
                str(fasta_dest.relative_to(project_dir)),
                str(manifest_path.relative_to(project_dir)),
                manifest_sha256,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "protsetphylo_init",
            "family_id": family_id,
            "pfams": pfam_accessions,
            "n_characterized": len(fasta_records),
            "characterized_tsv": str(tsv_dest),
            "characterized_fasta": str(fasta_dest),
        },
    )

    typer.echo(f"Initialized family: {family_id}")
    typer.echo(f"  Pfams:          {', '.join(pfam_accessions)}")
    typer.echo(f"  Characterized:  {len(fasta_records)} proteins")
    typer.echo(f"  Directory:      {family_dir}")
