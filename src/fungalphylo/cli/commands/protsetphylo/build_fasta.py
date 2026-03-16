from __future__ import annotations

import csv
import subprocess
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.ids import now_iso
from fungalphylo.core.manifest import read_manifest, write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect, init_db


def _read_characterized_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _sanitize_header_part(text: str) -> str:
    import re

    return re.sub(r"\s+", "_", text.strip()).strip("_")


def build_fasta_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Family to build FASTA for"),
    redundancy_tool: str | None = typer.Option(
        None,
        "--redundancy-tool",
        help="Tool for redundancy removal: cdhit or mmseqs2",
    ),
    identity_threshold: float = typer.Option(
        0.95, "--identity-threshold", help="Sequence identity threshold for redundancy removal"
    ),
) -> None:
    """Merge characterized and selected proteins into per-portal and combined FASTAs."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    if redundancy_tool and redundancy_tool not in ("cdhit", "mmseqs2"):
        raise typer.BadParameter(f"--redundancy-tool must be cdhit or mmseqs2. Got: {redundancy_tool!r}")

    # Verify family exists
    conn = connect(paths.db_path)
    try:
        family_row = conn.execute(
            "SELECT * FROM families WHERE family_id = ?", (family_id,)
        ).fetchone()
    finally:
        conn.close()
    if family_row is None:
        raise typer.BadParameter(f"Family not found: {family_id!r}")

    char_dir = paths.family_characterized_dir(family_id)
    char_tsv_path = char_dir / "characterized.tsv"
    char_fasta_path = char_dir / "characterized.faa"
    selected_dir = paths.family_selected_dir(family_id)
    fasta_dir = paths.family_fasta_dir(family_id)
    fasta_dir.mkdir(parents=True, exist_ok=True)

    if not char_tsv_path.exists() or not char_fasta_path.exists():
        raise typer.BadParameter(f"Characterized files not found for family {family_id!r}")

    # Read characterized TSV for portal_id and protein_id info
    char_rows = _read_characterized_tsv(char_tsv_path)

    # Build lookup: {short_name|protein_name: row}
    char_records = list(iter_fasta(char_fasta_path))
    char_by_header: dict[str, FastaRecord] = {r.header: r for r in char_records}

    # Build protein_id replacement map: portal_id -> set of protein_ids to remove
    # and portal_id -> list of characterized records to add
    portal_replacements: dict[str, set[str]] = {}
    portal_char_records: dict[str, list[FastaRecord]] = {}
    standalone_records: list[FastaRecord] = []

    for row in char_rows:
        short_name = _sanitize_header_part(row["short_name"])
        protein_name = _sanitize_header_part(row["protein_name"])
        header = f"{short_name}|{protein_name}"
        char_rec = char_by_header.get(header)
        if char_rec is None:
            continue

        portal_id = row.get("portal_id", "").strip()
        protein_id_raw = row.get("protein_id", "").strip()

        if not portal_id:
            # Standalone characterized protein (no portal)
            standalone_records.append(char_rec)
            continue

        if portal_id not in portal_replacements:
            portal_replacements[portal_id] = set()
            portal_char_records[portal_id] = []
        portal_char_records[portal_id].append(char_rec)

        if protein_id_raw:
            # Explicit protein IDs to replace (semicolon-separated)
            for pid in protein_id_raw.split(";"):
                pid = pid.strip()
                if pid:
                    portal_replacements[portal_id].add(pid)
                    # Also add with portal prefix for matching
                    portal_replacements[portal_id].add(f"{portal_id}|{pid}")

    # Build per-portal merged FASTAs
    all_records: list[FastaRecord] = []
    seen_headers: set[str] = set()

    for selected_faa in sorted(selected_dir.glob("*.faa")) if selected_dir.exists() else []:
        if selected_faa.name == "selection_report.tsv":
            continue
        portal_id = selected_faa.stem
        records_for_portal: list[FastaRecord] = []

        # Remove proteins that are being replaced by characterized
        replace_ids = portal_replacements.get(portal_id, set())
        for rec in iter_fasta(selected_faa):
            if rec.header in replace_ids:
                continue
            # Also check parsed protein_id
            parts = rec.header.split("|", 1)
            pid = parts[1] if len(parts) == 2 else parts[0]
            if pid in replace_ids:
                continue
            records_for_portal.append(rec)

        # Add characterized proteins for this portal
        for char_rec in portal_char_records.get(portal_id, []):
            records_for_portal.append(char_rec)

        if records_for_portal:
            portal_fasta = fasta_dir / f"{portal_id}.faa"
            write_fasta(records_for_portal, portal_fasta)
            for rec in records_for_portal:
                if rec.header not in seen_headers:
                    all_records.append(rec)
                    seen_headers.add(rec.header)

    # Add standalone characterized records
    for rec in standalone_records:
        standalone_fasta = fasta_dir / f"{rec.header.split('|')[0]}.faa"
        write_fasta([rec], standalone_fasta)
        if rec.header not in seen_headers:
            all_records.append(rec)
            seen_headers.add(rec.header)

    # Add characterized records that weren't associated with any portal in selected/
    for _portal_id, char_recs in portal_char_records.items():
        for rec in char_recs:
            if rec.header not in seen_headers:
                all_records.append(rec)
                seen_headers.add(rec.header)

    if not all_records:
        raise typer.BadParameter(f"No sequences to combine for family {family_id!r}")

    # Write combined FASTA
    combined_path = fasta_dir / "combined.faa"
    write_fasta(all_records, combined_path)

    # Optional redundancy removal
    if redundancy_tool:
        dedup_path = fasta_dir / "combined.dedup.faa"
        if redundancy_tool == "cdhit":
            try:
                subprocess.run(
                    [
                        "cd-hit",
                        "-i", str(combined_path),
                        "-o", str(dedup_path),
                        "-c", str(identity_threshold),
                        "-M", "0",
                        "-T", "0",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                dedup_path.replace(combined_path)
                # Clean up cd-hit cluster file
                clstr = Path(str(dedup_path) + ".clstr")
                if clstr.exists():
                    clstr.unlink()
            except FileNotFoundError:
                raise typer.BadParameter("cd-hit not found on PATH") from None
        elif redundancy_tool == "mmseqs2":
            try:
                tmp_dir = fasta_dir / "mmseqs_tmp"
                tmp_dir.mkdir(exist_ok=True)
                subprocess.run(
                    [
                        "mmseqs", "easy-cluster",
                        str(combined_path),
                        str(dedup_path),
                        str(tmp_dir),
                        "--min-seq-id", str(identity_threshold),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                rep_seqs = Path(str(dedup_path) + "_rep_seq.fasta")
                if rep_seqs.exists():
                    rep_seqs.replace(combined_path)
                import shutil

                shutil.rmtree(tmp_dir, ignore_errors=True)
            except FileNotFoundError:
                raise typer.BadParameter("mmseqs not found on PATH") from None

    n_combined = sum(1 for _ in iter_fasta(combined_path))

    # Update manifest
    manifest_path = paths.family_manifest(family_id)
    manifest = read_manifest(manifest_path)
    manifest["build_fasta"] = {
        "n_combined": n_combined,
        "redundancy_tool": redundancy_tool,
        "identity_threshold": identity_threshold if redundancy_tool else None,
        "combined_fasta": str(combined_path.relative_to(project_dir)),
    }
    write_manifest(manifest_path, manifest)

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_build_fasta",
            "family_id": family_id,
            "n_combined": n_combined,
            "redundancy_tool": redundancy_tool,
        },
    )

    typer.echo(f"Built FASTA for family: {family_id}")
    typer.echo(f"  Combined sequences: {n_combined}")
    typer.echo(f"  Output:             {combined_path}")
