from __future__ import annotations

import csv
import json
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

import typer

from fungalphylo.core.domain_arch import build_domain_architectures, compute_max_evalues
from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.ids import now_iso
from fungalphylo.core.ipr_tsv import IprHit, filter_by_accessions, parse_ipr_tsv
from fungalphylo.core.manifest import read_manifest, write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import resolve_staging_id
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db


def _find_project_ipr_run(
    paths: ProjectPaths, explicit_run_id: str | None
) -> tuple[str, Path]:
    """Find the project-wide InterProScan run and return (run_id, results_root)."""
    conn = connect(paths.db_path)
    try:
        if explicit_run_id:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ? AND kind = 'interproscan'",
                (explicit_run_id,),
            ).fetchone()
            if row is None:
                raise typer.BadParameter(
                    f"InterProScan run not found: {explicit_run_id!r}"
                )
            run_id = explicit_run_id
        else:
            row = conn.execute(
                "SELECT * FROM runs WHERE kind = 'interproscan' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise typer.BadParameter(
                    "No project-wide InterProScan run found. "
                    "Run `fungalphylo interproscan-slurm` first or pass --project-ipr-run-id."
                )
            run_id = row["run_id"]
    finally:
        conn.close()

    manifest_path = paths.root / row["manifest_path"]
    manifest = read_manifest(manifest_path)
    results_root = paths.root / manifest["paths"]["results_root"]
    return run_id, results_root


def _collect_portal_ipr_hits(
    results_root: Path, target_pfams: set[str]
) -> dict[str, list[IprHit]]:
    """Scan portal IPR result TSVs and collect hits matching target Pfams."""
    portal_hits: dict[str, list[IprHit]] = defaultdict(list)
    if not results_root.exists():
        return portal_hits

    for portal_dir in sorted(results_root.iterdir()):
        if not portal_dir.is_dir():
            continue
        portal_id = portal_dir.name
        tsv_path = portal_dir / f"{portal_id}.tsv"
        if not tsv_path.exists():
            continue
        for hit in filter_by_accessions(parse_ipr_tsv(tsv_path), target_pfams):
            portal_hits[portal_id].append(hit)
    return portal_hits


def _sanitize_header_part(text: str) -> str:
    import re

    return re.sub(r"\s+", "_", text.strip()).strip("_")


def _check_blast_available(makeblastdb_cmd: str) -> bool:
    """Check if BLAST is available on PATH."""
    try:
        subprocess.run(
            [makeblastdb_cmd, "-version"],
            capture_output=True,
            text=True,
        )
        return True
    except FileNotFoundError:
        return False


def _blast_characterized_against_portal(
    char_rec: FastaRecord,
    portal_fasta: Path,
    makeblastdb_cmd: str,
    blastp_cmd: str,
    evalue: float,
) -> str | None:
    """BLAST a characterized protein against a portal FASTA.

    Returns the best-hit header from the portal FASTA, or None if no hit.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        query_path = tmp_path / "query.faa"
        db_prefix = tmp_path / "db"

        # Write query FASTA
        query_path.write_text(f">{char_rec.header}\n{char_rec.sequence}\n", encoding="utf-8")

        # Build BLAST database
        subprocess.run(
            [makeblastdb_cmd, "-in", str(portal_fasta), "-dbtype", "prot", "-out", str(db_prefix)],
            check=True,
            capture_output=True,
            text=True,
        )

        # Run blastp
        result = subprocess.run(
            [
                blastp_cmd,
                "-query", str(query_path),
                "-db", str(db_prefix),
                "-outfmt", "6 sseqid evalue bitscore",
                "-evalue", str(evalue),
                "-max_target_seqs", "1",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        # Parse top hit
        for line in result.stdout.strip().splitlines():
            parts = line.split("\t")
            if parts:
                return parts[0]  # best hit subject header

    return None


def select_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Family to select proteins for"),
    staging_id: str | None = typer.Option(
        None, "--staging-id", help="Staging snapshot (default: latest)"
    ),
    project_ipr_run_id: str | None = typer.Option(
        None, "--project-ipr-run-id", help="Project InterProScan run ID (default: latest)"
    ),
    arch_mode: str = typer.Option(
        "flag",
        "--arch-mode",
        help="Domain architecture filtering: strict|flag|off",
    ),
    blast_evalue: float = typer.Option(
        10.0, "--blast-evalue", help="E-value cutoff for BLAST integration of characterized proteins"
    ),
) -> None:
    """Select matching proteins from project proteomes for a gene family."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)
    tools = load_tools(project_dir)

    if arch_mode not in ("strict", "flag", "off"):
        raise typer.BadParameter(f"--arch-mode must be strict, flag, or off. Got: {arch_mode!r}")

    # Resolve BLAST commands (prepend bin_dir if set)
    blast_bin = tools.blast.bin_dir
    makeblastdb_cmd = str(blast_bin / tools.blast.makeblastdb_cmd) if blast_bin else tools.blast.makeblastdb_cmd
    blastp_cmd = str(blast_bin / tools.blast.blastp_cmd) if blast_bin else tools.blast.blastp_cmd
    blast_available = _check_blast_available(makeblastdb_cmd)

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

    target_pfams = set(json.loads(family_row["pfams"]))

    # Load characterized data
    char_dir = paths.family_characterized_dir(family_id)
    char_tsv_path = char_dir / "characterized.tsv"
    char_fasta_path = char_dir / "characterized.faa"

    char_ipr_dir = char_dir / "interproscan"
    char_ipr_tsv = char_ipr_dir / "characterized.tsv"
    if not char_ipr_tsv.exists():
        raise typer.BadParameter(
            f"Characterized InterProScan results not found: {char_ipr_tsv}\n"
            "Run `protsetphylo interproscan` first."
        )

    # Compute thresholds from characterized set
    char_hits = list(filter_by_accessions(parse_ipr_tsv(char_ipr_tsv), target_pfams))
    evalue_thresholds = compute_max_evalues(char_hits, target_pfams)
    char_archs = build_domain_architectures(char_hits, target_pfams)
    allowed_archs = set(char_archs.values()) if arch_mode != "off" else set()

    # Parse characterized TSV to build portal and standalone lookups
    char_rows: list[dict[str, str]] = []
    with char_tsv_path.open("r", encoding="utf-8", newline="") as f:
        char_rows = list(csv.DictReader(f, delimiter="\t"))

    char_records = list(iter_fasta(char_fasta_path))
    char_by_header: dict[str, FastaRecord] = {r.header: r for r in char_records}

    # Group characterized proteins: {portal_id: [(header, record, row)]} and standalone
    portal_char: dict[str, list[tuple[str, FastaRecord, dict[str, str]]]] = defaultdict(list)
    standalone_char: dict[str, list[FastaRecord]] = defaultdict(list)  # keyed by short_name

    for row in char_rows:
        short_name = _sanitize_header_part(row["short_name"])
        protein_name = _sanitize_header_part(row["protein_name"])
        header = f"{short_name}|{protein_name}"
        char_rec = char_by_header.get(header)
        if char_rec is None:
            continue

        portal_id = row.get("portal_id", "").strip()
        if portal_id:
            portal_char[portal_id].append((header, char_rec, row))
        else:
            standalone_char[short_name].append(char_rec)

    # Find project IPR results
    ipr_run_id, results_root = _find_project_ipr_run(paths, project_ipr_run_id)

    # Resolve staging
    selected_staging_id = resolve_staging_id(project_dir, staging_id)
    proteomes_dir = paths.staging_proteomes_dir(selected_staging_id)
    if not proteomes_dir.exists():
        raise typer.BadParameter(f"Missing staged proteomes for {selected_staging_id}")

    # Collect portal hits
    portal_hits = _collect_portal_ipr_hits(results_root, target_pfams)

    # Warn if BLAST not available but characterized proteins need integration
    if not blast_available and portal_char:
        typer.echo(
            "WARNING: BLAST not found on PATH. Characterized proteins with portal_id "
            "will be appended without replacing best hits. Load BLAST for proper integration "
            "(e.g. module load blast)."
        )

    # Select proteins per portal
    selected_dir = paths.family_selected_dir(family_id)
    selected_dir.mkdir(parents=True, exist_ok=True)

    report_rows: list[dict[str, str]] = []
    total_selected = 0
    portals_with_hits = 0
    char_integrated = 0
    char_appended = 0

    for portal_id, hits in sorted(portal_hits.items()):
        # Filter by e-value thresholds
        passing_proteins: dict[str, list[IprHit]] = defaultdict(list)
        for hit in hits:
            threshold = evalue_thresholds.get(hit.accession)
            if threshold is not None and hit.evalue <= threshold:
                passing_proteins[hit.protein_id].append(hit)

        if not passing_proteins:
            continue

        # Build architectures for passing proteins
        protein_archs = build_domain_architectures(
            [h for hs in passing_proteins.values() for h in hs], target_pfams
        )

        # Apply architecture filter
        selected_protein_ids: list[str] = []
        for protein_id in sorted(passing_proteins.keys()):
            arch = protein_archs.get(protein_id, ())
            arch_match = arch in allowed_archs if allowed_archs else True

            if arch_mode == "strict" and not arch_match:
                report_rows.append({
                    "portal_id": portal_id,
                    "protein_id": protein_id,
                    "architecture": "|".join(arch),
                    "arch_match": "no",
                    "selected": "no",
                    "reason": "arch_mismatch",
                })
                continue

            selected_protein_ids.append(protein_id)
            report_rows.append({
                "portal_id": portal_id,
                "protein_id": protein_id,
                "architecture": "|".join(arch),
                "arch_match": "yes" if arch_match else "no",
                "selected": "yes",
                "reason": "pass" if arch_match else "arch_flagged",
            })

        if not selected_protein_ids:
            continue

        # Extract sequences from staged proteome
        proteome_path = proteomes_dir / f"{portal_id}.faa"
        if not proteome_path.exists():
            continue

        protein_id_set = set(selected_protein_ids)
        selected_records: list[FastaRecord] = []
        for rec in iter_fasta(proteome_path):
            # Match against full header (IPR uses full header as protein_id)
            if rec.header in protein_id_set:
                selected_records.append(rec)
                continue
            # Also try matching parsed protein_id (portal_id|protein_id format)
            parts = rec.header.split("|", 1)
            pid = parts[1] if len(parts) == 2 else parts[0]
            if pid in protein_id_set:
                selected_records.append(rec)

        if not selected_records:
            continue

        # Integrate characterized proteins for this portal via BLAST
        if portal_id in portal_char:
            if blast_available:
                out_fasta_tmp = selected_dir / f"{portal_id}.faa"
                write_fasta(selected_records, out_fasta_tmp)

                for _header, char_rec, _row in portal_char[portal_id]:
                    best_hit = _blast_characterized_against_portal(
                        char_rec, out_fasta_tmp, makeblastdb_cmd, blastp_cmd, blast_evalue,
                    )
                    if best_hit:
                        # Replace best hit with characterized protein
                        selected_records = [
                            char_rec if r.header == best_hit else r
                            for r in selected_records
                        ]
                        char_integrated += 1
                        report_rows.append({
                            "portal_id": portal_id,
                            "protein_id": char_rec.header,
                            "architecture": "",
                            "arch_match": "",
                            "selected": "yes",
                            "reason": f"characterized_replaced:{best_hit}",
                        })
                    else:
                        # No hit — append
                        selected_records.append(char_rec)
                        char_appended += 1
                        report_rows.append({
                            "portal_id": portal_id,
                            "protein_id": char_rec.header,
                            "architecture": "",
                            "arch_match": "",
                            "selected": "yes",
                            "reason": "characterized_appended",
                        })
            else:
                # BLAST not available — append characterized proteins directly
                for _header, char_rec, _row in portal_char[portal_id]:
                    selected_records.append(char_rec)
                    char_appended += 1
                    report_rows.append({
                        "portal_id": portal_id,
                        "protein_id": char_rec.header,
                        "architecture": "",
                        "arch_match": "",
                        "selected": "yes",
                        "reason": "characterized_appended_no_blast",
                    })

        out_fasta = selected_dir / f"{portal_id}.faa"
        write_fasta(selected_records, out_fasta)
        total_selected += len(selected_records)
        portals_with_hits += 1

    # Write standalone characterized proteins (no portal_id), grouped by short_name
    for short_name, char_recs in sorted(standalone_char.items()):
        out_fasta = selected_dir / f"{short_name}.faa"
        write_fasta(char_recs, out_fasta)
        total_selected += len(char_recs)
        portals_with_hits += 1
        for rec in char_recs:
            report_rows.append({
                "portal_id": short_name,
                "protein_id": rec.header,
                "architecture": "",
                "arch_match": "",
                "selected": "yes",
                "reason": "characterized_standalone",
            })

    # Write selection report
    report_path = selected_dir / "selection_report.tsv"
    if report_rows:
        with report_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["portal_id", "protein_id", "architecture", "arch_match", "selected", "reason"],
                delimiter="\t",
            )
            writer.writeheader()
            writer.writerows(report_rows)

    # Update manifest
    manifest_path = paths.family_manifest(family_id)
    manifest = read_manifest(manifest_path)
    manifest["selection"] = {
        "project_ipr_run_id": ipr_run_id,
        "staging_id": selected_staging_id,
        "arch_mode": arch_mode,
        "blast_evalue": blast_evalue,
        "evalue_thresholds": {k: v for k, v in evalue_thresholds.items()},
        "n_portals_with_hits": portals_with_hits,
        "n_selected_proteins": total_selected,
        "n_characterized_replaced": char_integrated,
        "n_characterized_appended": char_appended,
        "n_characterized_standalone": sum(len(v) for v in standalone_char.values()),
    }
    write_manifest(manifest_path, manifest)

    # Update DB
    conn = connect(paths.db_path)
    try:
        conn.execute(
            "UPDATE families SET project_ipr_run_id = ? WHERE family_id = ?",
            (ipr_run_id, family_id),
        )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_select",
            "family_id": family_id,
            "project_ipr_run_id": ipr_run_id,
            "staging_id": selected_staging_id,
            "arch_mode": arch_mode,
            "blast_evalue": blast_evalue,
            "n_portals_with_hits": portals_with_hits,
            "n_selected_proteins": total_selected,
            "n_characterized_replaced": char_integrated,
            "n_characterized_appended": char_appended,
        },
    )

    typer.echo(f"Selection complete for family: {family_id}")
    typer.echo(f"  Portals with hits:          {portals_with_hits}")
    typer.echo(f"  Proteins selected:          {total_selected}")
    typer.echo(f"  Characterized replaced:     {char_integrated}")
    typer.echo(f"  Characterized appended:     {char_appended}")
    typer.echo(f"  Characterized standalone:   {sum(len(v) for v in standalone_char.values())}")
    typer.echo(f"  Arch mode:                  {arch_mode}")
    typer.echo(f"  Output:                     {selected_dir}")
