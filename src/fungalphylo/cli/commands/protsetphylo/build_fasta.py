from __future__ import annotations

import csv
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.ids import now_iso
from fungalphylo.core.manifest import read_manifest, write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect, init_db


def _cdhit_clstr_to_tsv(clstr_path: Path, out_path: Path) -> None:
    """Convert a CD-HIT .clstr file to a two-column TSV (representative, member)."""
    import re

    clusters: list[tuple[str, list[str]]] = []
    current_members: list[str] = []
    representative: str | None = None

    with clstr_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith(">Cluster"):
                if representative and current_members:
                    clusters.append((representative, current_members))
                current_members = []
                representative = None
                continue
            m = re.search(r">(.+?)\.\.\.", line)
            if not m:
                continue
            name = m.group(1)
            current_members.append(name)
            if line.rstrip().endswith("*"):
                representative = name
    if representative and current_members:
        clusters.append((representative, current_members))

    with out_path.open("w", encoding="utf-8") as f:
        f.write("representative\tmember\n")
        for rep, members in clusters:
            for member in members:
                f.write(f"{rep}\t{member}\n")


def _split_clusters(
    cluster_members_path: Path,
    all_records_by_header: dict[str, FastaRecord],
    characterized_headers: set[str],
    clusters_dir: Path,
) -> Path:
    """Split sequences into per-cluster FASTAs and write a cluster summary.

    Returns the path to the cluster_summary.tsv file.
    """
    clusters_dir.mkdir(parents=True, exist_ok=True)

    # Parse cluster_members.tsv into {representative: [members]}
    rep_to_members: dict[str, list[str]] = defaultdict(list)
    with cluster_members_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rep_to_members[row["representative"]].append(row["member"])

    summary_rows: list[dict[str, str]] = []

    for idx, (rep, members) in enumerate(
        sorted(rep_to_members.items(), key=lambda kv: len(kv[1]), reverse=True), start=1
    ):
        cluster_id = f"cluster_{idx:04d}"
        records: list[FastaRecord] = []
        char_in_cluster: list[str] = []

        for member in members:
            rec = all_records_by_header.get(member)
            if rec is not None:
                records.append(rec)
                if member in characterized_headers:
                    char_in_cluster.append(member)

        if records:
            write_fasta(records, clusters_dir / f"{cluster_id}.faa")

        summary_rows.append({
            "cluster_id": cluster_id,
            "representative": rep,
            "n_members": str(len(members)),
            "n_sequences_found": str(len(records)),
            "has_characterized": "yes" if char_in_cluster else "no",
            "characterized_proteins": ";".join(char_in_cluster),
        })

    summary_path = clusters_dir / "cluster_summary.tsv"
    with summary_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "cluster_id", "representative", "n_members",
                "n_sequences_found", "has_characterized", "characterized_proteins",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    return summary_path


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
    n_clusters = 0
    clusters_dir = fasta_dir / "clusters"

    if redundancy_tool:
        dedup_path = fasta_dir / "combined.dedup.faa"
        # Preserve the pre-dedup FASTA for reference
        pre_dedup_path = fasta_dir / "combined.pre_dedup.faa"
        shutil.copy2(combined_path, pre_dedup_path)

        cluster_members_path = fasta_dir / "cluster_members.tsv"

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
                # Convert cd-hit .clstr to cluster_members.tsv
                clstr = Path(str(dedup_path) + ".clstr")
                if clstr.exists():
                    _cdhit_clstr_to_tsv(clstr, cluster_members_path)
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
                # Preserve cluster membership TSV
                mmseqs_cluster_tsv = Path(str(dedup_path) + "_cluster.tsv")
                if mmseqs_cluster_tsv.exists():
                    shutil.copy2(mmseqs_cluster_tsv, cluster_members_path)
                rep_seqs = Path(str(dedup_path) + "_rep_seq.fasta")
                if rep_seqs.exists():
                    rep_seqs.replace(combined_path)
                # Clean up mmseqs intermediates (but cluster_members.tsv is preserved)
                for f in fasta_dir.glob("combined.dedup*"):
                    f.unlink(missing_ok=True)
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except FileNotFoundError:
                raise typer.BadParameter("mmseqs not found on PATH") from None

        # Split clusters into per-cluster FASTAs
        if cluster_members_path.exists():
            # Build header lookup from the pre-dedup FASTA (all sequences)
            all_records_by_header = {r.header: r for r in iter_fasta(pre_dedup_path)}
            # Collect characterized protein headers
            char_headers: set[str] = set()
            for rec in iter_fasta(char_fasta_path):
                char_headers.add(rec.header)

            if clusters_dir.exists():
                shutil.rmtree(clusters_dir)
            summary_path = _split_clusters(
                cluster_members_path, all_records_by_header, char_headers, clusters_dir,
            )
            n_clusters = sum(1 for f in clusters_dir.glob("*.faa"))
            typer.echo(f"  Cluster FASTAs:     {clusters_dir}")
            typer.echo(f"  Cluster summary:    {summary_path}")

    n_combined = sum(1 for _ in iter_fasta(combined_path))

    # Update manifest
    manifest_path = paths.family_manifest(family_id)
    manifest = read_manifest(manifest_path)
    manifest["build_fasta"] = {
        "n_combined": n_combined,
        "redundancy_tool": redundancy_tool,
        "identity_threshold": identity_threshold if redundancy_tool else None,
        "combined_fasta": str(combined_path.relative_to(project_dir)),
        "n_clusters": n_clusters if redundancy_tool else None,
        "clusters_dir": str(clusters_dir.relative_to(project_dir)) if n_clusters else None,
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
            "n_clusters": n_clusters,
        },
    )

    typer.echo(f"Built FASTA for family: {family_id}")
    typer.echo(f"  Combined sequences: {n_combined}")
    if n_clusters:
        typer.echo(f"  Total clusters:     {n_clusters}")
    typer.echo(f"  Output:             {combined_path}")
