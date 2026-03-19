from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.fasta import iter_fasta
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_staging(paths: ProjectPaths, staging_id: str, proteomes: dict[str, str]) -> None:
    """Seed a staging snapshot with proteome FASTAs."""
    proteomes_dir = paths.staging_proteomes_dir(staging_id)
    proteomes_dir.mkdir(parents=True, exist_ok=True)

    for portal_id, fasta_content in proteomes.items():
        (proteomes_dir / f"{portal_id}.faa").write_text(fasta_content, encoding="utf-8")

    manifest = {
        "staging_id": staging_id,
        "created_at": _now(),
        "outputs": {"proteomes_dir": f"staging/{staging_id}/proteomes"},
        "actions": [],
    }
    manifest_path = paths.staging_manifest(staging_id)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) VALUES(?,?,?,?)",
            (staging_id, _now(), str(manifest_path.relative_to(paths.root)), "dummy"),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_ipr_run(
    paths: ProjectPaths,
    staging_id: str,
    run_id: str,
    portal_tsvs: dict[str, str],
) -> None:
    """Seed a project-wide InterProScan run with result TSVs."""
    run_root = paths.run_dir(run_id)
    results_root = run_root / "interproscan_results"

    for portal_id, tsv_content in portal_tsvs.items():
        portal_dir = results_root / portal_id
        portal_dir.mkdir(parents=True, exist_ok=True)
        (portal_dir / f"{portal_id}.tsv").write_text(tsv_content, encoding="utf-8")

    manifest = {
        "run_id": run_id,
        "kind": "interproscan",
        "created_at": _now(),
        "staging_id": staging_id,
        "paths": {
            "results_root": str(results_root.relative_to(paths.root)),
        },
    }
    manifest_path = paths.run_manifest(run_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256) VALUES(?,?,?,?,?,?)",
            (run_id, staging_id, "interproscan", _now(), str(manifest_path.relative_to(paths.root)), "dummy"),
        )
        conn.commit()
    finally:
        conn.close()


def _init_family_with_ipr(
    tmp_path: Path,
    project_dir: Path,
    paths: ProjectPaths,
    family_id: str = "mfs_sugar",
) -> None:
    """Initialize a family and write fake characterized IPR results."""
    tsv = tmp_path / "char.tsv"
    tsv.write_text(
        "portal_id\tspecies\tshort_name\tprotein_name\tsequence\n"
        "Portal1\tSp1\tSp1\tLAT1\tMKFLTVAAAA\n"
        "\tSp2\tAmbmo\tSTP1\tMDEFGHIJKL\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", family_id,
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output

    # Write fake characterized IPR TSV
    char_ipr_dir = paths.family_characterized_dir(family_id) / "interproscan"
    char_ipr_dir.mkdir(parents=True, exist_ok=True)
    (char_ipr_dir / "characterized.tsv").write_text(
        "Sp1|LAT1\t100\t500\tPfam\tPF00083\tSugar_tr\t10\t250\t1e-30\tT\t2024-01-01\n"
        "Ambmo|STP1\t200\t400\tPfam\tPF00083\tSugar_tr\t5\t200\t1e-20\tT\t2024-01-01\n",
        encoding="utf-8",
    )


def test_select_finds_matching_proteins(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _init_family_with_ipr(tmp_path, project_dir, paths)

    # Seed staging with proteomes (headers: portal_id|protein_id)
    _seed_staging(paths, "stg1", {
        "Portal1": ">Portal1|prot_A\nMAAAAAAAA\n>Portal1|prot_B\nMBBBBBBBB\n",
        "Portal2": ">Portal2|prot_C\nMCCCCCCCC\n",
    })

    # Seed IPR results: prot_A and prot_C match PF00083
    _seed_ipr_run(paths, "stg1", "ipr_run1", {
        "Portal1": (
            "Portal1|prot_A\t100\t500\tPfam\tPF00083\tSugar_tr\t10\t250\t1e-40\tT\t2024-01-01\n"
            "Portal1|prot_B\t100\t300\tPfam\tPF99999\tOther\t10\t100\t1e-10\tT\t2024-01-01\n"
        ),
        "Portal2": (
            "Portal2|prot_C\t100\t500\tPfam\tPF00083\tSugar_tr\t5\t200\t1e-25\tT\t2024-01-01\n"
        ),
    })

    result = runner.invoke(
        app,
        [
            "protsetphylo", "select", str(project_dir),
            "--family-id", "mfs_sugar",
            "--staging-id", "stg1",
            "--project-ipr-run-id", "ipr_run1",
            "--arch-mode", "off",
        ],
    )
    assert result.exit_code == 0, result.output
    # 2 from IPR selection + 1 characterized appended (Portal1) + 1 standalone (Ambmo)
    assert "Proteins selected:          4" in result.output

    # Verify selected FASTAs exist
    selected_dir = paths.family_selected_dir("mfs_sugar")
    assert (selected_dir / "Portal1.faa").exists()
    assert (selected_dir / "Portal2.faa").exists()
    assert (selected_dir / "Ambmo.faa").exists()  # standalone characterized

    # Check Portal1 content: selected prot_A + appended characterized Sp1|LAT1
    portal1_records = list(iter_fasta(selected_dir / "Portal1.faa"))
    assert len(portal1_records) == 2
    portal1_headers = {r.header for r in portal1_records}
    assert "Portal1|prot_A" in portal1_headers
    assert "Sp1|LAT1" in portal1_headers

    portal2_records = list(iter_fasta(selected_dir / "Portal2.faa"))
    assert len(portal2_records) == 1

    # Check standalone
    ambmo_records = list(iter_fasta(selected_dir / "Ambmo.faa"))
    assert len(ambmo_records) == 1
    assert ambmo_records[0].header == "Ambmo|STP1"

    # Verify report
    report_path = selected_dir / "selection_report.tsv"
    assert report_path.exists()
    with report_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert all(r["selected"] == "yes" for r in rows)


def test_select_strict_arch_mode_excludes_mismatches(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _init_family_with_ipr(tmp_path, project_dir, paths)

    _seed_staging(paths, "stg1", {
        "Portal1": ">Portal1|prot_A\nMAAAAAAAA\n>Portal1|prot_B\nMBBBBBBBB\n",
    })

    # prot_A has matching arch (PF00083 only), prot_B has PF00083 + extra PF99999
    _seed_ipr_run(paths, "stg1", "ipr_run1", {
        "Portal1": (
            "Portal1|prot_A\t100\t500\tPfam\tPF00083\tSugar_tr\t10\t250\t1e-40\tT\t2024-01-01\n"
            "Portal1|prot_B\t100\t300\tPfam\tPF00083\tSugar_tr\t10\t250\t1e-25\tT\t2024-01-01\n"
        ),
    })

    result = runner.invoke(
        app,
        [
            "protsetphylo", "select", str(project_dir),
            "--family-id", "mfs_sugar",
            "--staging-id", "stg1",
            "--project-ipr-run-id", "ipr_run1",
            "--arch-mode", "strict",
        ],
    )
    assert result.exit_code == 0, result.output
    # 2 from IPR + 1 characterized appended (Portal1) + 1 standalone (Ambmo)
    assert "Proteins selected:          4" in result.output


def test_select_evalue_threshold_filters(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _init_family_with_ipr(tmp_path, project_dir, paths)

    _seed_staging(paths, "stg1", {
        "Portal1": ">Portal1|prot_A\nMAAAAAAAA\n>Portal1|prot_B\nMBBBBBBBB\n",
    })

    # prot_A passes threshold (1e-40 <= 1e-20), prot_B fails (1e-5 > 1e-20)
    _seed_ipr_run(paths, "stg1", "ipr_run1", {
        "Portal1": (
            "Portal1|prot_A\t100\t500\tPfam\tPF00083\tSugar_tr\t10\t250\t1e-40\tT\t2024-01-01\n"
            "Portal1|prot_B\t100\t300\tPfam\tPF00083\tSugar_tr\t10\t250\t1e-5\tT\t2024-01-01\n"
        ),
    })

    result = runner.invoke(
        app,
        [
            "protsetphylo", "select", str(project_dir),
            "--family-id", "mfs_sugar",
            "--staging-id", "stg1",
            "--project-ipr-run-id", "ipr_run1",
            "--arch-mode", "off",
        ],
    )
    assert result.exit_code == 0, result.output
    # 1 from IPR (prot_A passes, prot_B fails) + 1 characterized appended (Portal1) + 1 standalone (Ambmo)
    assert "Proteins selected:          3" in result.output


def test_select_requires_characterized_ipr(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    tsv = tmp_path / "char.tsv"
    tsv.write_text(
        "portal_id\tspecies\tshort_name\tprotein_name\tsequence\n"
        "P1\tSp1\tSp1\tLAT1\tMPEPTIDE\n",
        encoding="utf-8",
    )
    runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "no_ipr",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "select", str(project_dir),
            "--family-id", "no_ipr",
            "--arch-mode", "off",
        ],
    )
    assert result.exit_code != 0
    assert "InterProScan results not found" in result.output
