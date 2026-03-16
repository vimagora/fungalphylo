from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.fasta import iter_fasta
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


CHARACTERIZED_TSV = (
    "portal_id\tspecies\tshort_name\tprotein_name\tsequence\n"
    "Portal1\tSpecies alpha\tSpalpha\tLAT1\tMKFLTVAAAA\n"
    "Portal2\tSpecies beta\tSpbeta\tHXT5\tMTPKLIVGGG\n"
    "\tCharacterized only\tAmbmo\tSTP1\tMDEFGHIJKL\n"
)


def _write_characterized(tmp_path: Path) -> Path:
    tsv = tmp_path / "char.tsv"
    tsv.write_text(CHARACTERIZED_TSV, encoding="utf-8")
    return tsv


def test_init_creates_family(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    tsv = _write_characterized(tmp_path)

    result = runner.invoke(
        app,
        [
            "protsetphylo",
            "init",
            str(project_dir),
            "--family-id",
            "mfs_sugar",
            "--characterized",
            str(tsv),
            "--pfam",
            "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Initialized family: mfs_sugar" in result.output

    # Verify directory structure
    family_dir = paths.family_dir("mfs_sugar")
    assert family_dir.exists()
    assert (paths.family_characterized_dir("mfs_sugar") / "characterized.tsv").exists()
    assert (paths.family_characterized_dir("mfs_sugar") / "characterized.faa").exists()
    assert (paths.family_config_dir("mfs_sugar") / "pfams.txt").exists()
    assert paths.family_manifest("mfs_sugar").exists()

    # Verify FASTA content
    records = list(iter_fasta(paths.family_characterized_dir("mfs_sugar") / "characterized.faa"))
    assert len(records) == 3
    assert records[0].header == "Spalpha|LAT1"
    assert records[0].sequence == "MKFLTVAAAA"
    assert records[1].header == "Spbeta|HXT5"
    assert records[2].header == "Ambmo|STP1"

    # Verify pfams.txt
    pfams = (paths.family_config_dir("mfs_sugar") / "pfams.txt").read_text(encoding="utf-8")
    assert "PF00083" in pfams

    # Verify manifest
    manifest = json.loads(paths.family_manifest("mfs_sugar").read_text(encoding="utf-8"))
    assert manifest["family_id"] == "mfs_sugar"
    assert manifest["pfams"] == ["PF00083"]
    assert manifest["n_characterized"] == 3

    # Verify DB row
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT * FROM families WHERE family_id = ?", ("mfs_sugar",)
        ).fetchone()
        assert row is not None
        assert json.loads(row["pfams"]) == ["PF00083"]
        assert row["characterized_tsv"] is not None
        assert row["characterized_fasta"] is not None
    finally:
        conn.close()


def test_init_rejects_duplicate_family(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)
    tsv = _write_characterized(tmp_path)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "dup",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "dup",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_init_rejects_invalid_pfam(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)
    tsv = _write_characterized(tmp_path)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "bad_pfam",
            "--characterized", str(tsv),
            "--pfam", "NOTPFAM",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid Pfam" in result.output


def test_init_rejects_missing_columns(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    bad_tsv = tmp_path / "bad.tsv"
    bad_tsv.write_text("portal_id\tspecies\n1\tFoo\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "missing_cols",
            "--characterized", str(bad_tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code != 0
    assert "missing required columns" in result.output


def test_init_with_pfam_list_file(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    tsv = _write_characterized(tmp_path)

    pfam_file = tmp_path / "pfams.txt"
    pfam_file.write_text("PF00083\n# comment\nPF00324\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "multi_pfam",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
            "--pfam-list", str(pfam_file),
        ],
    )
    assert result.exit_code == 0, result.output

    pfams = (
        paths.family_config_dir("multi_pfam") / "pfams.txt"
    ).read_text(encoding="utf-8").strip().split("\n")
    # PF00083 appears in both --pfam and file, should be deduplicated
    assert pfams == ["PF00083", "PF00324"]


def test_init_rejects_no_pfam(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)
    tsv = _write_characterized(tmp_path)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "no_pfam",
            "--characterized", str(tsv),
        ],
    )
    assert result.exit_code != 0
    assert "At least one Pfam" in result.output


def test_init_rejects_invalid_family_id(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)
    tsv = _write_characterized(tmp_path)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "bad id!",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid family_id" in result.output


def test_init_sanitizes_protein_names_with_spaces(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    tsv = tmp_path / "spaces.tsv"
    tsv.write_text(
        "portal_id\tspecies\tshort_name\tprotein_name\tsequence\n"
        "P1\tSp1\tMyOrg\tMy Protein Name\tMPEPTIDE\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "space_test",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output

    records = list(
        iter_fasta(paths.family_characterized_dir("space_test") / "characterized.faa")
    )
    assert records[0].header == "MyOrg|My_Protein_Name"
