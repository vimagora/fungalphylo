from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.fasta import iter_fasta
from fungalphylo.core.paths import ProjectPaths

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _setup_family(tmp_path: Path, project_dir: Path, paths: ProjectPaths) -> None:
    """Set up a family with characterized and selected proteins."""
    # Create characterized TSV and init
    tsv = tmp_path / "char.tsv"
    tsv.write_text(
        "portal_id\tspecies\tshort_name\tprotein_name\tsequence\tprotein_id\n"
        "Portal1\tSp1\tSp1\tLAT1\tMKFLTVAAAA\tprot_A\n"
        "\tSp2\tAmbmo\tSTP1\tMDEFGHIJKL\t\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "test_fam",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output

    # Write selected FASTAs
    selected_dir = paths.family_selected_dir("test_fam")
    selected_dir.mkdir(parents=True, exist_ok=True)
    (selected_dir / "Portal1.faa").write_text(
        ">Portal1|prot_A\nMAAAAAAAA\n>Portal1|prot_B\nMBBBBBBBB\n",
        encoding="utf-8",
    )
    (selected_dir / "Portal2.faa").write_text(
        ">Portal2|prot_C\nMCCCCCCCC\n",
        encoding="utf-8",
    )


def test_build_fasta_merges_characterized_and_selected(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "build-fasta", str(project_dir),
            "--family-id", "test_fam",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Built FASTA for family: test_fam" in result.output

    fasta_dir = paths.family_fasta_dir("test_fam")
    combined = fasta_dir / "combined.faa"
    assert combined.exists()

    records = list(iter_fasta(combined))
    headers = {r.header for r in records}

    # prot_A should be replaced by characterized Sp1|LAT1
    assert "Sp1|LAT1" in headers
    assert "Portal1|prot_A" not in headers  # replaced
    assert "Portal1|prot_B" in headers  # kept
    assert "Portal2|prot_C" in headers  # kept
    assert "Ambmo|STP1" in headers  # standalone characterized


def test_build_fasta_per_portal_files(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "build-fasta", str(project_dir),
            "--family-id", "test_fam",
        ],
    )
    assert result.exit_code == 0, result.output

    fasta_dir = paths.family_fasta_dir("test_fam")
    assert (fasta_dir / "Portal1.faa").exists()
    assert (fasta_dir / "Portal2.faa").exists()
    assert (fasta_dir / "Ambmo.faa").exists()  # standalone

    portal1_records = list(iter_fasta(fasta_dir / "Portal1.faa"))
    portal1_headers = {r.header for r in portal1_records}
    assert "Sp1|LAT1" in portal1_headers  # characterized added
    assert "Portal1|prot_B" in portal1_headers  # selected kept
    assert "Portal1|prot_A" not in portal1_headers  # replaced


def test_build_fasta_no_duplicate_headers(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "build-fasta", str(project_dir),
            "--family-id", "test_fam",
        ],
    )
    assert result.exit_code == 0, result.output

    combined = paths.family_fasta_dir("test_fam") / "combined.faa"
    records = list(iter_fasta(combined))
    headers = [r.header for r in records]
    assert len(headers) == len(set(headers)), f"Duplicate headers found: {headers}"


def test_build_fasta_requires_family(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "build-fasta", str(project_dir),
            "--family-id", "nonexistent",
        ],
    )
    assert result.exit_code != 0
    assert "Family not found" in result.output
