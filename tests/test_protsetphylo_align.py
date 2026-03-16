from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _setup_family_with_fasta(tmp_path: Path, project_dir: Path, paths: ProjectPaths) -> None:
    tsv = tmp_path / "char.tsv"
    tsv.write_text(
        "portal_id\tspecies\tshort_name\tprotein_name\tsequence\n"
        "P1\tSp1\tSp1\tLAT1\tMPEPTIDE\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        app,
        [
            "protsetphylo", "init", str(project_dir),
            "--family-id", "align_test",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output

    # Write combined FASTA
    fasta_dir = paths.family_fasta_dir("align_test")
    fasta_dir.mkdir(parents=True, exist_ok=True)
    (fasta_dir / "combined.faa").write_text(
        ">Sp1|LAT1\nMPEPTIDE\n>P1|prot_A\nMAAAAAAAA\n",
        encoding="utf-8",
    )


def test_align_writes_slurm_script(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family_with_fasta(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "align", str(project_dir),
            "--family-id", "align_test",
            "--account", "project_123",
            "--no-confirm",
            "--run-id", "align_run1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Wrote alignment script" in result.output

    # Verify script exists
    script = paths.run_dir("align_run1") / "slurm" / "align.sbatch"
    assert script.exists()
    content = script.read_text(encoding="utf-8")
    assert "mafft" in content
    assert "trimal" in content
    assert "project_123" in content

    # Verify manifest
    manifest_path = paths.run_manifest("align_run1")
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["kind"] == "family_align"
    assert manifest["family_id"] == "align_test"


def test_align_requires_combined_fasta(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

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
            "--family-id", "no_fasta",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "align", str(project_dir),
            "--family-id", "no_fasta",
            "--account", "proj",
            "--no-confirm",
        ],
    )
    assert result.exit_code != 0
    assert "Combined FASTA not found" in result.output
