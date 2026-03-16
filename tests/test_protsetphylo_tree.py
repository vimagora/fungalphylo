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


def _setup_family_with_alignment(
    tmp_path: Path, project_dir: Path, paths: ProjectPaths
) -> None:
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
            "--family-id", "tree_test",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )
    assert result.exit_code == 0, result.output

    alignment_dir = paths.family_alignment_dir("tree_test")
    alignment_dir.mkdir(parents=True, exist_ok=True)
    (alignment_dir / "combined.trimmed.aln").write_text(
        ">Sp1|LAT1\nMPEPTIDE\n>P1|prot_A\nMAAAAAAAA\n",
        encoding="utf-8",
    )


def test_tree_writes_iqtree_script(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family_with_alignment(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree", str(project_dir),
            "--family-id", "tree_test",
            "--account", "project_123",
            "--no-confirm",
            "--run-id", "tree_run1",
            "--tree-method", "iqtree",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Wrote tree script" in result.output

    script = paths.run_dir("tree_run1") / "slurm" / "tree.sbatch"
    assert script.exists()
    content = script.read_text(encoding="utf-8")
    assert "iqtree2" in content
    assert "-bb 1000" in content
    assert "project_123" in content

    manifest = json.loads(paths.run_manifest("tree_run1").read_text(encoding="utf-8"))
    assert manifest["kind"] == "family_tree"
    assert manifest["tree"]["method"] == "iqtree"


def test_tree_writes_fasttree_script(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family_with_alignment(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree", str(project_dir),
            "--family-id", "tree_test",
            "--account", "proj",
            "--no-confirm",
            "--run-id", "tree_ft",
            "--tree-method", "fasttree",
        ],
    )
    assert result.exit_code == 0, result.output

    script = paths.run_dir("tree_ft") / "slurm" / "tree.sbatch"
    content = script.read_text(encoding="utf-8")
    assert "FastTree" in content
    assert "-gamma" in content


def test_tree_requires_trimmed_alignment(tmp_path: Path) -> None:
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
            "--family-id", "no_aln",
            "--characterized", str(tsv),
            "--pfam", "PF00083",
        ],
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree", str(project_dir),
            "--family-id", "no_aln",
            "--account", "proj",
            "--no-confirm",
        ],
    )
    assert result.exit_code != 0
    assert "Trimmed alignment not found" in result.output


def test_tree_rejects_invalid_method(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _setup_family_with_alignment(tmp_path, project_dir, paths)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree", str(project_dir),
            "--family-id", "tree_test",
            "--account", "proj",
            "--no-confirm",
            "--tree-method", "invalid",
        ],
    )
    assert result.exit_code != 0
    assert "must be iqtree or fasttree" in result.output
