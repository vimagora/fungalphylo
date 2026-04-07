from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fungalphylo.cli.commands.astral_slurm import _extract_tips, _rename_tips_to_species
from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_phylo_run(
    paths: ProjectPaths, run_id: str, og_trees: dict[str, str]
) -> Path:
    """Create a fake phylo run with gene tree files.

    og_trees: {og_name: newick_string}
    """
    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "kind": "phylo",
        "created_at": "2026-04-02T00:00:00+00:00",
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    gene_trees_dir = run_dir / "gene_trees"
    for og_name, newick in og_trees.items():
        og_dir = gene_trees_dir / og_name
        og_dir.mkdir(parents=True, exist_ok=True)
        (og_dir / f"{og_name}.treefile").write_text(newick + "\n", encoding="utf-8")

    return gene_trees_dir


# --- Unit tests for helpers ---


def test_extract_tips_simple() -> None:
    newick = "((A|p1,B|p2),(C|p3,D|p4));"
    tips = _extract_tips(newick)
    assert sorted(tips) == ["A|p1", "B|p2", "C|p3", "D|p4"]


def test_extract_tips_with_branch_lengths() -> None:
    newick = "((A|p1:0.1,B|p2:0.2):0.5,(C|p3:0.3,D|p4:0.4):0.6);"
    tips = _extract_tips(newick)
    assert sorted(tips) == ["A|p1", "B|p2", "C|p3", "D|p4"]


def test_rename_tips_to_species() -> None:
    newick = "((SpA|p1:0.1,SpB|p2:0.2):0.5,(SpC|p3:0.3,SpA|p4:0.4):0.6);"
    renamed, species = _rename_tips_to_species(newick, "|")
    assert sorted(species) == ["SpA", "SpB", "SpC"]
    # Tips should be species names, not full labels
    assert "SpA|p1" not in renamed
    assert "SpA" in renamed
    assert "SpB" in renamed
    assert "SpC" in renamed


# --- Integration tests ---


def test_astral_prep_collects_trees_and_writes_mapping(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_test", {
        "OG0000001": "((SpA|p1,SpB|p2),(SpC|p3,SpD|p4));",
        "OG0000002": "((SpA|p5,SpB|p6),(SpC|p7,SpD|p8));",
        "OG0000003": "((SpA|p9,SpB|p10),(SpC|p11,SpD|p12));",
    })

    monkeypatch.setattr(
        "fungalphylo.cli.commands.astral_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "astral-slurm",
            "--account", "project_123",
            "--run-id", "phylo_test",
            "--output-run-id", "astral_test",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "3 kept" in result.output
    assert "Species:      4" in result.output

    # Gene trees file — tips should be species names only
    gt_file = project_dir / "runs/astral_test/slurm/gene_trees.nwk"
    assert gt_file.exists()
    gt_text = gt_file.read_text(encoding="utf-8")
    lines = [l for l in gt_text.strip().split("\n") if l]
    assert len(lines) == 3
    # Verify tips were renamed: species names present, full labels gone
    assert "SpA" in gt_text
    assert "SpB" in gt_text
    assert "|p1" not in gt_text
    assert "|p2" not in gt_text

    # No mapping file (tips renamed instead)
    mapping = project_dir / "runs/astral_test/slurm/species_map.txt"
    assert not mapping.exists()

    # SLURM script
    script = (project_dir / "runs/astral_test/slurm/astral.sbatch").read_text(encoding="utf-8")
    assert "module load aster/1.23" in script
    assert "astral-pro3" in script
    assert "gene_trees.nwk" in script
    assert "species_map.txt" not in script
    assert "species_tree.nwk" in script

    # Manifest
    manifest = json.loads(
        (project_dir / "runs/astral_test/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["kind"] == "astral"
    assert manifest["stats"]["kept_trees"] == 3
    assert manifest["stats"]["total_species"] == 4

    # DB row
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT run_id, kind FROM runs WHERE run_id = ?", ("astral_test",)
        ).fetchone()
    finally:
        conn.close()
    assert row["kind"] == "astral"


def test_astral_prep_min_taxa_filters(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_filter", {
        # 4 species — kept with default min_taxa=4
        "OG0000001": "((SpA|p1,SpB|p2),(SpC|p3,SpD|p4));",
        # Only 3 species — skipped with min_taxa=4
        "OG0000002": "((SpA|p5,SpB|p6),SpC|p7);",
        # Only 2 species — skipped
        "OG0000003": "(SpA|p9,SpB|p10);",
    })

    monkeypatch.setattr(
        "fungalphylo.cli.commands.astral_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "astral-slurm",
            "--account", "project_123",
            "--run-id", "phylo_filter",
            "--output-run-id", "astral_filter",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "1 kept" in result.output
    assert "2 skipped" in result.output


def test_astral_prep_auto_detects_latest_phylo_run(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_latest", {
        "OG0000001": "((SpA|p1,SpB|p2),(SpC|p3,SpD|p4));",
    })

    monkeypatch.setattr(
        "fungalphylo.cli.commands.astral_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "astral-slurm",
            "--account", "project_123",
            "--output-run-id", "astral_auto",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Using gene trees from run: phylo_latest" in result.output


def test_astral_prep_submit_mocked(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_sub", {
        "OG0000001": "((SpA|p1,SpB|p2),(SpC|p3,SpD|p4));",
    })

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 12345\n")

    monkeypatch.setattr("fungalphylo.cli.commands.astral_slurm.subprocess.run", _fake_run)

    result = runner.invoke(
        app,
        [
            "astral-slurm",
            "--account", "project_123",
            "--run-id", "phylo_sub",
            "--output-run-id", "astral_sub",
            "--submit",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Submitted batch job 12345" in result.output
    assert len(calls) == 1
    assert "sbatch" in calls[0][0]


def test_astral_prep_no_trees_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(
        app,
        [
            "astral-slurm",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No phylo runs with gene trees found" in result.output
