from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _create_family(paths: ProjectPaths, family_id: str, og_names: list[str]) -> Path:
    """Create a family with OG FASTAs in og_placed/."""
    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO families(family_id, created_at, pfams) "
            "VALUES(?, datetime('now'), 'PF00001')",
            (family_id,),
        )
        conn.commit()
    finally:
        conn.close()

    og_placed = paths.family_og_placed_dir(family_id)
    og_placed.mkdir(parents=True, exist_ok=True)
    for name in og_names:
        (og_placed / f"{name}.fa").write_text(
            f">{name}_Sp1|p1\nMPEPTIDE\n>{name}_Sp2|p2\nMPEPTIDE\n",
            encoding="utf-8",
        )
    return og_placed


def _write_tools_yaml(paths: ProjectPaths) -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        '  command: "busco"\n'
        "mafft:\n"
        '  command: "mafft"\n'
        "trimal:\n"
        '  command: "trimal"\n'
        "iqtree:\n"
        '  command: "iqtree2"\n',
        encoding="utf-8",
    )


def test_protsetphylo_phylo_slurm_writes_scripts(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _create_family(paths, "mfs_sugar", ["OG0001000", "OG0001001", "OG0001002"])

    monkeypatch.setattr(
        "fungalphylo.core.slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "phylo-slurm",
            "--family-id", "mfs_sugar",
            "--account", "project_123",
            "--output-run-id", "phylo_mfs_test",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "mfs_sugar" in result.output
    assert "Orthogroups:  3" in result.output

    orchestrator = project_dir / "runs/phylo_mfs_test/slurm/phylo_orchestrate.sh"
    worker = project_dir / "runs/phylo_mfs_test/slurm/phylo_worker.sbatch"
    assert orchestrator.exists()
    assert worker.exists()

    # Orchestrator has chained submissions
    orch_text = orchestrator.read_text(encoding="utf-8")
    assert "--step align" in orch_text
    assert "--step trim" in orch_text
    assert "--step tree" in orch_text
    assert "--dependency=afterok:" in orch_text
    # Per-step resource overrides
    assert "--time=04:00:00" in orch_text  # align default
    assert "--time=00:15:00" in orch_text  # trim default
    assert "--time=08:00:00" in orch_text  # tree default

    # Worker has --step parsing and mafft --auto
    worker_text = worker.read_text(encoding="utf-8")
    assert '--step) STEP="$2"' in worker_text
    assert "case \"$STEP\" in" in worker_text
    assert "--auto" in worker_text
    assert "-m TEST" in worker_text
    assert "-B 1000" in worker_text
    # No --retree/--maxiterate (uses --auto for families)
    assert "--retree" not in worker_text
    assert "--maxiterate" not in worker_text

    # File list
    filelist = (project_dir / "runs/phylo_mfs_test/slurm/og_filelist.txt").read_text(
        encoding="utf-8"
    )
    lines = [line for line in filelist.strip().split("\n") if line]
    assert len(lines) == 3

    # Manifest
    manifest = json.loads(
        (project_dir / "runs/phylo_mfs_test/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["kind"] == "family_phylo"
    assert manifest["family_id"] == "mfs_sugar"
    assert manifest["parameters"]["mafft_mode"] == "auto"
    assert manifest["slurm"]["total_ogs"] == 3

    # DB row
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT run_id, kind FROM runs WHERE run_id = ?", ("phylo_mfs_test",)
        ).fetchone()
    finally:
        conn.close()
    assert row["kind"] == "family_phylo"


def test_protsetphylo_phylo_slurm_iqtree_fast_flag(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _create_family(paths, "abc_trans", ["OG0002000"])

    monkeypatch.setattr(
        "fungalphylo.core.slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "phylo-slurm",
            "--family-id", "abc_trans",
            "--account", "project_123",
            "--output-run-id", "phylo_fast_test",
            "--iqtree-fast",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    worker_text = (project_dir / "runs/phylo_fast_test/slurm/phylo_worker.sbatch").read_text(
        encoding="utf-8"
    )
    assert "-fast" in worker_text

    manifest = json.loads(
        (project_dir / "runs/phylo_fast_test/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["parameters"]["iqtree_fast"] is True


def test_protsetphylo_phylo_slurm_custom_step_resources(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _create_family(paths, "glyco_hydro", ["OG0003000"])

    monkeypatch.setattr(
        "fungalphylo.core.slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "phylo-slurm",
            "--family-id", "glyco_hydro",
            "--account", "project_123",
            "--output-run-id", "phylo_custom_res",
            "--align-time", "06:00:00",
            "--align-cpus", "16",
            "--tree-time", "24:00:00",
            "--tree-mem-per-cpu", "8G",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    orch_text = (project_dir / "runs/phylo_custom_res/slurm/phylo_orchestrate.sh").read_text(
        encoding="utf-8"
    )
    assert "--time=06:00:00" in orch_text
    assert "--cpus-per-task=16" in orch_text
    assert "--time=24:00:00" in orch_text
    assert "--mem-per-cpu=8G" in orch_text

    manifest = json.loads(
        (project_dir / "runs/phylo_custom_res/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["slurm"]["align"]["time"] == "06:00:00"
    assert manifest["slurm"]["align"]["cpus"] == 16
    assert manifest["slurm"]["tree"]["time"] == "24:00:00"
    assert manifest["slurm"]["tree"]["mem_per_cpu"] == "8G"


def test_protsetphylo_phylo_slurm_uses_og_selected_fallback(
    tmp_path: Path, monkeypatch
) -> None:
    """Falls back to og_selected/ when og_placed/ doesn't exist."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)

    family_id = "fallback_fam"
    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO families(family_id, created_at, pfams) "
            "VALUES(?, datetime('now'), 'PF00002')",
            (family_id,),
        )
        conn.commit()
    finally:
        conn.close()

    # Only create og_selected (no og_placed)
    og_selected = paths.family_og_selected_dir(family_id)
    og_selected.mkdir(parents=True)
    (og_selected / "OG0000099.fa").write_text(">p1\nM\n", encoding="utf-8")

    monkeypatch.setattr(
        "fungalphylo.core.slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "phylo-slurm",
            "--family-id", family_id,
            "--account", "project_123",
            "--output-run-id", "phylo_fallback",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "og_selected" in result.output


def test_protsetphylo_phylo_slurm_no_family_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "phylo-slurm",
            "--family-id", "nonexistent",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "Family not found" in result.output
