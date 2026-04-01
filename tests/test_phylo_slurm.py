from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_filtered_ogs(
    paths: ProjectPaths, run_id: str, og_names: list[str]
) -> Path:
    """Create a fake orthofinder run with filtered orthogroups."""
    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "kind": "orthofinder",
        "created_at": "2026-03-26T00:00:00+00:00",
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    filtered_dir = run_dir / "filtered_orthogroups"
    filtered_dir.mkdir(parents=True)
    for name in og_names:
        (filtered_dir / f"{name}.fa").write_text(
            f">{name}_SpA|p1\nMPEPTIDE\n>{name}_SpB|p2\nMPEPTIDE\n",
            encoding="utf-8",
        )
    return filtered_dir


def _write_tools_yaml(paths: ProjectPaths) -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        '  bin_dir: ""\n'
        '  command: "busco"\n'
        "mafft:\n"
        '  command: "mafft"\n'
        "trimal:\n"
        '  command: "trimal"\n'
        "iqtree:\n"
        '  command: "iqtree3"\n',
        encoding="utf-8",
    )


def test_phylo_slurm_writes_orchestrator_and_worker(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_filtered_ogs(paths, "of_run1", ["OG0000001", "OG0000002", "OG0000003"])

    monkeypatch.setattr(
        "fungalphylo.cli.commands.phylo_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "phylo-slurm",
            "--account", "project_123",
            "--run-id", "of_run1",
            "--output-run-id", "phylo_test",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Orthogroups:  3" in result.output

    # Both scripts written
    orchestrator = project_dir / "runs/phylo_test/slurm/phylo_orchestrate.sh"
    worker = project_dir / "runs/phylo_test/slurm/phylo_worker.sbatch"
    assert orchestrator.exists()
    assert worker.exists()

    # Orchestrator references worker and filelist
    orch_text = orchestrator.read_text(encoding="utf-8")
    assert "phylo_worker.sbatch" in orch_text
    assert "og_filelist.txt" in orch_text
    assert "MAX_ARRAY_SIZE=380" in orch_text
    assert "MAX_CONCURRENT=100" in orch_text
    assert ".treefile" in orch_text  # checks for completion

    # Worker has step-level resume and correct parameters
    worker_text = worker.read_text(encoding="utf-8")
    assert "--cpus-per-task=16" in worker_text
    assert "--mem-per-cpu=2G" in worker_text
    assert "--time=12:00:00" in worker_text
    assert "--retree 2" in worker_text
    assert "--maxiterate 1000" in worker_text
    assert "-gt 0.8" in worker_text
    assert "-cons 10" in worker_text
    assert "-m TEST" in worker_text
    assert "-B 1000" in worker_text
    assert "-alrt 1000" in worker_text
    assert '"mafft"' in worker_text
    assert '"trimal"' in worker_text
    assert '"iqtree3"' in worker_text
    # Step-level resume: checks for existing output
    assert '[ ! -s "$ALIGNED" ]' in worker_text
    assert '[ ! -s "$TRIMMED" ]' in worker_text
    assert "skipping MAFFT" in worker_text
    assert "skipping trimAl" in worker_text
    assert "skipping IQ-TREE" in worker_text

    # File list
    filelist = (project_dir / "runs/phylo_test/slurm/og_filelist.txt").read_text(encoding="utf-8")
    lines = [line for line in filelist.strip().split("\n") if line]
    assert len(lines) == 3
    assert all("OG000000" in line for line in lines)

    # Manifest
    manifest = json.loads(
        (project_dir / "runs/phylo_test/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["kind"] == "phylo"
    assert manifest["slurm"]["total_ogs"] == 3
    assert manifest["slurm"]["max_array_size"] == 380
    assert manifest["slurm"]["max_concurrent"] == 100
    assert manifest["parameters"]["mafft_maxiterate"] == 1000
    assert manifest["parameters"]["iqtree_model"] == "TEST"

    # DB row
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT run_id, kind FROM runs WHERE run_id = ?", ("phylo_test",)
        ).fetchone()
    finally:
        conn.close()
    assert row["kind"] == "phylo"


def test_phylo_slurm_auto_finds_filtered_ogs(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_filtered_ogs(paths, "of_auto", ["OG0000001"])

    monkeypatch.setattr(
        "fungalphylo.cli.commands.phylo_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "phylo-slurm",
            "--account", "project_123",
            "--output-run-id", "phylo_auto",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Using filtered OGs from run: of_auto" in result.output


def test_phylo_slurm_explicit_input_dir(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)

    custom_dir = tmp_path / "my_ogs"
    custom_dir.mkdir()
    (custom_dir / "OG0000001.fa").write_text(">p1\nM\n", encoding="utf-8")

    monkeypatch.setattr(
        "fungalphylo.cli.commands.phylo_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "phylo-slurm",
            "--account", "project_123",
            "--input-dir", str(custom_dir),
            "--output-run-id", "phylo_custom",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Orthogroups:  1" in result.output


def test_phylo_slurm_custom_parameters(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_filtered_ogs(paths, "of_params", ["OG0000001"])

    monkeypatch.setattr(
        "fungalphylo.cli.commands.phylo_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "phylo-slurm",
            "--account", "project_123",
            "--run-id", "of_params",
            "--output-run-id", "phylo_params",
            "--mafft-maxiterate", "500",
            "--trimal-gt", "0.5",
            "--iqtree-model", "LG+G4",
            "--iqtree-bootstrap", "2000",
            "--max-concurrent", "50",
            "--max-array-size", "200",
            "--cpus", "8",
            "--mem-per-cpu", "4G",
            "--time", "24:00:00",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    worker = (project_dir / "runs/phylo_params/slurm/phylo_worker.sbatch").read_text(
        encoding="utf-8"
    )
    assert "--maxiterate 500" in worker
    assert "-gt 0.5" in worker
    assert "-m LG+G4" in worker
    assert "-B 2000" in worker
    assert "--cpus-per-task=8" in worker
    assert "--mem-per-cpu=4G" in worker
    assert "--time=24:00:00" in worker

    orch = (project_dir / "runs/phylo_params/slurm/phylo_orchestrate.sh").read_text(
        encoding="utf-8"
    )
    assert "MAX_CONCURRENT=50" in orch
    assert "MAX_ARRAY_SIZE=200" in orch


def test_phylo_slurm_submit_mocked(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_filtered_ogs(paths, "of_sub", ["OG0000001"])

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted job 99999\n")

    monkeypatch.setattr("fungalphylo.cli.commands.phylo_slurm.subprocess.run", _fake_run)

    result = runner.invoke(
        app,
        [
            "phylo-slurm",
            "--account", "project_123",
            "--run-id", "of_sub",
            "--output-run-id", "phylo_sub",
            "--submit",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert "bash" in calls[0][0]


def test_phylo_slurm_no_filtered_ogs_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(
        app,
        [
            "phylo-slurm",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No OrthoFinder runs with filtered orthogroups found" in result.output
