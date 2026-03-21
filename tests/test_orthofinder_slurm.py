from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_staging(paths: ProjectPaths, staging_id: str, n_proteomes: int = 3) -> None:
    proteomes_dir = paths.staging_proteomes_dir(staging_id)
    proteomes_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_proteomes):
        (proteomes_dir / f"Portal{i}.faa").write_text(
            f">Portal{i}|p1\nMPEPTIDE\n", encoding="utf-8"
        )

    manifest = {
        "staging_id": staging_id,
        "created_at": _now(),
        "outputs": {"proteomes_dir": f"staging/{staging_id}/proteomes"},
        "actions": [],
    }
    manifest_path = paths.staging_manifest(staging_id)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    paths.staging_checksums(staging_id).write_text("", encoding="utf-8")

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?)",
            (staging_id, _now(), str(manifest_path.relative_to(paths.root)), "dummy-sha256"),
        )
        conn.commit()
    finally:
        conn.close()


def _write_tools_yaml(paths: ProjectPaths, *, env_activate: str = "") -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        '  bin_dir: ""\n'
        '  command: "busco"\n'
        "orthofinder:\n"
        f"  env_activate: {json.dumps(env_activate)}\n"
        '  command: "orthofinder"\n'
        '  msa_program: "mafft"\n',
        encoding="utf-8",
    )


def test_orthofinder_slurm_writes_script_for_latest_staging(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg_old")
    _seed_staging(paths, "stg_new")
    _write_tools_yaml(paths)

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--run-id", "of_test",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    script_path = project_dir / "runs" / "of_test" / "slurm" / "orthofinder.sbatch"
    manifest_path = project_dir / "runs" / "of_test" / "manifest.json"
    assert script_path.exists()
    assert manifest_path.exists()

    script = script_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # Should use latest staging
    proteomes_dir = paths.staging_proteomes_dir("stg_new")
    assert f'-f "{proteomes_dir.as_posix()}"' in script
    assert "-A mafft" in script
    assert "--time=48:00:00" in script
    assert "--cpus-per-task=16" in script
    assert "--mem-per-cpu=4G" in script
    assert "module purge" not in script  # no env_activate configured
    assert "module load mafft" in script  # MSA tool always loaded
    assert manifest["kind"] == "orthofinder"
    assert manifest["source_kind"] == "staging"
    assert manifest["source_id"] == "stg_new"
    assert manifest["orthofinder"]["msa_program"] == "mafft"
    assert manifest["slurm"]["submit"] is False
    assert "3 .faa files" in result.output

    # Verify DB row
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT run_id, staging_id, kind FROM runs WHERE run_id = ?", ("of_test",)
        ).fetchone()
    finally:
        conn.close()
    assert row["run_id"] == "of_test"
    assert row["staging_id"] == "stg_new"
    assert row["kind"] == "orthofinder"


def test_orthofinder_slurm_with_env_activate(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg1")

    env_path = tmp_path / "of_env" / "bin" / "activate"
    env_path.parent.mkdir(parents=True)
    env_path.write_text("# activate\n", encoding="utf-8")
    _write_tools_yaml(paths, env_activate=str(env_path))

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--run-id", "of_env",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    script = (project_dir / "runs/of_env/slurm/orthofinder.sbatch").read_text(encoding="utf-8")
    assert "module purge" in script
    assert "module load StdEnv" in script
    assert "module load python-data" in script
    assert f'source "{env_path.resolve().as_posix()}"' in script
    assert "module load mafft" in script


def test_orthofinder_slurm_with_family_id(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)

    # Create family selected dir with FASTAs
    selected_dir = paths.family_selected_dir("mfs_sugar")
    selected_dir.mkdir(parents=True)
    (selected_dir / "SpeciesA.faa").write_text(">p1\nMPEPTIDE\n", encoding="utf-8")
    (selected_dir / "SpeciesB.faa").write_text(">p2\nMPEPTIDE\n", encoding="utf-8")

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--family-id", "mfs_sugar",
            "--run-id", "of_family",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    manifest = json.loads(
        (project_dir / "runs/of_family/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["source_kind"] == "family"
    assert manifest["source_id"] == "mfs_sugar"
    assert manifest["paths"]["input_dir"] == str(selected_dir)

    script = (project_dir / "runs/of_family/slurm/orthofinder.sbatch").read_text(encoding="utf-8")
    assert f'-f "{selected_dir.as_posix()}"' in script
    assert "2 .faa files" in result.output

    # DB should use __family__ sentinel
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT staging_id FROM runs WHERE run_id = ?", ("of_family",)
        ).fetchone()
    finally:
        conn.close()
    assert row["staging_id"] == "__family__"


def test_orthofinder_slurm_with_explicit_input_dir(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)

    custom_dir = tmp_path / "my_fastas"
    custom_dir.mkdir()
    (custom_dir / "sp1.faa").write_text(">p1\nM\n", encoding="utf-8")

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--input-dir", str(custom_dir),
            "--run-id", "of_custom",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    manifest = json.loads(
        (project_dir / "runs/of_custom/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["source_kind"] == "custom"


def test_orthofinder_slurm_rejects_family_and_staging_together(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--family-id", "fam1",
            "--staging-id", "stg1",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "either --family-id or --staging-id" in result.output.lower()


def test_orthofinder_slurm_submit_mocked(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg_sub")
    _write_tools_yaml(paths)

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 54321\n")

    monkeypatch.setattr("fungalphylo.cli.commands.orthofinder_slurm.subprocess.run", _fake_run)

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--staging-id", "stg_sub",
            "--run-id", "of_sub",
            "--submit",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Submitted batch job 54321" in result.output
    expected = str(project_dir / "runs/of_sub/slurm/orthofinder.sbatch")
    assert calls == [["sbatch", expected]]


def test_orthofinder_slurm_resume_refreshes_script(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg_resume")
    _write_tools_yaml(paths)

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    # Create initial run
    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--run-id", "of_orig",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    original_script = (project_dir / "runs/of_orig/slurm/orthofinder.sbatch").read_text(
        encoding="utf-8"
    )
    assert "--time=48:00:00" in original_script

    # Resume with updated time
    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--resume-run-id", "of_orig",
            "--time", "72:00:00",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Resuming OrthoFinder run" in result.output

    refreshed = (project_dir / "runs/of_orig/slurm/orthofinder.sbatch").read_text(
        encoding="utf-8"
    )
    assert "--time=72:00:00" in refreshed

    # No second DB row
    conn = connect(paths.db_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM runs WHERE kind = 'orthofinder'"
        ).fetchone()["n"]
    finally:
        conn.close()
    assert n == 1


def test_orthofinder_slurm_missing_staging_proteomes(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--staging-id", "nonexistent",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "Missing staged proteomes dir" in result.output


def test_orthofinder_slurm_custom_msa_program(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg_msa")
    _write_tools_yaml(paths)

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--run-id", "of_msa",
            "--msa-program", "muscle",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    script = (project_dir / "runs/of_msa/slurm/orthofinder.sbatch").read_text(encoding="utf-8")
    assert "-A muscle" in script

    manifest = json.loads(
        (project_dir / "runs/of_msa/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["orthofinder"]["msa_program"] == "muscle"


def test_orthofinder_slurm_og_only_flag(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg_og")
    _write_tools_yaml(paths)

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--run-id", "of_og",
            "--og-only",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    script = (project_dir / "runs/of_og/slurm/orthofinder.sbatch").read_text(encoding="utf-8")
    assert "-M dendroblast" in script
    assert "-A mafft" not in script

    manifest = json.loads(
        (project_dir / "runs/of_og/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["orthofinder"]["og_only"] is True


def test_orthofinder_slurm_og_only_not_set_by_default(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "stg_no_og")
    _write_tools_yaml(paths)

    monkeypatch.setattr(
        "fungalphylo.cli.commands.orthofinder_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "orthofinder-slurm",
            "--account", "project_123",
            "--run-id", "of_no_og",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    script = (project_dir / "runs/of_no_og/slurm/orthofinder.sbatch").read_text(encoding="utf-8")
    assert "-M dendroblast" not in script
    assert "-A mafft" in script

    manifest = json.loads(
        (project_dir / "runs/of_no_og/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["orthofinder"]["og_only"] is False
