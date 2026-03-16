from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fungalphylo.core.slurm import infer_account_from_project_dir
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


def _seed_staging(paths: ProjectPaths, staging_id: str) -> None:
    proteomes_dir = paths.staging_proteomes_dir(staging_id)
    proteomes_dir.mkdir(parents=True, exist_ok=True)
    (proteomes_dir / "TestPortal.faa").write_text(">p1\nMPEPTIDE\n", encoding="utf-8")

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
            """
            INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256)
            VALUES(?,?,?,?)
            """,
            (staging_id, _now(), str(manifest_path.relative_to(paths.root)), "dummy-sha256"),
        )
        conn.commit()
    finally:
        conn.close()


def _write_tools_yaml(paths: ProjectPaths, *, bin_dir: Path, command: str = "busco") -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        f"  bin_dir: {json.dumps(str(bin_dir))}\n"
        f"  command: {json.dumps(command)}\n",
        encoding="utf-8",
    )


def test_busco_slurm_writes_script_for_latest_staging_without_submitting(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging_old")
    _seed_staging(paths, "staging_new")

    bin_dir = tmp_path / "busco-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir, command="busco-custom")

    def _unexpected_submit(*args, **kwargs):
        raise AssertionError("submit path should not be reached without --submit")

    monkeypatch.setattr("fungalphylo.cli.commands.busco_slurm.subprocess.run", _unexpected_submit)

    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--account",
            "project_1234567",
            "--run-id",
            "busco_test",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    script_path = project_dir / "runs" / "busco_test" / "slurm" / "busco.sbatch"
    manifest_path = project_dir / "runs" / "busco_test" / "manifest.json"
    assert script_path.exists()
    assert manifest_path.exists()

    script = script_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert 'STAGING_ID="staging_new"' in script
    assert f'SEQ_DIR="{paths.staging_proteomes_dir("staging_new").as_posix()}"' in script
    assert f'export PATH="{bin_dir.as_posix()}:$PATH"' in script
    assert '"busco-custom" -c "$THREADS"' in script
    assert manifest["kind"] == "busco"
    assert manifest["staging_id"] == "staging_new"
    assert manifest["paths"]["script_path"] == "runs/busco_test/slurm/busco.sbatch"
    assert manifest["paths"]["batch_root"] == "runs/busco_test/busco_results/busco_staging_new_busco_test"
    assert manifest["paths"]["batch_summary"] == "runs/busco_test/busco_results/busco_staging_new_busco_test/batch_summary.txt"
    assert manifest["slurm"]["submit"] is False

    conn = connect(paths.db_path)
    try:
        run_row = conn.execute(
            "SELECT run_id, staging_id, kind, manifest_path FROM runs WHERE run_id = ?",
            ("busco_test",),
        ).fetchone()
    finally:
        conn.close()

    assert run_row["run_id"] == "busco_test"
    assert run_row["staging_id"] == "staging_new"
    assert run_row["kind"] == "busco"
    assert run_row["manifest_path"] == "runs/busco_test/manifest.json"


def test_busco_slurm_submit_path_is_mockable_without_real_sbatch(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging_submit")

    bin_dir = tmp_path / "busco-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 12345\n")

    monkeypatch.setattr("fungalphylo.cli.commands.busco_slurm.subprocess.run", _fake_run)

    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging_submit",
            "--run-id",
            "busco_submit_test",
            "--submit",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    manifest = json.loads((project_dir / "runs" / "busco_submit_test" / "manifest.json").read_text(encoding="utf-8"))
    assert calls == [["sbatch", str(project_dir / "runs" / "busco_submit_test" / "slurm" / "busco.sbatch")]]
    assert "Submitted batch job 12345" in result.output
    assert manifest["slurm"]["submit"] is True


def test_busco_slurm_requires_existing_staged_proteomes_dir(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    bin_dir = tmp_path / "busco-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "missing_stage",
            str(project_dir),
        ],
    )

    assert result.exit_code != 0
    assert "Missing staged proteomes dir for missing_stage" in result.output


def test_busco_slurm_resume_refreshes_script_and_preserves_run(tmp_path: Path, monkeypatch) -> None:
    """--resume-run-id loads the existing manifest, refreshes the script, and does not create a new runs row."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging_resume")

    bin_dir = tmp_path / "busco-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir, command="busco-custom")

    monkeypatch.setattr(
        "fungalphylo.cli.commands.busco_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    # First: create an initial run
    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--account", "project_1234567",
            "--run-id", "busco_orig",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    original_script = (project_dir / "runs/busco_orig/slurm/busco.sbatch").read_text(encoding="utf-8")
    assert "--time=24:00:00" in original_script

    # Now resume with updated time
    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--resume-run-id", "busco_orig",
            "--time", "48:00:00",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Resuming BUSCO run" in result.output

    refreshed_script = (project_dir / "runs/busco_orig/slurm/busco.sbatch").read_text(encoding="utf-8")
    assert "--time=48:00:00" in refreshed_script
    assert 'STAGING_ID="staging_resume"' in refreshed_script
    assert '"busco-custom"' in refreshed_script

    # Verify no second runs row was created
    conn = connect(paths.db_path)
    try:
        run_count = conn.execute("SELECT COUNT(*) AS n FROM runs WHERE kind = 'busco'").fetchone()["n"]
    finally:
        conn.close()
    assert run_count == 1


def test_busco_slurm_resume_submit_uses_existing_script(tmp_path: Path, monkeypatch) -> None:
    """--resume-run-id --submit resubmits the refreshed script."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging_resub")

    bin_dir = tmp_path / "busco-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 99999\n")

    # Create initial run without submit
    monkeypatch.setattr(
        "fungalphylo.cli.commands.busco_slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )
    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--account", "project_1234567",
            "--run-id", "busco_resub",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    # Resume with submit
    monkeypatch.setattr("fungalphylo.cli.commands.busco_slurm.subprocess.run", _fake_run)
    result = runner.invoke(
        app,
        [
            "busco-slurm",
            "--resume-run-id", "busco_resub",
            "--submit",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Submitted batch job 99999" in result.output
    expected_script = str(project_dir / "runs/busco_resub/slurm/busco.sbatch")
    assert calls == [["sbatch", expected_script]]


def test_infer_account_from_project_dir_uses_scratch_prefix() -> None:
    assert infer_account_from_project_dir(Path("/scratch/project_2015320/myproj")) == "project_2015320"
    assert infer_account_from_project_dir(Path("/tmp/myproj")) is None
