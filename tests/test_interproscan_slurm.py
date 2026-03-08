from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect


runner = CliRunner()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_staging(paths: ProjectPaths, staging_id: str) -> None:
    staging_dir = paths.staging_dir(staging_id)
    proteomes_dir = paths.staging_proteomes_dir(staging_id)
    proteomes_dir.mkdir(parents=True, exist_ok=True)
    (proteomes_dir / "PortalA.faa").write_text(">p1\nMPEPTIDE\n", encoding="utf-8")
    (proteomes_dir / "PortalB.faa").write_text(">p2\nMQQQQ\n", encoding="utf-8")

    manifest_path = paths.staging_manifest(staging_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps({"staging_id": staging_id, "created_at": _now(), "outputs": {}}, indent=2) + "\n",
        encoding="utf-8",
    )
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


def _write_tools_yaml(paths: ProjectPaths, *, bin_dir: Path, command: str = "cluster_interproscan") -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        "  bin_dir: \"\"\n"
        "  command: \"busco\"\n"
        "interproscan:\n"
        f"  bin_dir: {json.dumps(str(bin_dir))}\n"
        f"  command: {json.dumps(command)}\n",
        encoding="utf-8",
    )


def test_interproscan_slurm_writes_launcher_worker_queue_and_manifest(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

    bin_dir = tmp_path / "ipr-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    def _unexpected_submit(*args, **kwargs):
        raise AssertionError("submit path should not be reached without --submit")

    monkeypatch.setattr("fungalphylo.cli.commands.interproscan_slurm.subprocess.run", _unexpected_submit)

    result = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging1",
            "--run-id",
            "ipr_test",
            "--application",
            "pfam",
            "--application",
            "panther",
            "--format",
            "tsv",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    launcher = project_dir / "runs" / "ipr_test" / "slurm" / "interproscan_launcher.sbatch"
    worker = project_dir / "runs" / "ipr_test" / "scripts" / "run_one_interproscan.sh"
    queue = project_dir / "runs" / "ipr_test" / "queue.tsv"
    manifest_path = project_dir / "runs" / "ipr_test" / "manifest.json"
    assert launcher.exists()
    assert worker.exists()
    assert queue.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["kind"] == "interproscan"
    assert manifest["interproscan"]["applications"] == ["pfam", "panther"]
    assert manifest["interproscan"]["formats"] == ["tsv"]
    assert manifest["slurm"]["submit"] is False

    worker_text = worker.read_text(encoding="utf-8")
    launcher_text = launcher.read_text(encoding="utf-8")
    assert "--applications 'pfam'" in worker_text
    assert "--applications 'panther'" in worker_text
    assert "--formats 'tsv'" in worker_text
    assert 'IPR_CMD="cluster_interproscan"' in launcher_text

    with queue.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert [row["portal_id"] for row in rows] == ["PortalA", "PortalB"]
    assert all(row["status"] == "pending" for row in rows)


def test_interproscan_slurm_submit_path_is_mockable(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

    bin_dir = tmp_path / "ipr-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 99999\n")

    monkeypatch.setattr("fungalphylo.cli.commands.interproscan_slurm.subprocess.run", _fake_run)

    result = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging1",
            "--run-id",
            "ipr_submit",
            "--submit",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [[
        "sbatch",
        str(project_dir / "runs" / "ipr_submit" / "slurm" / "interproscan_launcher.sbatch"),
    ]]
    assert "Submitted batch job 99999" in result.output


def test_interproscan_slurm_requires_existing_staging_dir(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    bin_dir = tmp_path / "ipr-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    result = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "missing_stage",
            str(project_dir),
        ],
    )

    assert result.exit_code != 0
    assert "Missing staged proteomes dir for missing_stage" in result.output
