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
    _write_tools_yaml_optional(paths, str(bin_dir), command)


def _write_tools_yaml_optional(paths: ProjectPaths, bin_dir: str, command: str = "cluster_interproscan") -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        "  bin_dir: \"\"\n"
        "  command: \"busco\"\n"
        "interproscan:\n"
        f"  bin_dir: {json.dumps(bin_dir)}\n"
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
    worker = project_dir / "runs" / "ipr_test" / "slurm" / "interproscan_worker.sbatch"
    controller = project_dir / "runs" / "ipr_test" / "scripts" / "interproscan_controller.py"
    queue = project_dir / "runs" / "ipr_test" / "queue.tsv"
    manifest_path = project_dir / "runs" / "ipr_test" / "manifest.json"
    assert launcher.exists()
    assert worker.exists()
    assert controller.exists()
    assert queue.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["kind"] == "interproscan"
    assert manifest["interproscan"]["applications"] == ["pfam", "panther"]
    assert manifest["interproscan"]["formats"] == ["TSV"]
    assert manifest["interproscan"]["controller_mode"] == "submit_and_poll"
    assert manifest["slurm"]["submit"] is False

    worker_text = worker.read_text(encoding="utf-8")
    launcher_text = launcher.read_text(encoding="utf-8")
    controller_text = controller.read_text(encoding="utf-8")
    assert "module load biokit" in worker_text
    assert "module load interproscan" in worker_text
    assert 'OUTPUT_TSV="${OUTPUT_TSV:?missing OUTPUT_TSV}"' in worker_text
    assert "-appl 'pfam'" in worker_text
    assert "-appl 'panther'" in worker_text
    assert '-f \'TSV\'' in worker_text
    assert '-o "$OUTPUT_TSV"' in worker_text
    assert "#SBATCH --partition=small" in worker_text
    assert f'python3 "{controller.as_posix()}"' in launcher_text
    assert '"sbatch",' in controller_text
    assert '"sacct", "-n", "-P"' in controller_text
    assert '"squeue", "-h", "-j"' in controller_text
    assert 'f"OUTPUT_TSV={row[\'tsv_path\']}"' in controller_text

    with queue.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert [row["portal_id"] for row in rows] == ["PortalA", "PortalB"]
    assert all(row["status"] == "pending" for row in rows)
    assert rows[0]["tsv_path"].endswith("/PortalA/PortalA.tsv")
    assert rows[1]["tsv_path"].endswith("/PortalB/PortalB.tsv")


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


def test_interproscan_slurm_allows_distinct_worker_resources(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

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
            "staging1",
            "--run-id",
            "ipr_resources",
            "--worker-partition",
            "long",
            "--worker-time",
            "72:00:00",
            "--worker-cpus",
            "8",
            "--worker-mem",
            "32G",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    worker = project_dir / "runs" / "ipr_resources" / "slurm" / "interproscan_worker.sbatch"
    manifest = json.loads((project_dir / "runs" / "ipr_resources" / "manifest.json").read_text(encoding="utf-8"))
    worker_text = worker.read_text(encoding="utf-8")
    assert "#SBATCH --partition=long" in worker_text
    assert "#SBATCH --time=72:00:00" in worker_text
    assert "#SBATCH --cpus-per-task=8" in worker_text
    assert "#SBATCH --mem=32G" in worker_text
    assert manifest["slurm"]["worker_partition"] == "long"
    assert manifest["slurm"]["worker_time"] == "72:00:00"
    assert manifest["slurm"]["worker_cpus"] == 8
    assert manifest["slurm"]["worker_mem"] == "32G"


def test_interproscan_slurm_limit_restricts_queue_to_first_n_proteomes(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

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
            "staging1",
            "--run-id",
            "ipr_limit",
            "--limit",
            "1",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    queue = project_dir / "runs" / "ipr_limit" / "queue.tsv"
    manifest = json.loads((project_dir / "runs" / "ipr_limit" / "manifest.json").read_text(encoding="utf-8"))
    with queue.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert [row["portal_id"] for row in rows] == ["PortalA"]
    assert manifest["interproscan"]["limit"] == 1
    assert manifest["interproscan"]["n_proteomes"] == 1


def test_interproscan_slurm_does_not_require_bin_dir_when_modules_are_used(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

    _write_tools_yaml_optional(paths, "", "cluster_interproscan")

    result = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging1",
            "--run-id",
            "ipr_modules",
            str(project_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    worker = project_dir / "runs" / "ipr_modules" / "slurm" / "interproscan_worker.sbatch"
    manifest = json.loads((project_dir / "runs" / "ipr_modules" / "manifest.json").read_text(encoding="utf-8"))
    worker_text = worker.read_text(encoding="utf-8")
    assert "module load biokit" in worker_text
    assert "module load interproscan" in worker_text
    assert "export PATH=" not in worker_text
    assert manifest["interproscan"]["bin_dir"] is None
    assert manifest["interproscan"]["module_loads"] == ["biokit", "interproscan"]


def test_interproscan_slurm_rejects_non_tsv_formats_for_puhti_wrapper(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

    _write_tools_yaml_optional(paths, "", "cluster_interproscan")

    result = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging1",
            "--run-id",
            "ipr_gff3",
            "--format",
            "tsv",
            "--format",
            "gff3",
            str(project_dir),
        ],
    )

    assert result.exit_code != 0
    assert "supports only" in result.output
    assert "TSV" in result.output


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
