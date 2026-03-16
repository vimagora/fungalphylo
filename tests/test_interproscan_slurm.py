from __future__ import annotations

import csv
import json
import sys
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


def _seed_staging(paths: ProjectPaths, staging_id: str) -> None:
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
    assert f"'{sys.executable}' " in launcher_text
    assert f'"{controller.as_posix()}"' in launcher_text
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


def test_interproscan_slurm_resume_reuses_existing_queue(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

    bin_dir = tmp_path / "ipr-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    initial = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging1",
            "--run-id",
            "ipr_resume",
            str(project_dir),
        ],
    )
    assert initial.exit_code == 0, initial.output

    queue = project_dir / "runs" / "ipr_resume" / "queue.tsv"
    with queue.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    rows[0]["status"] = "completed"
    rows[0]["submitted_job_id"] = "11111"
    rows[1]["status"] = "submitted"
    rows[1]["submitted_job_id"] = "22222"
    with queue.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    resumed = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--resume-run-id",
            "ipr_resume",
            str(project_dir),
        ],
    )
    assert resumed.exit_code == 0, resumed.output
    assert "Resuming InterProScan run:  ipr_resume" in resumed.output
    assert "Reused queue ledger:" in resumed.output

    with queue.open("r", encoding="utf-8", newline="") as f:
        resumed_rows = list(csv.DictReader(f, delimiter="\t"))
    assert resumed_rows == rows


def test_interproscan_slurm_resume_submit_uses_existing_launcher(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_staging(paths, "staging1")

    bin_dir = tmp_path / "ipr-bin"
    bin_dir.mkdir()
    _write_tools_yaml(paths, bin_dir=bin_dir)

    initial = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--account",
            "project_1234567",
            "--staging-id",
            "staging1",
            "--run-id",
            "ipr_resume_submit",
            str(project_dir),
        ],
    )
    assert initial.exit_code == 0, initial.output

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 12345\n")

    monkeypatch.setattr("fungalphylo.cli.commands.interproscan_slurm.subprocess.run", _fake_run)

    resumed = runner.invoke(
        app,
        [
            "interproscan-slurm",
            "--resume-run-id",
            "ipr_resume_submit",
            "--submit",
            str(project_dir),
        ],
    )
    assert resumed.exit_code == 0, resumed.output
    assert calls == [[
        "sbatch",
        str(project_dir / "runs" / "ipr_resume_submit" / "slurm" / "interproscan_launcher.sbatch"),
    ]]
    assert "Submitted batch job 12345" in resumed.output


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


def test_interproscan_slurm_controller_includes_retry_failed_sequences(tmp_path: Path) -> None:
    """Verify the generated controller has retry_failed_sequences logic and is valid Python."""
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
            "ipr_retry",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    controller = project_dir / "runs" / "ipr_retry" / "scripts" / "interproscan_controller.py"
    controller_text = controller.read_text(encoding="utf-8")

    # Verify retry logic is present
    assert "def retry_failed_sequences" in controller_text
    assert ".failed_sequences" in controller_text
    assert "retry_failed_sequences()" in controller_text
    assert ".failed_sequences." in controller_text  # numbered backup rotation

    # Verify the generated script is valid Python
    compile(controller_text, str(controller), "exec")


def test_interproscan_slurm_retry_failed_sequences_appends_results(tmp_path: Path) -> None:
    """Simulate the retry logic: rotate .failed_sequences, append results, clean up."""
    import importlib
    import types

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
            "ipr_retry_sim",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    # Set up: mark both portals as completed in queue
    queue = project_dir / "runs" / "ipr_retry_sim" / "queue.tsv"
    with queue.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    for row in rows:
        row["status"] = "completed"
        row["submitted_job_id"] = "99999"
    with queue.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    # Create main TSV and .failed_sequences for PortalA only
    results_dir = Path(rows[0]["results_dir"])
    results_dir.mkdir(parents=True, exist_ok=True)
    tsv_path = Path(rows[0]["tsv_path"])
    tsv_path.write_text("col1\tcol2\noriginal_line\tdata\n", encoding="utf-8")
    failed_path = Path(f"{tsv_path}.failed_sequences")
    failed_path.write_text(">PortalA|999\nMPEPTIDESEQ\n", encoding="utf-8")

    # Also create PortalB's TSV (no failed_sequences — should be untouched)
    results_dir_b = Path(rows[1]["results_dir"])
    results_dir_b.mkdir(parents=True, exist_ok=True)
    tsv_path_b = Path(rows[1]["tsv_path"])
    tsv_path_b.write_text("col1\tcol2\nportalb_line\tdata\n", encoding="utf-8")

    # Load the generated controller as a module so we can test retry_failed_sequences
    controller = project_dir / "runs" / "ipr_retry_sim" / "scripts" / "interproscan_controller.py"
    controller_text = controller.read_text(encoding="utf-8")

    # Patch submit_row and wait_for_terminal_state in the controller source
    # so they don't actually call sbatch/squeue/sacct
    patched = controller_text.replace(
        "def main() -> int:",
        # Inject stubs before main
        "def _real_submit_row(row):\n"
        "    raise AssertionError('should not reach real submit')\n\n"
        "def main() -> int:",
    )

    # Load controller module
    spec = importlib.util.spec_from_loader("controller_test", loader=None)
    mod = types.ModuleType("controller_test")
    mod.__spec__ = spec
    exec(compile(patched, str(controller), "exec"), mod.__dict__)

    # Stub submit_row to return a fake job ID
    mod.submit_row = lambda row: "55555"
    # Stub wait_for_terminal_state to return COMPLETED
    mod.wait_for_terminal_state = lambda job_id: "COMPLETED"

    # Create the retry output that the worker would produce
    retry_output = Path(f"{tsv_path}.retry_1")

    # We need the worker to "produce" the retry output before the controller reads it.
    # Patch submit_row to also create the retry output file.
    def fake_submit(row):
        out = Path(row["tsv_path"])
        out.write_text("retry_col1\tretry_col2\nretried_line\trecovered\n", encoding="utf-8")
        return "55555"

    mod.submit_row = fake_submit

    rc = mod.retry_failed_sequences()
    assert rc == 0

    # Verify: .failed_sequences rotated to .failed_sequences.1
    assert not failed_path.exists(), ".failed_sequences should be rotated away"
    rotated = Path(f"{tsv_path}.failed_sequences.1")
    assert rotated.exists(), ".failed_sequences.1 should exist as audit trail"
    assert ">PortalA|999" in rotated.read_text(encoding="utf-8")

    # Verify: retry results appended to main TSV
    final_tsv = tsv_path.read_text(encoding="utf-8")
    assert "original_line" in final_tsv
    assert "retried_line" in final_tsv

    # Verify: retry temp file cleaned up
    assert not retry_output.exists()

    # Verify: PortalB untouched (no .failed_sequences)
    assert tsv_path_b.read_text(encoding="utf-8") == "col1\tcol2\nportalb_line\tdata\n"

    # Verify: queue.tsv updated with retry note
    with queue.open("r", encoding="utf-8", newline="") as f:
        final_rows = list(csv.DictReader(f, delimiter="\t"))
    portal_a_row = [r for r in final_rows if r["portal_id"] == "PortalA"][0]
    assert "retry 1 done" in portal_a_row["note"]
    assert "all sequences recovered" in portal_a_row["note"]


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
