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


def _write_tools_yaml(paths: ProjectPaths) -> None:
    paths.tools_yaml.write_text(
        "busco:\n"
        '  bin_dir: ""\n'
        '  command: "busco"\n'
        "mafft:\n"
        '  command: "mafft"\n'
        "hmmer:\n"
        '  hmmbuild_cmd: "hmmbuild"\n'
        '  hmmsearch_cmd: "hmmsearch"\n',
        encoding="utf-8",
    )


def _seed_of_results(paths: ProjectPaths, run_id: str, og_names: list[str]) -> Path:
    """Create a fake OrthoFinder results directory with Orthogroup_Sequences/."""
    run_dir = paths.run_dir(run_id)
    of_results = run_dir / "orthofinder_results" / "Results_Test" / "Orthogroup_Sequences"
    of_results.mkdir(parents=True)
    for name in og_names:
        (of_results / f"{name}.fa").write_text(
            f">{name}_SpA|p1\nMPEPTIDE\n>{name}_SpB|p2\nMPEPTIDE\n",
            encoding="utf-8",
        )
    # Write manifest
    manifest = {
        "run_id": run_id,
        "kind": "orthofinder",
        "created_at": _now(),
    }
    manifest_path = paths.run_manifest(run_id)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    # DB row
    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?)",
            ("__family__", _now(), "__family__", "__family__"),
        )
        conn.execute(
            "INSERT INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?,?,?)",
            (run_id, "__family__", "orthofinder", _now(), str(manifest_path.relative_to(paths.root)), "dummy"),
        )
        conn.commit()
    finally:
        conn.close()
    return of_results


def _seed_standalone(paths: ProjectPaths, family_id: str, entries: dict[str, str]) -> Path:
    standalone_dir = paths.family_selected_dir(family_id) / "standalone"
    standalone_dir.mkdir(parents=True)
    for name, content in entries.items():
        (standalone_dir / f"{name}.faa").write_text(content, encoding="utf-8")
    return standalone_dir


def test_place_standalone_writes_script(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_of_results(paths, "of_test", ["OG0000001", "OG0000002"])
    _seed_standalone(paths, "mfs_sugar", {
        "Ambmo": ">Ambmo|STP1\nMPEPTIDE\n",
        "OutSp": ">OutSp|SUT3\nMPEPTIDE\n",
    })

    monkeypatch.setattr(
        "fungalphylo.core.slurm.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "mfs_sugar",
            "--run-id", "of_test",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "OGs to profile:      2" in result.output
    assert "2 sequences" in result.output

    script_path = (
        paths.family_dir("mfs_sugar") / "place_standalone" / "slurm" / "place_standalone.sbatch"
    )
    assert script_path.exists()

    script = script_path.read_text(encoding="utf-8")
    assert "module load biokit" in script
    assert "hmmbuild" in script
    assert "hmmsearch" in script
    assert "mafft" in script
    assert "--auto" in script
    assert "placements.tsv" in script
    # Should reference Orthogroup_Sequences as source
    assert "Orthogroup_Sequences" in script
    # Should reference og_placed as output
    assert "og_placed" in script


def test_place_standalone_submit_mocked(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_of_results(paths, "of_sub", ["OG0000001"])
    _seed_standalone(paths, "test_fam", {"Ambmo": ">Ambmo|STP1\nMPEPTIDE\n"})

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 77777\n")

    monkeypatch.setattr(
        "fungalphylo.core.slurm.subprocess.run",
        _fake_run,
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "test_fam",
            "--run-id", "of_sub",
            "--account", "project_123",
            "--submit",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Submitted batch job 77777" in result.output
    assert len(calls) == 1


def test_place_standalone_missing_of_results_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_standalone(paths, "no_ogs", {"Ambmo": ">Ambmo|STP1\nMPEPTIDE\n"})

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "no_ogs",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0


def test_place_standalone_no_standalone_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_of_results(paths, "of_no_stand", ["OG0000001"])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "no_stand",
            "--run-id", "of_no_stand",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No standalone FASTAs" in result.output
