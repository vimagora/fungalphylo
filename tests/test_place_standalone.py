from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths

runner = CliRunner()


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


def _seed_og_selected(paths: ProjectPaths, family_id: str, og_names: list[str]) -> Path:
    og_dir = paths.family_og_selected_dir(family_id)
    og_dir.mkdir(parents=True)
    for name in og_names:
        (og_dir / f"{name}.fa").write_text(
            f">{name}_SpA|p1\nMPEPTIDE\n>{name}_SpB|p2\nMPEPTIDE\n",
            encoding="utf-8",
        )
    return og_dir


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
    _seed_og_selected(paths, "mfs_sugar", ["OG0000001", "OG0000002"])
    _seed_standalone(paths, "mfs_sugar", {
        "Ambmo": ">Ambmo|STP1\nMPEPTIDE\n",
        "OutSp": ">OutSp|SUT3\nMPEPTIDE\n",
    })

    monkeypatch.setattr(
        "fungalphylo.cli.commands.protsetphylo.place_standalone.subprocess.run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not submit")),
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "mfs_sugar",
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
    assert str(paths.family_og_selected_dir("mfs_sugar")) in script


def test_place_standalone_submit_mocked(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_og_selected(paths, "test_fam", ["OG0000001"])
    _seed_standalone(paths, "test_fam", {"Ambmo": ">Ambmo|STP1\nMPEPTIDE\n"})

    calls: list[list[str]] = []

    def _fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(stdout="Submitted batch job 77777\n")

    monkeypatch.setattr(
        "fungalphylo.cli.commands.protsetphylo.place_standalone.subprocess.run",
        _fake_run,
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "test_fam",
            "--account", "project_123",
            "--submit",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Submitted batch job 77777" in result.output
    assert len(calls) == 1


def test_place_standalone_missing_og_dir_errors(tmp_path: Path) -> None:
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
    assert "og-apply" in result.output.lower()


def test_place_standalone_no_standalone_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _write_tools_yaml(paths)
    _seed_og_selected(paths, "no_stand", ["OG0000001"])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "place-standalone",
            "--family-id", "no_stand",
            "--account", "project_123",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No standalone FASTAs" in result.output
