from __future__ import annotations

import csv
import json
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_orthofinder_run(
    paths: ProjectPaths,
    run_id: str,
    species: list[str],
    orthogroups: dict[str, list[int]],
) -> Path:
    """Create a fake OrthoFinder run with GeneCount.tsv and Orthogroup_Sequences/."""
    run_dir = paths.run_dir(run_id)
    of_results = run_dir / "orthofinder_results" / "Results_Mar26"

    # Manifest
    manifest = {
        "run_id": run_id,
        "kind": "orthofinder",
        "created_at": "2026-03-26T00:00:00+00:00",
        "paths": {
            "results_dir": str((run_dir / "orthofinder_results").relative_to(paths.root)),
        },
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    # GeneCount.tsv
    og_dir = of_results / "Orthogroups"
    og_dir.mkdir(parents=True)
    gene_count_path = og_dir / "Orthogroups.GeneCount.tsv"
    with gene_count_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(["Orthogroup"] + species + ["Total"])
        for og_id, counts in orthogroups.items():
            writer.writerow([og_id] + counts + [sum(counts)])

    # Orthogroup_Sequences/
    seq_dir = of_results / "Orthogroup_Sequences"
    seq_dir.mkdir(parents=True)
    for og_id, counts in orthogroups.items():
        seqs = []
        for i, c in enumerate(counts):
            for j in range(c):
                seqs.append(f">{species[i]}|p{j}\nMPEPTIDE\n")
        (seq_dir / f"{og_id}.fa").write_text("".join(seqs), encoding="utf-8")

    return of_results


def test_filter_selects_single_copy_ogs(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    # 4 species: OG0 has 4/4 single-copy, OG1 has 3/4 (75%), OG2 has 2/4 (50%)
    species = ["SpA", "SpB", "SpC", "SpD"]
    orthogroups = {
        "OG0000000": [1, 1, 1, 1],  # 100% single-copy → pass
        "OG0000001": [1, 1, 1, 2],  # 75% single-copy → pass at 0.75
        "OG0000002": [1, 1, 3, 0],  # 50% single-copy → fail
        "OG0000003": [0, 0, 0, 0],  # 0% → fail
    }
    _seed_orthofinder_run(paths, "of_test", species, orthogroups)

    result = runner.invoke(
        app,
        [
            "filter-orthogroups",
            "--run-id", "of_test",
            "--min-single-copy", "0.75",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Selected orthogroups: 2" in result.output

    out_dir = paths.run_dir("of_test") / "filtered_orthogroups"
    assert (out_dir / "OG0000000.fa").exists()
    assert (out_dir / "OG0000001.fa").exists()
    assert not (out_dir / "OG0000002.fa").exists()
    assert not (out_dir / "OG0000003.fa").exists()

    # Check summary TSV
    summary = out_dir / "filter_summary.tsv"
    assert summary.exists()
    rows = list(csv.DictReader(summary.open(encoding="utf-8"), delimiter="\t"))
    assert len(rows) == 2
    og0 = next(r for r in rows if r["orthogroup"] == "OG0000000")
    assert og0["single_copy"] == "4"
    assert og0["multi_copy"] == "0"
    assert og0["missing"] == "0"


def test_filter_auto_finds_latest_run(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    species = ["SpA", "SpB", "SpC", "SpD"]
    orthogroups = {"OG0000000": [1, 1, 1, 1]}
    _seed_orthofinder_run(paths, "of_auto", species, orthogroups)

    result = runner.invoke(
        app,
        ["filter-orthogroups", str(project_dir)],
    )
    assert result.exit_code == 0, result.output
    assert "Using latest OrthoFinder run: of_auto" in result.output
    assert "Selected orthogroups: 1" in result.output


def test_filter_with_explicit_results_dir(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    species = ["SpA", "SpB"]
    orthogroups = {"OG0000000": [1, 1]}
    _seed_orthofinder_run(paths, "of_ext", species, orthogroups)

    results_root = paths.run_dir("of_ext") / "orthofinder_results"

    result = runner.invoke(
        app,
        [
            "filter-orthogroups",
            "--results-dir", str(results_root),
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Selected orthogroups: 1" in result.output


def test_filter_no_ogs_pass(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    species = ["SpA", "SpB", "SpC", "SpD"]
    orthogroups = {
        "OG0000000": [2, 2, 2, 2],  # all multi-copy
        "OG0000001": [0, 0, 1, 1],  # 50% single-copy, below 75%
    }
    _seed_orthofinder_run(paths, "of_none", species, orthogroups)

    result = runner.invoke(
        app,
        ["filter-orthogroups", "--run-id", "of_none", str(project_dir)],
    )
    assert result.exit_code == 0, result.output
    assert "No orthogroups passed the filter" in result.output


def test_filter_custom_output_dir(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    species = ["SpA", "SpB", "SpC", "SpD"]
    orthogroups = {"OG0000000": [1, 1, 1, 1]}
    _seed_orthofinder_run(paths, "of_out", species, orthogroups)

    custom_out = tmp_path / "my_filtered"
    result = runner.invoke(
        app,
        [
            "filter-orthogroups",
            "--run-id", "of_out",
            "--output-dir", str(custom_out),
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (custom_out / "OG0000000.fa").exists()


def test_filter_strict_threshold(tmp_path: Path) -> None:
    """With min_single_copy=1.0, only fully single-copy OGs pass."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    species = ["SpA", "SpB", "SpC", "SpD"]
    orthogroups = {
        "OG0000000": [1, 1, 1, 1],  # 100% → pass
        "OG0000001": [1, 1, 1, 2],  # 75% → fail at 1.0
    }
    _seed_orthofinder_run(paths, "of_strict", species, orthogroups)

    result = runner.invoke(
        app,
        [
            "filter-orthogroups",
            "--run-id", "of_strict",
            "--min-single-copy", "1.0",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Selected orthogroups: 1" in result.output

    out_dir = paths.run_dir("of_strict") / "filtered_orthogroups"
    assert (out_dir / "OG0000000.fa").exists()
    assert not (out_dir / "OG0000001.fa").exists()
