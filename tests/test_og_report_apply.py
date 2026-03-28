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


def _seed_characterized(paths: ProjectPaths, family_id: str) -> None:
    """Create a minimal characterized.tsv for testing."""
    char_dir = paths.family_characterized_dir(family_id)
    char_dir.mkdir(parents=True)
    tsv_path = char_dir / "characterized.tsv"
    tsv_path.write_text(
        "portal_id\tspecies\tshort_name\tprotein_name\tsequence\n"
        "SpA\tSpecies A\tSpA\tSUT1\tMPEPTIDE\n"
        "SpB\tSpecies B\tSpB\tSUT2\tMPEPTIDE\n"
        "\tOutgroup\tOutSp\tSUT3\tMPEPTIDE\n",
        encoding="utf-8",
    )


def _seed_orthofinder_run(
    paths: ProjectPaths,
    run_id: str,
    portals: list[str],
    orthogroups: dict[str, dict[str, list[str]]],
) -> Path:
    """Create a fake OrthoFinder run with Orthogroups.tsv and Orthogroup_Sequences/."""
    run_dir = paths.run_dir(run_id)
    of_results = run_dir / "orthofinder_results" / "Results_Mar26"

    # Manifest
    manifest = {
        "run_id": run_id,
        "kind": "orthofinder",
        "created_at": "2026-03-26T00:00:00+00:00",
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    # Orthogroups.tsv
    og_dir = of_results / "Orthogroups"
    og_dir.mkdir(parents=True)
    og_tsv = og_dir / "Orthogroups.tsv"
    with og_tsv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(["Orthogroup"] + portals)
        for og_id, portal_genes in orthogroups.items():
            row = [og_id]
            for portal in portals:
                genes = portal_genes.get(portal, [])
                row.append(", ".join(genes))
            writer.writerow(row)

    # Orthogroup_Sequences/
    seq_dir = of_results / "Orthogroup_Sequences"
    seq_dir.mkdir(parents=True)
    for og_id, portal_genes in orthogroups.items():
        seqs = []
        for genes in portal_genes.values():
            for gene in genes:
                seqs.append(f">{gene}\nMPEPTIDE\n")
        (seq_dir / f"{og_id}.fa").write_text("".join(seqs), encoding="utf-8")

    return of_results


# ---- og-report tests ----


def test_og_report_generates_reports(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_characterized(paths, "mfs_sugar")

    portals = ["SpA.faa", "SpB.faa", "OutSp.faa"]
    orthogroups = {
        "OG0000001": {
            "SpA.faa": ["SpA|SUT1", "SpA|extra"],
            "SpB.faa": ["SpB|SUT2"],
            "OutSp.faa": ["OutSp|SUT3"],
        },
        "OG0000002": {
            "SpA.faa": ["SpA|other1"],
            "SpB.faa": ["SpB|other2"],
            "OutSp.faa": [],
        },
        "OG0000003": {
            "SpA.faa": ["SpA|SUT1b"],
            "SpB.faa": [],
            "OutSp.faa": ["OutSp|SUT3b"],
        },
    }
    _seed_orthofinder_run(paths, "of_report", portals, orthogroups)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "og-report",
            "--family-id", "mfs_sugar",
            "--run-id", "of_report",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "OGs with characterized genes: 1" in result.output

    report_dir = paths.family_og_report_dir("mfs_sugar")
    assert (report_dir / "characterized_og_matrix.tsv").exists()
    assert (report_dir / "characterized_og_matrix.html").exists()
    assert (report_dir / "portal_og_matrix.tsv").exists()
    assert (report_dir / "portal_og_matrix.html").exists()
    assert (report_dir / "og_decisions.txt").exists()

    # Check characterized matrix
    with (report_dir / "characterized_og_matrix.tsv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    assert len(rows) == 1  # only OG0000001 has characterized genes
    assert rows[0]["orthogroup"] == "OG0000001"
    assert rows[0]["characterized_count"] == "3"

    # Check portal matrix
    with (report_dir / "portal_og_matrix.tsv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    assert len(rows) == 1
    assert rows[0]["SpA.faa"] == "2"  # SUT1 + extra
    assert rows[0]["SpB.faa"] == "1"

    # Check decision template
    template = (report_dir / "og_decisions.txt").read_text(encoding="utf-8")
    assert "OG0000001" in template
    assert "include:" in template
    assert "merge:" in template


def test_og_report_no_matches(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_characterized(paths, "test_fam")

    portals = ["SpX.faa"]
    orthogroups = {
        "OG0000001": {"SpX.faa": ["SpX|unrelated"]},
    }
    _seed_orthofinder_run(paths, "of_nomatch", portals, orthogroups)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "og-report",
            "--family-id", "test_fam",
            "--run-id", "of_nomatch",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0
    assert "No orthogroups contain characterized genes" in result.output


def test_og_report_html_has_styling(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_characterized(paths, "mfs_html")

    portals = ["SpA.faa", "SpB.faa"]
    orthogroups = {
        "OG0000001": {
            "SpA.faa": ["SpA|SUT1"],
            "SpB.faa": ["SpB|SUT2"],
        },
    }
    _seed_orthofinder_run(paths, "of_html", portals, orthogroups)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "og-report",
            "--family-id", "mfs_html",
            "--run-id", "of_html",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    html = (paths.family_og_report_dir("mfs_html") / "characterized_og_matrix.html").read_text(
        encoding="utf-8"
    )
    assert "<table>" in html
    assert "marked" in html  # x cells get marked class


# ---- og-apply tests ----


def test_og_apply_include_and_merge(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_characterized(paths, "mfs_apply")

    portals = ["SpA.faa"]
    orthogroups = {
        "OG0000001": {"SpA.faa": ["SpA|g1"]},
        "OG0000002": {"SpA.faa": ["SpA|g2"]},
        "OG0000003": {"SpA.faa": ["SpA|g3"]},
        "OG0000004": {"SpA.faa": ["SpA|g4"]},
    }
    _seed_orthofinder_run(paths, "of_apply", portals, orthogroups)

    # Write decision template
    report_dir = paths.family_og_report_dir("mfs_apply")
    report_dir.mkdir(parents=True)
    (report_dir / "og_decisions.txt").write_text(
        "include: OG0000001\n"
        "merge: OG0000002,OG0000003;OG0000004\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "og-apply",
            "--family-id", "mfs_apply",
            "--run-id", "of_apply",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Copied 1 OGs as-is" in result.output
    assert "Created 2 merged FASTAs" in result.output

    out_dir = paths.family_og_selected_dir("mfs_apply")
    assert (out_dir / "OG0000001.fa").exists()
    assert (out_dir / "merge_OG0000002.fa").exists()
    assert (out_dir / "merge_OG0000004.fa").exists()

    # Merged file should contain both OGs
    merged = (out_dir / "merge_OG0000002.fa").read_text(encoding="utf-8")
    assert ">SpA|g2" in merged
    assert ">SpA|g3" in merged

    # Single OG merge
    single_merge = (out_dir / "merge_OG0000004.fa").read_text(encoding="utf-8")
    assert ">SpA|g4" in single_merge

    assert "phylo-slurm" in result.output


def test_og_apply_empty_template(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    report_dir = paths.family_og_report_dir("empty_fam")
    report_dir.mkdir(parents=True)
    (report_dir / "og_decisions.txt").write_text(
        "# Nothing selected\ninclude:\nmerge:\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "og-apply",
            "--family-id", "empty_fam",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0
    assert "Nothing to do" in result.output


def test_og_apply_missing_og_warns(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_characterized(paths, "mfs_miss")

    portals = ["SpA.faa"]
    orthogroups = {"OG0000001": {"SpA.faa": ["SpA|g1"]}}
    _seed_orthofinder_run(paths, "of_miss", portals, orthogroups)

    report_dir = paths.family_og_report_dir("mfs_miss")
    report_dir.mkdir(parents=True)
    (report_dir / "og_decisions.txt").write_text(
        "include: OG0000001,OG9999999\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "og-apply",
            "--family-id", "mfs_miss",
            "--run-id", "of_miss",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "WARNING" in result.output
    assert "Copied 1 OGs" in result.output
