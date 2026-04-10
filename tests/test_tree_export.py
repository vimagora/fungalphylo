from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.commands.protsetphylo.tree_export import (
    _load_characterized,
    _load_taxonomy,
    _number_internal_nodes,
    _tip_to_species,
)
from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_phylo_run(
    paths: ProjectPaths, run_id: str, og_trees: dict[str, str]
) -> Path:
    """Create a fake family_phylo run with gene tree files."""
    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "kind": "family_phylo",
        "created_at": "2026-04-07T00:00:00+00:00",
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    gene_trees_dir = run_dir / "gene_trees"
    for og_name, newick in og_trees.items():
        og_dir = gene_trees_dir / og_name
        og_dir.mkdir(parents=True, exist_ok=True)
        (og_dir / f"{og_name}.treefile").write_text(newick + "\n", encoding="utf-8")

    return gene_trees_dir


def _seed_characterized(paths: ProjectPaths, family_id: str, rows: list[dict]) -> None:
    char_dir = paths.family_characterized_dir(family_id)
    char_dir.mkdir(parents=True, exist_ok=True)
    tsv_path = char_dir / "characterized.tsv"
    header = list(rows[0].keys())
    lines = ["\t".join(header)]
    for r in rows:
        lines.append("\t".join(str(r.get(h, "")) for h in header))
    tsv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _seed_taxonomy(paths: ProjectPaths, family_id: str, rows: list[dict]) -> None:
    config_dir = paths.family_config_dir(family_id)
    config_dir.mkdir(parents=True, exist_ok=True)
    tsv_path = config_dir / "taxonomy.tsv"
    header = list(rows[0].keys())
    lines = ["\t".join(header)]
    for r in rows:
        lines.append("\t".join(str(r.get(h, "")) for h in header))
    tsv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# --- Unit tests ---


def test_tip_to_species() -> None:
    assert _tip_to_species("PortalA|prot123") == "PortalA"
    assert _tip_to_species("OutgroupX|seq1") == "OutgroupX"
    assert _tip_to_species("nopipe") == "nopipe"


def test_number_internal_nodes() -> None:
    import toytree

    t = toytree.tree("((A:0.1,B:0.2)95/88:0.3,(C:0.3,D:0.4)100/99:0.5);")
    t = _number_internal_nodes(t)
    internal_names = [n.name for n in t.traverse() if not n.is_leaf()]
    # Each internal node should have /N<idx> appended
    assert any("/N" in name for name in internal_names if name)
    # Root node (no existing label) should be just N<idx>
    root_name = t.treenode.name
    assert root_name.startswith("N")


def test_number_internal_nodes_no_support() -> None:
    import toytree

    t = toytree.tree("((A:0.1,B:0.2):0.3,(C:0.3,D:0.4):0.5);")
    t = _number_internal_nodes(t)
    internal_names = [n.name for n in t.traverse() if not n.is_leaf()]
    # All should be just N<idx>
    for name in internal_names:
        assert name.startswith("N"), f"Expected N-prefixed name, got {name!r}"


# --- Integration tests ---


def test_tree_export_renders_trees(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_test", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
        "OG0002": "((SpA|p5:0.1,SpC|p6:0.2):0.3,(SpB|p7:0.3,SpD|p8:0.4):0.5);",
    })

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "test_fam",
            "--run-id", "phylo_test",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Rendered 2 trees" in result.output

    out_dir = paths.family_dir("test_fam") / "tree_export"
    # Check numbered newick files
    assert (out_dir / "numbered_newick" / "OG0001.nwk").exists()
    assert (out_dir / "numbered_newick" / "OG0002.nwk").exists()

    # Check that numbered newick contains N-numbers
    nwk = (out_dir / "numbered_newick" / "OG0001.nwk").read_text(encoding="utf-8")
    assert "/N" in nwk or "N" in nwk

    # Check PDFs
    assert (out_dir / "trees" / "OG0001.pdf").exists()
    assert (out_dir / "trees" / "OG0002.pdf").exists()

    # Check SVGs
    assert (out_dir / "trees" / "svg" / "OG0001.svg").exists()


def test_tree_export_with_taxonomy(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_tax", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    _seed_taxonomy(paths, "tax_fam", [
        {"short_name": "SpA", "species": "Sp A", "order": "Eurotiales", "family": "Aspergillaceae"},
        {"short_name": "SpB", "species": "Sp B", "order": "Eurotiales", "family": "Aspergillaceae"},
        {"short_name": "SpC", "species": "Sp C", "order": "Hypocreales", "family": "Nectriaceae"},
        {"short_name": "SpD", "species": "Sp D", "order": "Hypocreales", "family": "Nectriaceae"},
    ])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "tax_fam",
            "--run-id", "phylo_tax",
            "--tax-level", "order",
            "--tax-level", "family",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Tax levels:       order, family" in result.output


def test_tree_export_with_characterized(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_char", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    _seed_characterized(paths, "char_fam", [
        {"short_name": "SpA", "species": "Sp A", "portal_id": "SpA", "group_function": "transporter"},
        {"short_name": "SpC", "species": "Sp C", "portal_id": "", "group_function": "enzyme"},
    ])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "char_fam",
            "--run-id", "phylo_char",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Rendered 1 trees" in result.output


def test_tree_export_auto_detects_latest_run(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_auto", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "auto_fam",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Using phylo run: phylo_auto" in result.output


def test_tree_export_no_trees_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "no_trees",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
