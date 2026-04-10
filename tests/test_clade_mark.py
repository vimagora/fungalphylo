from __future__ import annotations

import csv
from pathlib import Path

import toytree
from typer.testing import CliRunner

from fungalphylo.cli.commands.protsetphylo.clade_mark import (
    _build_count_matrix,
    _descendant_tips,
    _find_node_by_number,
    _parse_node_number,
)
from fungalphylo.cli.commands.protsetphylo.tree_export import (
    _number_internal_nodes,
    _write_numbered_newick,
)
from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths

runner = CliRunner()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


# --- Unit tests ---


def test_parse_node_number() -> None:
    assert _parse_node_number("95/88/N4") == 4
    assert _parse_node_number("N6") == 6
    assert _parse_node_number("") is None
    assert _parse_node_number("leaf_name") is None
    assert _parse_node_number("100/N12") == 12


def test_find_node_by_number() -> None:
    t = toytree.tree("((A:0.1,B:0.2):0.3,(C:0.3,D:0.4):0.5);")
    node = _find_node_by_number(t, 4)
    assert node is not None
    assert not node.is_leaf()

    # Tips have idx 0-3
    tip = _find_node_by_number(t, 0)
    assert tip is not None
    assert tip.is_leaf()

    # Non-existent
    assert _find_node_by_number(t, 99) is None


def test_descendant_tips() -> None:
    t = toytree.tree("((A:0.1,B:0.2):0.3,(C:0.3,D:0.4):0.5);")
    # Node 4 is the (A,B) internal node
    node = _find_node_by_number(t, 4)
    tips = _descendant_tips(node)
    assert sorted(tips) == ["A", "B"]

    # Root has all tips
    root = _find_node_by_number(t, t.treenode.idx)
    tips = _descendant_tips(root)
    assert sorted(tips) == ["A", "B", "C", "D"]


def test_build_count_matrix() -> None:
    clade_tips = {
        "clade_A": ["SpA|p1", "SpB|p2", "SpA|p3"],
        "clade_B": ["SpC|p4", "SpD|p5"],
    }
    species, clades, matrix = _build_count_matrix(clade_tips, "|")
    assert species == ["SpA", "SpB", "SpC", "SpD"]
    assert clades == ["clade_A", "clade_B"]
    assert matrix["SpA"]["clade_A"] == 2
    assert matrix["SpB"]["clade_A"] == 1
    assert matrix["SpC"]["clade_B"] == 1
    assert matrix["SpA"]["clade_B"] == 0


# --- Integration tests ---


def _setup_tree_export(paths: ProjectPaths, family_id: str, og_trees: dict[str, str]) -> None:
    """Create numbered newick files as tree-export would."""
    numbered_dir = paths.family_dir(family_id) / "tree_export" / "numbered_newick"
    numbered_dir.mkdir(parents=True, exist_ok=True)
    for og_name, newick in og_trees.items():
        t = toytree.tree(newick)
        t = _number_internal_nodes(t)
        _write_numbered_newick(t, numbered_dir / f"{og_name}.nwk")


def test_clade_mark_full_workflow(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    # Create numbered newick files
    _setup_tree_export(paths, "test_fam", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    # Read the numbered newick to find node numbers
    nwk = (paths.family_dir("test_fam") / "tree_export" / "numbered_newick" / "OG0001.nwk").read_text()
    # The tree has nodes: 0=SpA|p1, 1=SpB|p2, 2=SpC|p3, 3=SpD|p4, 4=(A,B), 5=(C,D), 6=root
    # Node 4 groups SpA and SpB, node 5 groups SpC and SpD

    # Write clade TSV
    clade_tsv = tmp_path / "clades.tsv"
    clade_tsv.write_text(
        "clade_name\tog_id\tnode_number\n"
        "transporters\tOG0001\t4\n"
        "enzymes\tOG0001\t5\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "clade-mark",
            "--family-id", "test_fam",
            "--clade-tsv", str(clade_tsv),
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Marked 2 clades" in result.output
    assert "Species:        4" in result.output

    out_dir = paths.family_dir("test_fam") / "clade_mark"

    # Count matrix
    matrix_path = out_dir / "clade_count_matrix.tsv"
    assert matrix_path.exists()
    with matrix_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
    assert len(rows) == 4
    sp_a_row = next(r for r in rows if r["species"] == "SpA")
    assert sp_a_row["transporters"] == "1"
    assert sp_a_row["enzymes"] == "0"

    # iTOL heatmap
    itol_path = out_dir / "itol_clade_heatmap.txt"
    assert itol_path.exists()
    text = itol_path.read_text(encoding="utf-8")
    assert "DATASET_HEATMAP" in text
    assert "transporters" in text
    assert "enzymes" in text

    # Highlighted tree PDFs
    assert (out_dir / "trees" / "OG0001.pdf").exists()
    assert (out_dir / "trees" / "svg" / "OG0001.svg").exists()


def test_clade_mark_missing_tree_export_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    clade_tsv = tmp_path / "clades.tsv"
    clade_tsv.write_text(
        "clade_name\tog_id\tnode_number\n"
        "test\tOG0001\t4\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "protsetphylo", "clade-mark",
            "--family-id", "no_export",
            "--clade-tsv", str(clade_tsv),
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "numbered newick" in result.output


def test_clade_mark_bad_tsv_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _setup_tree_export(paths, "test_fam", {
        "OG0001": "((A:0.1,B:0.2):0.3,(C:0.3,D:0.4):0.5);",
    })

    # Missing required columns
    bad_tsv = tmp_path / "bad.tsv"
    bad_tsv.write_text("name\tnode\ntest\t4\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "protsetphylo", "clade-mark",
            "--family-id", "test_fam",
            "--clade-tsv", str(bad_tsv),
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "Missing columns" in result.output
