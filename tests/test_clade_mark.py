from __future__ import annotations

import csv
from pathlib import Path

import dendropy
from typer.testing import CliRunner

from fungalphylo.cli.commands.protsetphylo.clade_mark import (
    _build_count_matrix,
    _descendant_tip_labels,
    _find_internal_node,
    _node_index,
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


def _numbered_tree(newick: str) -> dendropy.Tree:
    t = dendropy.Tree.get(data=newick, schema="newick")
    _number_internal_nodes(t)
    return t


# --- Unit tests ---


def test_node_index_parsing() -> None:
    assert _node_index("95/88/N4") == 4
    assert _node_index("N6") == 6
    assert _node_index("") is None
    assert _node_index(None) is None
    assert _node_index("leaf_name") is None
    assert _node_index("100/N12") == 12


def test_find_internal_node_and_descendants() -> None:
    # Preorder internal numbering for ((A,B),(C,D)); — N0=root, N1=(A,B), N2=(C,D)
    t = _numbered_tree("((A:0.1,B:0.2):0.3,(C:0.3,D:0.4):0.5);")
    n1 = _find_internal_node(t, 1)
    assert n1 is not None
    assert sorted(_descendant_tip_labels(n1)) == ["A", "B"]

    n2 = _find_internal_node(t, 2)
    assert sorted(_descendant_tip_labels(n2)) == ["C", "D"]

    root = _find_internal_node(t, 0)
    assert sorted(_descendant_tip_labels(root)) == ["A", "B", "C", "D"]

    assert _find_internal_node(t, 99) is None


def test_build_count_matrix() -> None:
    per_og = {
        "OG1": {
            "clade_A": ["SpA|p1", "SpB|p2", "SpA|p3"],
            "clade_B": ["SpC|p4", "SpD|p5"],
        },
    }
    species, clades, matrix = _build_count_matrix(per_og, "|")
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
        t = _numbered_tree(newick)
        _write_numbered_newick(t, numbered_dir / f"{og_name}.nwk")


def test_clade_mark_full_workflow(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _setup_tree_export(paths, "test_fam", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    # Preorder internal: N0=root, N1=(SpA,SpB), N2=(SpC,SpD)
    clade_tsv = tmp_path / "clades.tsv"
    clade_tsv.write_text(
        "clade_name\tog_id\tnode_number\n"
        "transporters\tOG0001\t1\n"
        "enzymes\tOG0001\t2\n",
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
    assert "Species:            4" in result.output

    out_dir = paths.family_dir("test_fam") / "clade_mark"

    matrix_path = out_dir / "clade_count_matrix.tsv"
    assert matrix_path.exists()
    with matrix_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert len(rows) == 4
    sp_a = next(r for r in rows if r["species"] == "SpA")
    assert sp_a["transporters"] == "1"
    assert sp_a["enzymes"] == "0"

    # Per-OG iTOL color strip
    og_cs = out_dir / "itol" / "OG0001" / "dataset_clades.txt"
    assert og_cs.exists()
    cs_text = og_cs.read_text(encoding="utf-8")
    assert "DATASET_COLORSTRIP" in cs_text
    assert "transporters" in cs_text
    assert "enzymes" in cs_text

    # Species-tree heatmap
    sp_hm = out_dir / "itol_species_clade_heatmap.txt"
    assert sp_hm.exists()
    hm_text = sp_hm.read_text(encoding="utf-8")
    assert "DATASET_HEATMAP" in hm_text
    assert "transporters" in hm_text


def test_clade_mark_missing_tree_export_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    clade_tsv = tmp_path / "clades.tsv"
    clade_tsv.write_text(
        "clade_name\tog_id\tnode_number\n"
        "test\tOG0001\t1\n",
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

    bad_tsv = tmp_path / "bad.tsv"
    bad_tsv.write_text("name\tnode\ntest\t1\n", encoding="utf-8")

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
