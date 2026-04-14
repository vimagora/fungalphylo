from __future__ import annotations

import json
from pathlib import Path

import dendropy
from typer.testing import CliRunner

from fungalphylo.cli.commands.protsetphylo.tree_export import (
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


def test_number_internal_nodes_preserves_support() -> None:
    t = dendropy.Tree.get(
        data="((A:0.1,B:0.2)95/88:0.3,(C:0.3,D:0.4)100/99:0.5);",
        schema="newick",
    )
    _number_internal_nodes(t)
    labels = [
        n.label for n in t.preorder_internal_node_iter() if n.label
    ]
    assert any("/N" in lbl for lbl in labels)
    assert any(lbl.startswith("95/88/N") for lbl in labels)


def test_number_internal_nodes_no_support() -> None:
    t = dendropy.Tree.get(
        data="((A:0.1,B:0.2):0.3,(C:0.3,D:0.4):0.5);",
        schema="newick",
    )
    _number_internal_nodes(t)
    for node in t.preorder_internal_node_iter():
        assert node.label and node.label.startswith("N")


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
    assert "Rendered iTOL datasets for 2 OGs" in result.output

    out_dir = paths.family_dir("test_fam") / "tree_export"
    assert (out_dir / "numbered_newick" / "OG0001.nwk").exists()
    assert (out_dir / "numbered_newick" / "OG0002.nwk").exists()

    nwk = (out_dir / "numbered_newick" / "OG0001.nwk").read_text(encoding="utf-8")
    assert "N0" in nwk

    # iTOL per-OG tree.nwk
    assert (out_dir / "itol" / "OG0001" / "tree.nwk").exists()
    assert (out_dir / "itol" / "OG0002" / "tree.nwk").exists()
    assert (out_dir / "iTOL_UPLOAD.md").exists()


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
    assert "order, family" in result.output

    itol_og = paths.family_dir("tax_fam") / "tree_export" / "itol" / "OG0001"
    assert (itol_og / "dataset_tax_order.txt").exists()
    assert (itol_og / "dataset_tax_family.txt").exists()
    order_text = (itol_og / "dataset_tax_order.txt").read_text(encoding="utf-8")
    assert "DATASET_COLORSTRIP" in order_text
    assert "Eurotiales" in order_text


def test_tree_export_with_characterized_and_groups(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_char", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    _seed_characterized(paths, "char_fam", [
        {
            "short_name": "SpA", "species": "Sp A", "portal_id": "SpA",
            "group_function": "transporter", "group_substrate": "glucose;xylose",
        },
        {
            "short_name": "SpC", "species": "Sp C", "portal_id": "",
            "group_function": "enzyme", "group_substrate": "hexoses",
        },
    ])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "char_fam",
            "--run-id", "phylo_char",
            "--color-bar", "group_function",
            "--heatmap", "group_substrate",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Rendered iTOL datasets for 1 OGs" in result.output

    itol_og = paths.family_dir("char_fam") / "tree_export" / "itol" / "OG0001"
    cb_path = itol_og / "dataset_colorbar_group_function.txt"
    hm_path = itol_og / "dataset_heatmap_group_substrate.txt"
    landmarks_path = itol_og / "dataset_landmarks.txt"
    assert cb_path.exists()
    assert hm_path.exists()
    assert landmarks_path.exists()

    cb_text = cb_path.read_text(encoding="utf-8")
    assert "DATASET_COLORSTRIP" in cb_text
    assert "transporter" in cb_text
    assert "enzyme" in cb_text

    hm_text = hm_path.read_text(encoding="utf-8")
    assert "DATASET_BINARY" in hm_text
    # atomic values should appear as field labels
    assert "glucose" in hm_text
    assert "xylose" in hm_text
    assert "hexoses" in hm_text


def test_tree_export_heatmap_unknown_group_errors(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_bad", {
        "OG0001": "((SpA|p1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })
    _seed_characterized(paths, "bad_fam", [
        {"short_name": "SpA", "species": "Sp A", "portal_id": "SpA", "group_function": "x"},
    ])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "bad_fam",
            "--run-id", "phylo_bad",
            "--heatmap", "group_missing",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "group_missing" in result.output


def test_tree_export_quotes_underscored_tip_labels(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_us", {
        "OG0001": "((foo_a|prot_1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "us_fam",
            "--run-id", "phylo_us",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    nwk = (
        paths.family_dir("us_fam") / "tree_export" / "itol" / "OG0001" / "tree.nwk"
    ).read_text(encoding="utf-8")
    assert "'foo_a|prot_1'" in nwk


def test_tree_export_taxonomy_for_portal_characterized(tmp_path: Path) -> None:
    """A portal-characterized gene enters the tree as ``{short_name}|...``.

    Since the new family taxonomy export keys portal-characterized rows by
    the characterized short_name (not the portal_id), a direct lookup
    resolves their lineage.
    """
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    _seed_phylo_run(paths, "phylo_pc", {
        "OG0001": "((foo_a|prot1:0.1,SpB|p2:0.2):0.3,(SpC|p3:0.3,SpD|p4:0.4):0.5);",
    })

    # foo_a is characterized AND linked to portal Aspnid1
    _seed_characterized(paths, "pc_fam", [
        {"short_name": "foo_a", "species": "Sp A", "portal_id": "Aspnid1", "group_function": "transporter"},
    ])

    # New taxonomy.tsv schema: characterized rows keyed by short_name,
    # portal_id column retained for traceability.
    _seed_taxonomy(paths, "pc_fam", [
        {"short_name": "foo_a", "species": "Sp A", "portal_id": "Aspnid1", "order": "Eurotiales", "family": "Aspergillaceae"},
        {"short_name": "SpB", "species": "Sp B", "portal_id": "SpB", "order": "Hypocreales", "family": "Nectriaceae"},
        {"short_name": "SpC", "species": "Sp C", "portal_id": "SpC", "order": "Hypocreales", "family": "Nectriaceae"},
        {"short_name": "SpD", "species": "Sp D", "portal_id": "SpD", "order": "Hypocreales", "family": "Nectriaceae"},
    ])

    result = runner.invoke(
        app,
        [
            "protsetphylo", "tree-export",
            "--family-id", "pc_fam",
            "--run-id", "phylo_pc",
            "--tax-level", "order",
            str(project_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    og_dir = paths.family_dir("pc_fam") / "tree_export" / "itol" / "OG0001"
    order_txt = (og_dir / "dataset_tax_order.txt").read_text(encoding="utf-8")
    # Annotation-file IDs are unquoted (iTOL matches the post-parse label).
    assert "foo_a|prot1\t" in order_txt
    # The portal-characterized tip must resolve taxonomy via Aspnid1's lineage.
    foo_line = next(line for line in order_txt.splitlines() if line.startswith("foo_a|prot1\t"))
    assert "Eurotiales" in foo_line

    # The newick itself must quote the underscored tip so iTOL preserves the "_".
    nwk = (og_dir / "tree.nwk").read_text(encoding="utf-8")
    assert "'foo_a|prot1'" in nwk


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
