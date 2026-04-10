"""protsetphylo clade-mark — annotate clades on gene trees and build count matrix.

Reads a user-provided TSV (clade_name, og_id, node_number), identifies
descendant tips for each clade, builds a species × clade gene count matrix,
re-renders trees with clade highlights, and generates iTOL heatmap annotation.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import dendropy
import toyplot
import toyplot.pdf
import toyplot.svg
import toytree
import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db

# Reuse helpers from tree_export
from fungalphylo.cli.commands.protsetphylo.tree_export import (
    _color_map,
    _load_characterized,
    _load_taxonomy,
    _tip_to_species,
    _PALETTE,
)


# ---------------------------------------------------------------------------
# Clade resolution
# ---------------------------------------------------------------------------

def _parse_node_number(node_name: str) -> int | None:
    """Extract node number from a name like '95/88/N4' or 'N6'."""
    m = re.search(r"N(\d+)$", node_name or "")
    return int(m.group(1)) if m else None


def _find_node_by_number(tree: toytree.ToyTree, node_number: int) -> object | None:
    """Find the node with the given idx in the tree."""
    for node in tree.traverse():
        if node.idx == node_number:
            return node
    return None


def _descendant_tips(node) -> list[str]:
    """Get all tip labels descending from a node."""
    tips = []
    for leaf in node.iter_leaves():
        tips.append(leaf.name)
    return tips


def _load_clade_definitions(tsv_path: Path) -> list[dict[str, str]]:
    """Load clade definition TSV. Expects columns: clade_name, og_id, node_number."""
    with tsv_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if reader.fieldnames is None:
            raise typer.BadParameter(f"Empty clade definitions file: {tsv_path}")
        required = {"clade_name", "og_id", "node_number"}
        missing = required - set(reader.fieldnames)
        if missing:
            raise typer.BadParameter(
                f"Missing columns in clade TSV: {', '.join(sorted(missing))}. "
                f"Found: {', '.join(reader.fieldnames)}"
            )
        rows = list(reader)
    if not rows:
        raise typer.BadParameter(f"No data rows in clade TSV: {tsv_path}")
    return rows


# ---------------------------------------------------------------------------
# Count matrix
# ---------------------------------------------------------------------------

def _build_count_matrix(
    clade_tips: dict[str, list[str]],
    delimiter: str,
) -> tuple[list[str], list[str], dict[str, dict[str, int]]]:
    """Build species × clade gene count matrix.

    Returns (species_list, clade_list, matrix) where matrix[species][clade] = count.
    """
    all_species: set[str] = set()
    for tips in clade_tips.values():
        for tip in tips:
            all_species.add(_tip_to_species(tip, delimiter))

    species_list = sorted(all_species)
    clade_list = sorted(clade_tips.keys())
    matrix: dict[str, dict[str, int]] = {sp: {cl: 0 for cl in clade_list} for sp in species_list}

    for clade, tips in clade_tips.items():
        for tip in tips:
            sp = _tip_to_species(tip, delimiter)
            matrix[sp][clade] += 1

    return species_list, clade_list, matrix


def _write_count_matrix(
    species_list: list[str],
    clade_list: list[str],
    matrix: dict[str, dict[str, int]],
    out_path: Path,
) -> None:
    """Write species × clade count matrix as TSV."""
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["species"] + clade_list)
        for sp in species_list:
            w.writerow([sp] + [str(matrix[sp][cl]) for cl in clade_list])


# ---------------------------------------------------------------------------
# iTOL heatmap annotation
# ---------------------------------------------------------------------------

def _write_itol_heatmap(
    species_list: list[str],
    clade_list: list[str],
    matrix: dict[str, dict[str, int]],
    out_path: Path,
) -> None:
    """Write iTOL DATASET_HEATMAP annotation file."""
    # Assign colors to clades
    clade_colors = {cl: _PALETTE[i % len(_PALETTE)] for i, cl in enumerate(clade_list)}

    with out_path.open("w", encoding="utf-8") as f:
        f.write("DATASET_HEATMAP\n")
        f.write("SEPARATOR TAB\n")
        f.write("DATASET_LABEL\tClade Gene Counts\n")
        f.write("COLOR\t#333333\n")
        f.write(f"FIELD_LABELS\t{chr(9).join(clade_list)}\n")
        f.write(f"FIELD_COLORS\t{chr(9).join(clade_colors[cl] for cl in clade_list)}\n")
        f.write("COLOR_MIN\t#ffffff\n")
        f.write("COLOR_MAX\t#e6194b\n")
        f.write("DATA\n")
        for sp in species_list:
            vals = [str(matrix[sp][cl]) for cl in clade_list]
            f.write(f"{sp}\t{chr(9).join(vals)}\n")


# ---------------------------------------------------------------------------
# Re-render trees with clade highlights
# ---------------------------------------------------------------------------

def _render_tree_with_clades(
    tree: toytree.ToyTree,
    og_name: str,
    clade_nodes: dict[str, int],
    delimiter: str,
    width: int,
    height_per_tip: int,
) -> toyplot.canvas.Canvas:
    """Render a tree with highlighted clade subtrees."""
    ntips = tree.ntips
    height = max(300, ntips * height_per_tip)

    tip_labels = tree.get_tip_labels()

    # Map each tip to its clade (if any)
    tip_clade: dict[str, str] = {}
    clade_colors = {}
    for i, (clade_name, node_num) in enumerate(clade_nodes.items()):
        color = _PALETTE[i % len(_PALETTE)]
        clade_colors[clade_name] = color
        node = _find_node_by_number(tree, node_num)
        if node is None:
            continue
        for tip in _descendant_tips(node):
            tip_clade[tip] = clade_name

    # Color tips by clade
    tip_colors = []
    for tip in tip_labels:
        clade = tip_clade.get(tip)
        if clade:
            tip_colors.append(clade_colors[clade])
        else:
            tip_colors.append("#333333")

    # Node labels (keep numbered names on internals)
    node_labels = []
    for node in tree.traverse():
        if node.is_leaf():
            node_labels.append("")
        else:
            node_labels.append(node.name or "")

    canvas, axes, mark = tree.draw(
        width=width,
        height=height,
        tip_labels_align=True,
        tip_labels_style={"font-size": "9px"},
        tip_labels_colors=tip_colors,
        node_labels=node_labels,
        node_labels_style={"font-size": "7px", "fill": "#666"},
        node_sizes=0,
    )

    # Add OG name as title
    canvas.text(
        width / 2, 12, og_name,
        style={"font-size": "12px", "font-weight": "bold", "text-anchor": "middle"},
    )

    # Clade legend as text labels at bottom
    legend_parts = [f"{name}" for name in clade_colors]
    if legend_parts:
        canvas.text(
            width / 2, height - 8,
            "  |  ".join(legend_parts),
            style={"font-size": "9px", "text-anchor": "middle", "fill": "#666"},
        )

    return canvas


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------

def clade_mark_command(
    project_dir: Path = typer.Argument(..., help="Project directory."),
    family_id: str = typer.Option(..., "--family-id", help="Gene family identifier."),
    clade_tsv: Path = typer.Option(
        ..., "--clade-tsv",
        help="TSV with clade_name, og_id, node_number columns.",
    ),
    delimiter: str = typer.Option("|", "--delimiter", help="Tip label delimiter."),
    width: int = typer.Option(800, "--width", help="Tree width in pixels."),
    height_per_tip: int = typer.Option(18, "--height-per-tip", help="Pixels per tip."),
    out_dir: Path | None = typer.Option(
        None, "--out-dir", help="Output directory (default: families/<id>/clade_mark/).",
    ),
) -> None:
    """Mark clades on gene trees and build species × clade count matrix."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    # Load clade definitions
    clade_tsv = clade_tsv.expanduser().resolve()
    clade_defs = _load_clade_definitions(clade_tsv)

    # Output directory
    if out_dir is None:
        out_dir = paths.family_dir(family_id) / "clade_mark"
    else:
        out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Find numbered newick files from tree-export
    tree_export_dir = paths.family_dir(family_id) / "tree_export" / "numbered_newick"
    if not tree_export_dir.is_dir():
        raise typer.BadParameter(
            f"No numbered newick directory found: {tree_export_dir}\n"
            "Run `protsetphylo tree-export` first."
        )

    # Group clade definitions by OG
    og_clades: dict[str, dict[str, int]] = {}
    for row in clade_defs:
        og_id = row["og_id"].strip()
        clade_name = row["clade_name"].strip()
        try:
            node_num = int(row["node_number"].strip())
        except ValueError as e:
            raise typer.BadParameter(
                f"Invalid node_number '{row['node_number']}' for clade '{clade_name}'"
            ) from e
        og_clades.setdefault(og_id, {})[clade_name] = node_num

    # Process each OG
    all_clade_tips: dict[str, list[str]] = {}
    n_rendered = 0
    n_skipped = 0

    trees_dir = out_dir / "trees"
    trees_dir.mkdir(parents=True, exist_ok=True)
    svg_dir = trees_dir / "svg"
    svg_dir.mkdir(parents=True, exist_ok=True)

    for og_id, clades in sorted(og_clades.items()):
        nwk_path = tree_export_dir / f"{og_id}.nwk"
        if not nwk_path.exists():
            typer.echo(f"  SKIP {og_id}: numbered newick not found")
            n_skipped += 1
            continue

        try:
            tree = toytree.tree(nwk_path.read_text(encoding="utf-8").strip())
        except Exception as e:
            typer.echo(f"  SKIP {og_id}: {e}")
            n_skipped += 1
            continue

        # Collect tips for each clade
        for clade_name, node_num in clades.items():
            node = _find_node_by_number(tree, node_num)
            if node is None:
                typer.echo(f"  WARNING: node N{node_num} not found in {og_id}")
                continue
            tips = _descendant_tips(node)
            key = clade_name
            all_clade_tips.setdefault(key, []).extend(tips)

        # Render tree with highlights
        canvas = _render_tree_with_clades(
            tree, og_id, clades, delimiter, width, height_per_tip,
        )
        with (trees_dir / f"{og_id}.pdf").open("wb") as f:
            toyplot.pdf.render(canvas, f)
        with (svg_dir / f"{og_id}.svg").open("wb") as f:
            toyplot.svg.render(canvas, f)
        n_rendered += 1

    if not all_clade_tips:
        raise typer.BadParameter("No clade tips could be resolved from the provided definitions.")

    # Build count matrix
    species_list, clade_list, matrix = _build_count_matrix(all_clade_tips, delimiter)
    matrix_path = out_dir / "clade_count_matrix.tsv"
    _write_count_matrix(species_list, clade_list, matrix, matrix_path)

    # Write iTOL heatmap
    itol_path = out_dir / "itol_clade_heatmap.txt"
    _write_itol_heatmap(species_list, clade_list, matrix, itol_path)

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_clade_mark",
            "family_id": family_id,
            "clade_tsv": str(clade_tsv),
            "n_clades": len(all_clade_tips),
            "n_species": len(species_list),
            "n_rendered": n_rendered,
            "n_skipped": n_skipped,
            "out_dir": str(out_dir),
        },
    )

    typer.echo(f"Marked {len(all_clade_tips)} clades across {n_rendered} trees ({n_skipped} skipped)")
    typer.echo(f"  Count matrix:   {matrix_path}")
    typer.echo(f"  iTOL heatmap:   {itol_path}")
    typer.echo(f"  Highlighted:    {trees_dir}/")
    typer.echo(f"  Species:        {len(species_list)}")
