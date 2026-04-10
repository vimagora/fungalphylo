"""protsetphylo tree-export — render gene trees as annotated PDF/SVG.

Reads IQ-TREE gene tree files from a phylo run, numbers internal nodes,
and renders a multi-page PDF with optional taxonomy color bars, group
annotations, and characterized-gene landmarks.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Optional

import toyplot
import toyplot.pdf
import toyplot.svg
import toytree
import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db

# ---------------------------------------------------------------------------
# Palette for taxonomy / group color bars
# ---------------------------------------------------------------------------
_PALETTE = [
    "#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
    "#911eb4", "#42d4f4", "#f032e6", "#bfef45", "#fabed4",
    "#469990", "#dcbeff", "#9A6324", "#fffac8", "#800000",
    "#aaffc3", "#808000", "#ffd8b1", "#000075", "#a9a9a9",
]


def _color_map(values: list[str]) -> dict[str, str]:
    """Map unique non-empty string values to palette colors."""
    unique = sorted({v for v in values if v})
    return {v: _PALETTE[i % len(_PALETTE)] for i, v in enumerate(unique)}


# ---------------------------------------------------------------------------
# Tree helpers
# ---------------------------------------------------------------------------

def _find_gene_trees_dir(paths: ProjectPaths, run_id: str | None) -> tuple[Path, str]:
    """Locate gene trees from a phylo run. Auto-detects latest if run_id is None."""
    if run_id is not None:
        gt_dir = paths.run_dir(run_id) / "gene_trees"
        if not gt_dir.is_dir():
            raise typer.BadParameter(f"No gene_trees directory in run {run_id}")
        return gt_dir, run_id

    # Auto-detect latest family_phylo run
    candidates = []
    for manifest_path in paths.runs_root.glob("*/manifest.json"):
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            if data.get("kind") == "family_phylo":
                candidates.append((data.get("created_at", ""), data["run_id"]))
        except (json.JSONDecodeError, KeyError):
            continue
    if not candidates:
        raise typer.BadParameter("No family_phylo runs found. Provide --run-id.")
    candidates.sort(reverse=True)
    resolved = candidates[0][1]
    typer.echo(f"Using phylo run: {resolved}")
    return paths.run_dir(resolved) / "gene_trees", resolved


def _load_tree(newick_path: Path) -> toytree.ToyTree:
    """Load a newick file, stripping empty lines."""
    text = newick_path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Empty tree file: {newick_path}")
    return toytree.tree(text)


def _number_internal_nodes(tree: toytree.ToyTree) -> toytree.ToyTree:
    """Append /N<idx> to internal node names (preserving UFBoot/SH-aLRT).

    IQ-TREE produces internal node names like '95/88' (UFBoot/SH-aLRT).
    This function appends the node index: '95/88/N4'.
    """
    for node in tree.traverse():
        if not node.is_leaf():
            existing = node.name or ""
            if existing:
                node.name = f"{existing}/N{node.idx}"
            else:
                node.name = f"N{node.idx}"
    return tree


def _write_numbered_newick(tree: toytree.ToyTree, out_path: Path) -> None:
    """Write tree with numbered internal nodes to newick file."""
    out_path.write_text(tree.write(internal_labels="name") + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Annotation loading
# ---------------------------------------------------------------------------

def _load_taxonomy(paths: ProjectPaths, family_id: str) -> dict[str, dict[str, str]]:
    """Load taxonomy TSV for a family. Returns {short_name: {rank: value}}."""
    tax_path = paths.family_config_dir(family_id) / "taxonomy.tsv"
    if not tax_path.exists():
        return {}
    result: dict[str, dict[str, str]] = {}
    with tax_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            sn = row.get("short_name", "").strip()
            if sn:
                result[sn] = {k: v.strip() for k, v in row.items() if k != "short_name"}
    return result


def _load_characterized(paths: ProjectPaths, family_id: str) -> dict[str, dict]:
    """Load characterized.tsv. Returns {protein_header: row_dict}.

    Protein headers in the tree are like 'PortalA|proteinID' or 'short_name|proteinID'.
    The characterized.tsv maps short_name to the species info and group columns.
    """
    char_path = paths.family_characterized_dir(family_id) / "characterized.tsv"
    if not char_path.exists():
        return {}
    result: dict[str, dict] = {}
    with char_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            sn = row.get("short_name", "").strip()
            if sn:
                result[sn] = dict(row)
    return result


def _tip_to_species(tip_label: str, delimiter: str = "|") -> str:
    """Extract species/short_name from a tree tip label like 'PortalA|proteinID'."""
    return tip_label.split(delimiter, 1)[0]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _render_tree_page(
    tree: toytree.ToyTree,
    og_name: str,
    taxonomy: dict[str, dict[str, str]],
    characterized: dict[str, dict],
    tax_levels: list[str],
    delimiter: str,
    width: int,
    height_per_tip: int,
) -> toyplot.canvas.Canvas:
    """Render a single gene tree with annotations."""
    ntips = tree.ntips
    height = max(300, ntips * height_per_tip)

    # Extra width for color bars
    n_bars = len(tax_levels)
    # Count group columns
    group_cols = _get_group_columns(characterized)
    n_bars += len(group_cols)
    bar_width = 20
    extra_width = n_bars * (bar_width + 5) + 60  # padding
    total_width = width + extra_width

    tip_labels = tree.get_tip_labels()

    # Build node labels: internal nodes get their numbered name, tips stay empty
    node_labels = []
    for node in tree.traverse():
        if node.is_leaf():
            node_labels.append("")
        else:
            node_labels.append(node.name or "")

    # Characterized gene landmarks: mark tip with a star
    char_tips = set()
    for tip in tip_labels:
        sp = _tip_to_species(tip, delimiter)
        if sp in characterized:
            char_tips.add(tip)

    # Tip label colors: characterized genes in red
    tip_colors = []
    for tip in tip_labels:
        if tip in char_tips:
            tip_colors.append("#e6194b")
        else:
            tip_colors.append("#333333")

    canvas, axes, mark = tree.draw(
        width=total_width,
        height=height,
        tip_labels_align=True,
        tip_labels_style={
            "font-size": "9px",
            "-toyplot-anchor-shift": f"{extra_width}px",
        },
        tip_labels_colors=tip_colors,
        node_labels=node_labels,
        node_labels_style={"font-size": "7px", "fill": "#666"},
        node_sizes=0,
    )

    # Add OG name as title text on the canvas
    canvas.text(
        total_width / 2, 12, og_name,
        style={"font-size": "12px", "font-weight": "bold", "text-anchor": "middle"},
    )

    # Add color bars to the right of tips
    # Use tree height to position bars just past tip labels
    tree_height = tree.treenode.height or 1.0
    bar_x_start = tree_height * 1.08
    bar_x_step = tree_height * 0.05

    col_idx = 0

    # Taxonomy color bars
    for rank in tax_levels:
        values = []
        for tip in tip_labels:
            sp = _tip_to_species(tip, delimiter)
            tax = taxonomy.get(sp, {})
            values.append(tax.get(rank, ""))

        cmap = _color_map(values)
        x_left = bar_x_start + col_idx * bar_x_step
        x_right = x_left + bar_x_step * 0.7

        for i, val in enumerate(values):
            color = cmap.get(val, "#f0f0f0")
            axes.rectangle(
                x_left, x_right,
                i - 0.4, i + 0.4,
                style={"fill": color, "stroke": "none"},
            )

        # Label at top
        axes.text(
            (x_left + x_right) / 2, ntips - 0.2,
            rank[:3].upper(),
            style={"font-size": "7px", "text-anchor": "middle", "fill": "#666"},
        )
        col_idx += 1

    # Group columns
    for gcol, is_multi in group_cols:
        display_name = gcol.replace("group_", "")
        if is_multi:
            # Multi-value heatmap: intensity by count of semicolons
            values = []
            for tip in tip_labels:
                sp = _tip_to_species(tip, delimiter)
                char = characterized.get(sp, {})
                raw = char.get(gcol, "").strip()
                if raw:
                    values.append(len(raw.split(";")))
                else:
                    values.append(0)

            max_val = max(values) if values and max(values) > 0 else 1
            x_left = bar_x_start + col_idx * bar_x_step
            x_right = x_left + bar_x_step * 0.7

            for i, count in enumerate(values):
                intensity = count / max_val if count > 0 else 0
                r = int(255 * (1 - intensity))
                color = f"rgb({r}, {r}, 255)"
                axes.rectangle(
                    x_left, x_right,
                    i - 0.4, i + 0.4,
                    style={"fill": color, "stroke": "none"},
                )

            axes.text(
                (x_left + x_right) / 2, ntips - 0.2,
                display_name[:4].upper(),
                style={"font-size": "7px", "text-anchor": "middle", "fill": "#666"},
            )
        else:
            # Single-value: color bar
            values = []
            for tip in tip_labels:
                sp = _tip_to_species(tip, delimiter)
                char = characterized.get(sp, {})
                values.append(char.get(gcol, "").strip())

            cmap = _color_map(values)
            x_left = bar_x_start + col_idx * bar_x_step
            x_right = x_left + bar_x_step * 0.7

            for i, val in enumerate(values):
                color = cmap.get(val, "#f0f0f0")
                axes.rectangle(
                    x_left, x_right,
                    i - 0.4, i + 0.4,
                    style={"fill": color, "stroke": "none"},
                )

            axes.text(
                (x_left + x_right) / 2, ntips - 0.2,
                display_name[:4].upper(),
                style={"font-size": "7px", "text-anchor": "middle", "fill": "#666"},
            )
        col_idx += 1

    return canvas


def _get_group_columns(characterized: dict[str, dict]) -> list[tuple[str, bool]]:
    """Identify group_* columns and whether they are multi-value (semicolons).

    Returns [(col_name, is_multi), ...].
    """
    if not characterized:
        return []
    sample = next(iter(characterized.values()))
    group_cols = sorted(k for k in sample if k.startswith("group_"))
    result = []
    for gcol in group_cols:
        has_semi = any(";" in row.get(gcol, "") for row in characterized.values())
        result.append((gcol, has_semi))
    return result


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _write_individual_pdfs(
    canvases: list[tuple[str, toyplot.canvas.Canvas]], out_path: Path
) -> None:
    """Write each canvas as a separate PDF file in a directory."""
    out_dir = out_path.parent / out_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)
    for og_name, tp_canvas in canvases:
        pdf_path = out_dir / f"{og_name}.pdf"
        with pdf_path.open("wb") as f:
            toyplot.pdf.render(tp_canvas, f)

    # Also write SVGs
    svg_dir = out_dir / "svg"
    svg_dir.mkdir(parents=True, exist_ok=True)
    for og_name, tp_canvas in canvases:
        svg_path = svg_dir / f"{og_name}.svg"
        with svg_path.open("wb") as f:
            toyplot.svg.render(tp_canvas, f)


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------

def tree_export_command(
    project_dir: Path = typer.Argument(..., help="Project directory."),
    family_id: str = typer.Option(..., "--family-id", help="Gene family identifier."),
    run_id: str | None = typer.Option(
        None, "--run-id", help="Phylo run ID (default: auto-detect latest family_phylo).",
    ),
    tax_level: Optional[list[str]] = typer.Option(
        None, "--tax-level",
        help="Taxonomy rank to display as color bar (repeatable: --tax-level order --tax-level family).",
    ),
    delimiter: str = typer.Option("|", "--delimiter", help="Tip label delimiter (default: |)."),
    width: int = typer.Option(800, "--width", help="Base tree width in pixels."),
    height_per_tip: int = typer.Option(18, "--height-per-tip", help="Pixels per tip for height."),
    out_dir: Path | None = typer.Option(
        None, "--out-dir", help="Output directory (default: families/<id>/tree_export/).",
    ),
) -> None:
    """Render gene trees as annotated PDF/SVG with node numbers, taxonomy, and groups."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    tax_levels = tax_level or []

    # Find gene trees
    gene_trees_dir, resolved_run_id = _find_gene_trees_dir(paths, run_id)

    # Collect tree files
    tree_files = sorted(gene_trees_dir.glob("*/*.treefile"))
    if not tree_files:
        raise typer.BadParameter(f"No .treefile files found in {gene_trees_dir}")

    # Load annotations
    taxonomy = _load_taxonomy(paths, family_id) if tax_levels else {}
    characterized = _load_characterized(paths, family_id)

    # Output directory
    if out_dir is None:
        out_dir = paths.family_dir(family_id) / "tree_export"
    else:
        out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    numbered_dir = out_dir / "numbered_newick"
    numbered_dir.mkdir(parents=True, exist_ok=True)

    canvases: list[tuple[str, toyplot.canvas.Canvas]] = []
    n_rendered = 0
    n_skipped = 0

    for tf in tree_files:
        og_name = tf.parent.name
        try:
            tree = _load_tree(tf)
        except (ValueError, Exception) as e:
            typer.echo(f"  SKIP {og_name}: {e}")
            n_skipped += 1
            continue

        # Number internal nodes
        tree = _number_internal_nodes(tree)

        # Write numbered newick
        _write_numbered_newick(tree, numbered_dir / f"{og_name}.nwk")

        # Render
        canvas = _render_tree_page(
            tree=tree,
            og_name=og_name,
            taxonomy=taxonomy,
            characterized=characterized,
            tax_levels=tax_levels,
            delimiter=delimiter,
            width=width,
            height_per_tip=height_per_tip,
        )
        canvases.append((og_name, canvas))
        n_rendered += 1

    if not canvases:
        raise typer.BadParameter("No trees could be rendered.")

    # Write PDFs and SVGs
    _write_individual_pdfs(canvases, out_dir / "trees.pdf")

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_tree_export",
            "family_id": family_id,
            "run_id": resolved_run_id,
            "n_rendered": n_rendered,
            "n_skipped": n_skipped,
            "tax_levels": tax_levels,
            "out_dir": str(out_dir),
        },
    )
    typer.echo(f"Rendered {n_rendered} trees ({n_skipped} skipped)")
    typer.echo(f"  PDFs:             {out_dir / 'trees'}/")
    typer.echo(f"  SVGs:             {out_dir / 'trees' / 'svg'}/")
    typer.echo(f"  Numbered Newick:  {numbered_dir}/")
    if tax_levels:
        typer.echo(f"  Tax levels:       {', '.join(tax_levels)}")
