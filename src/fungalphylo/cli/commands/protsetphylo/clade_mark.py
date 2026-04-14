"""protsetphylo clade-mark — mark clades on gene trees and build count matrix.

Reads a user-provided TSV (clade_name, og_id, node_number), identifies
descendant tips for each clade from the numbered newick trees produced by
``tree-export``, builds a species × clade gene count matrix, and emits iTOL
annotation files: one ``DATASET_COLORSTRIP`` per OG highlighting clade tips,
plus a species-tree ``DATASET_HEATMAP`` of the count matrix.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import dendropy
import typer

from fungalphylo.cli.commands.protsetphylo.tree_export import (
    _PALETTE,
    _tip_to_species,
)
from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db


# ---------------------------------------------------------------------------
# Clade resolution on numbered dendropy trees
# ---------------------------------------------------------------------------

_NODE_RE = re.compile(r"(?:^|/)N(\d+)$")


def _node_index(label: str | None) -> int | None:
    if not label:
        return None
    m = _NODE_RE.search(label.strip())
    return int(m.group(1)) if m else None


def _find_internal_node(tree: dendropy.Tree, node_number: int) -> dendropy.Node | None:
    for node in tree.preorder_internal_node_iter():
        if _node_index(node.label) == node_number:
            return node
    return None


def _descendant_tip_labels(node: dendropy.Node) -> list[str]:
    out: list[str] = []
    for leaf in node.leaf_iter():
        if leaf.taxon and leaf.taxon.label:
            out.append(leaf.taxon.label)
    return out


def _tree_tip_labels(tree: dendropy.Tree) -> list[str]:
    return [leaf.taxon.label for leaf in tree.leaf_node_iter() if leaf.taxon]


def _load_clade_definitions(tsv_path: Path) -> list[dict[str, str]]:
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
    per_og_clade_tips: dict[str, dict[str, list[str]]],
    delimiter: str,
) -> tuple[list[str], list[str], dict[str, dict[str, int]]]:
    """Build species × clade gene count matrix across all OGs.

    ``per_og_clade_tips[og][clade_name] = [tip, ...]``
    """
    all_species: set[str] = set()
    all_clades: set[str] = set()
    for clade_map in per_og_clade_tips.values():
        for clade, tips in clade_map.items():
            all_clades.add(clade)
            for tip in tips:
                all_species.add(_tip_to_species(tip, delimiter))

    species_list = sorted(all_species)
    clade_list = sorted(all_clades)
    matrix: dict[str, dict[str, int]] = {
        sp: {cl: 0 for cl in clade_list} for sp in species_list
    }
    for clade_map in per_og_clade_tips.values():
        for clade, tips in clade_map.items():
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
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["species"] + clade_list)
        for sp in species_list:
            w.writerow([sp] + [str(matrix[sp][cl]) for cl in clade_list])


# ---------------------------------------------------------------------------
# iTOL writers
# ---------------------------------------------------------------------------

def _write_og_clade_colorstrip(
    out_path: Path,
    og_name: str,
    tip_to_clade: dict[str, str],
    clade_colors: dict[str, str],
) -> None:
    """Per-OG DATASET_COLORSTRIP marking tips by the clade they belong to."""
    active_clades = sorted({c for c in tip_to_clade.values() if c})
    first_color = clade_colors.get(active_clades[0], "#aaaaaa") if active_clades else "#aaaaaa"
    lines = [
        "DATASET_COLORSTRIP",
        "SEPARATOR TAB",
        f"DATASET_LABEL\tClades: {og_name}",
        f"COLOR\t{first_color}",
        "STRIP_WIDTH\t25",
        "MARGIN\t2",
        "BORDER_WIDTH\t0",
        "COLOR_BRANCHES\t1",
        "LEGEND_TITLE\tClades",
    ]
    if active_clades:
        lines.append("LEGEND_SHAPES\t" + "\t".join("1" for _ in active_clades))
        lines.append(
            "LEGEND_COLORS\t" + "\t".join(clade_colors[c] for c in active_clades)
        )
        lines.append("LEGEND_LABELS\t" + "\t".join(active_clades))
    lines.append("DATA")
    for tip in sorted(tip_to_clade):
        clade = tip_to_clade[tip]
        if not clade:
            continue
        color = clade_colors.get(clade, "#cccccc")
        lines.append(f"{tip}\t{color}\t{clade}")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_species_heatmap(
    species_list: list[str],
    clade_list: list[str],
    matrix: dict[str, dict[str, int]],
    clade_colors: dict[str, str],
    out_path: Path,
) -> None:
    """Species-tree DATASET_HEATMAP for the clade count matrix."""
    lines = [
        "DATASET_HEATMAP",
        "SEPARATOR TAB",
        "DATASET_LABEL\tClade Gene Counts",
        "COLOR\t#333333",
        "FIELD_LABELS\t" + "\t".join(clade_list),
        "FIELD_COLORS\t" + "\t".join(clade_colors[c] for c in clade_list),
        "COLOR_MIN\t#ffffff",
        "COLOR_MAX\t#e6194b",
        "DATA",
    ]
    for sp in species_list:
        vals = [str(matrix[sp][cl]) for cl in clade_list]
        lines.append(f"{sp}\t" + "\t".join(vals))
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    out_dir: Path | None = typer.Option(
        None, "--out-dir", help="Output directory (default: families/<id>/clade_mark/).",
    ),
) -> None:
    """Mark clades on gene trees and build species × clade count matrix.

    Reads numbered newicks written by ``tree-export``, resolves each clade's
    descendant tips, and emits iTOL annotation files plus a TSV count matrix.
    """
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    clade_tsv = clade_tsv.expanduser().resolve()
    clade_defs = _load_clade_definitions(clade_tsv)

    if out_dir is None:
        out_dir = paths.family_dir(family_id) / "clade_mark"
    else:
        out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    numbered_dir = paths.family_dir(family_id) / "tree_export" / "numbered_newick"
    if not numbered_dir.is_dir():
        raise typer.BadParameter(
            f"No numbered newick directory found: {numbered_dir}\n"
            "Run `protsetphylo tree-export` first."
        )

    # Group clade rows by OG
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

    # Global clade color assignment (stable across OGs and the species heatmap)
    all_clade_names = sorted({c for m in og_clades.values() for c in m})
    clade_colors = {
        name: _PALETTE[i % len(_PALETTE)] for i, name in enumerate(all_clade_names)
    }

    itol_root = out_dir / "itol"
    itol_root.mkdir(parents=True, exist_ok=True)

    per_og_clade_tips: dict[str, dict[str, list[str]]] = {}
    n_rendered = 0
    n_skipped = 0

    for og_id, clades in sorted(og_clades.items()):
        nwk_path = numbered_dir / f"{og_id}.nwk"
        if not nwk_path.exists():
            typer.echo(f"  SKIP {og_id}: numbered newick not found")
            n_skipped += 1
            continue
        try:
            tree = dendropy.Tree.get(path=str(nwk_path), schema="newick")
        except Exception as e:
            typer.echo(f"  SKIP {og_id}: {e}")
            n_skipped += 1
            continue

        og_clade_tips: dict[str, list[str]] = {}
        tip_to_clade: dict[str, str] = {tip: "" for tip in _tree_tip_labels(tree)}

        for clade_name, node_num in clades.items():
            node = _find_internal_node(tree, node_num)
            if node is None:
                typer.echo(f"  WARNING: node N{node_num} not found in {og_id}")
                continue
            tips = _descendant_tip_labels(node)
            og_clade_tips[clade_name] = tips
            for t in tips:
                tip_to_clade[t] = clade_name

        if og_clade_tips:
            per_og_clade_tips[og_id] = og_clade_tips
            og_out = itol_root / og_id
            og_out.mkdir(parents=True, exist_ok=True)
            _write_og_clade_colorstrip(
                og_out / "dataset_clades.txt",
                og_name=og_id,
                tip_to_clade=tip_to_clade,
                clade_colors=clade_colors,
            )
            n_rendered += 1
        else:
            n_skipped += 1

    if not per_og_clade_tips:
        raise typer.BadParameter("No clade tips could be resolved from the provided definitions.")

    species_list, clade_list, matrix = _build_count_matrix(per_og_clade_tips, delimiter)
    matrix_path = out_dir / "clade_count_matrix.tsv"
    _write_count_matrix(species_list, clade_list, matrix, matrix_path)

    species_heatmap_path = out_dir / "itol_species_clade_heatmap.txt"
    _write_species_heatmap(
        species_list, clade_list, matrix, clade_colors, species_heatmap_path
    )

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_clade_mark",
            "family_id": family_id,
            "clade_tsv": str(clade_tsv),
            "n_clades": len(all_clade_names),
            "n_species": len(species_list),
            "n_rendered": n_rendered,
            "n_skipped": n_skipped,
            "out_dir": str(out_dir),
        },
    )

    typer.echo(
        f"Marked {len(all_clade_names)} clades across {n_rendered} OGs ({n_skipped} skipped)"
    )
    typer.echo(f"  Count matrix:       {matrix_path}")
    typer.echo(f"  Per-OG iTOL:        {itol_root}/")
    typer.echo(f"  Species heatmap:    {species_heatmap_path}")
    typer.echo(f"  Species:            {len(species_list)}")
