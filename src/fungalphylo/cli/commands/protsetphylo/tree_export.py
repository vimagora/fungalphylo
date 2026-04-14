"""protsetphylo tree-export — generate iTOL annotation files for gene trees.

Reads IQ-TREE gene tree files from a phylo run, numbers internal nodes (so
clades can be referenced by ``N<idx>``), and emits iTOL-compatible annotation
files for taxonomy, user-specified group columns, and characterized-gene
landmarks. Users upload the numbered newick + dataset files to iTOL to
explore, identify clades, and export rendered figures.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Optional

import dendropy
import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db

# ---------------------------------------------------------------------------
# Palette for global color assignment (qualitative, high contrast)
# ---------------------------------------------------------------------------
_PALETTE = [
    "#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
    "#911eb4", "#42d4f4", "#f032e6", "#bfef45", "#fabed4",
    "#469990", "#dcbeff", "#9A6324", "#fffac8", "#800000",
    "#aaffc3", "#808000", "#ffd8b1", "#000075", "#a9a9a9",
]

CHARACTERIZED_COLOR = "#e6194b"


def _color_map(values: list[str]) -> dict[str, str]:
    """Assign a palette color to each unique non-empty value (sorted)."""
    unique = sorted({v for v in values if v})
    return {v: _PALETTE[i % len(_PALETTE)] for i, v in enumerate(unique)}


# ---------------------------------------------------------------------------
# Tree I/O (dendropy)
# ---------------------------------------------------------------------------

def _find_gene_trees_dir(paths: ProjectPaths, run_id: str | None) -> tuple[Path, str]:
    """Locate gene trees from a phylo run. Auto-detects latest if run_id is None."""
    if run_id is not None:
        gt_dir = paths.run_dir(run_id) / "gene_trees"
        if not gt_dir.is_dir():
            raise typer.BadParameter(f"No gene_trees directory in run {run_id}")
        return gt_dir, run_id

    candidates: list[tuple[str, str]] = []
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


def _load_tree(newick_path: Path) -> dendropy.Tree:
    text = newick_path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Empty tree file: {newick_path}")
    # preserve_underscores=True keeps tip labels like "Aspnid1|prot_123" intact
    # (without it dendropy turns "_" into " " per the newick standard).
    return dendropy.Tree.get(data=text, schema="newick", preserve_underscores=True)


def _number_internal_nodes(tree: dendropy.Tree) -> dendropy.Tree:
    """Append ``/N<idx>`` to internal node labels (idx assigned in preorder).

    IQ-TREE outputs internal labels like ``95/88`` (UFBoot/SH-aLRT). After this
    function, the same label becomes ``95/88/N42``. Unlabeled internals become
    just ``N42``. Leaves are untouched.
    """
    counter = 0
    for node in tree.preorder_internal_node_iter():
        existing = (node.label or "").strip()
        if existing:
            node.label = f"{existing}/N{counter}"
        else:
            node.label = f"N{counter}"
        counter += 1
    return tree


_TIP_LABEL_RE = re.compile(r"([(,])([^(),:;]+)")


def _quote_tip_labels_with_underscores(newick: str) -> str:
    """Wrap any unquoted leaf labels containing ``_`` in single quotes.

    iTOL follows the classic newick convention of converting unquoted
    underscores into spaces, which breaks annotation-to-tip matching. Quoting
    the offending labels preserves them verbatim. Internal labels (which appear
    after ``)`` in newick) are not matched and therefore untouched.
    """

    def repl(match: re.Match) -> str:
        prefix = match.group(1)
        label = match.group(2)
        if "_" not in label:
            return match.group(0)
        if label.startswith("'") and label.endswith("'"):
            return match.group(0)
        return f"{prefix}'{label}'"

    return _TIP_LABEL_RE.sub(repl, newick)


def _write_numbered_newick(tree: dendropy.Tree, out_path: Path) -> None:
    """Write tree with numbered internal labels to a newick file."""
    text = tree.as_string(
        schema="newick",
        suppress_internal_node_labels=False,
        unquoted_underscores=True,
    ).strip()
    # Dendropy may prefix with "[&R]" or similar — strip any leading metadata.
    if text.startswith("[") and "]" in text:
        text = text.split("]", 1)[1].strip()
    text = _quote_tip_labels_with_underscores(text)
    out_path.write_text(text + "\n", encoding="utf-8")


def _tree_tip_labels(tree: dendropy.Tree) -> list[str]:
    return [leaf.taxon.label for leaf in tree.leaf_node_iter() if leaf.taxon]


def _tip_to_species(tip_label: str, delimiter: str = "|") -> str:
    """Extract species/short_name from ``'PortalA|proteinID'``."""
    return tip_label.split(delimiter, 1)[0]


# ---------------------------------------------------------------------------
# Annotation loading
# ---------------------------------------------------------------------------

def _load_taxonomy(paths: ProjectPaths, family_id: str) -> dict[str, dict[str, str]]:
    """Load resolved taxonomy TSV. Returns {short_name: {rank: value}}."""
    tax_path = paths.family_config_dir(family_id) / "taxonomy.tsv"
    if not tax_path.exists():
        return {}
    result: dict[str, dict[str, str]] = {}
    with tax_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            sn = (row.get("short_name") or "").strip()
            if sn:
                result[sn] = {k: (v or "").strip() for k, v in row.items() if k != "short_name"}
    return result


def _load_characterized(paths: ProjectPaths, family_id: str) -> dict[str, dict[str, str]]:
    """Load characterized.tsv. Returns {short_name: row_dict}."""
    char_path = paths.family_characterized_dir(family_id) / "characterized.tsv"
    if not char_path.exists():
        return {}
    result: dict[str, dict[str, str]] = {}
    with char_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            sn = (row.get("short_name") or "").strip()
            if sn:
                result[sn] = {k: (v or "") for k, v in row.items()}
    return result


def _available_group_columns(characterized: dict[str, dict[str, str]]) -> list[str]:
    if not characterized:
        return []
    sample = next(iter(characterized.values()))
    return sorted(k for k in sample if k.startswith("group_"))


def _validate_groups(
    selected: list[str], available: list[str], flag_name: str
) -> None:
    """Raise BadParameter if any selected group is not present in characterized.tsv."""
    missing = [g for g in selected if g not in available]
    if missing:
        avail_str = ", ".join(available) if available else "(none)"
        raise typer.BadParameter(
            f"{flag_name} columns not found in characterized.tsv: {', '.join(missing)}\n"
            f"Available group_* columns: {avail_str}"
        )


# ---------------------------------------------------------------------------
# Global color assignment (one palette, computed once per run)
# ---------------------------------------------------------------------------

def _compute_global_colors(
    characterized: dict[str, dict[str, str]],
    taxonomy: dict[str, dict[str, str]],
    tax_levels: list[str],
    color_bar_groups: list[str],
    heatmap_groups: list[str],
) -> dict:
    """Assign global colors to annotation tracks so every OG uses the same mapping.

    Returns::

        {
          "tax": {rank: {value: color}},
          "color_bar": {group: {value: color}},
          "heatmap": {group: {"color": str, "fields": [atomic values]}},
        }
    """
    result: dict = {"tax": {}, "color_bar": {}, "heatmap": {}}

    for rank in tax_levels:
        values = [tax.get(rank, "") for tax in taxonomy.values()]
        result["tax"][rank] = _color_map(values)

    for gcol in color_bar_groups:
        values = [(row.get(gcol) or "").strip() for row in characterized.values()]
        result["color_bar"][gcol] = _color_map(values)

    # Heatmap groups: one color per group, N atomic-value fields
    n_prior = len(tax_levels) + len(color_bar_groups)
    for i, gcol in enumerate(heatmap_groups):
        atomic: set[str] = set()
        for row in characterized.values():
            raw = (row.get(gcol) or "").strip()
            if not raw:
                continue
            for part in raw.split(";"):
                p = part.strip()
                if p:
                    atomic.add(p)
        color = _PALETTE[(n_prior + i) % len(_PALETTE)]
        result["heatmap"][gcol] = {
            "color": color,
            "fields": sorted(atomic),
        }

    return result


# ---------------------------------------------------------------------------
# iTOL dataset writers
# ---------------------------------------------------------------------------

def _write_colorstrip(
    out_path: Path,
    label: str,
    tip_values: dict[str, str],
    color_map: dict[str, str],
) -> None:
    """Write a DATASET_COLORSTRIP file keyed by tip label."""
    legend_values = sorted({v for v in tip_values.values() if v})
    lines = [
        "DATASET_COLORSTRIP",
        "SEPARATOR TAB",
        f"DATASET_LABEL\t{label}",
        f"COLOR\t{color_map.get(legend_values[0], '#aaaaaa') if legend_values else '#aaaaaa'}",
        "STRIP_WIDTH\t25",
        "MARGIN\t2",
        "BORDER_WIDTH\t0",
        "COLOR_BRANCHES\t0",
        f"LEGEND_TITLE\t{label}",
    ]
    if legend_values:
        lines.append("LEGEND_SHAPES\t" + "\t".join("1" for _ in legend_values))
        lines.append(
            "LEGEND_COLORS\t"
            + "\t".join(color_map.get(v, "#cccccc") for v in legend_values)
        )
        lines.append("LEGEND_LABELS\t" + "\t".join(legend_values))
    lines.append("DATA")
    for tip in sorted(tip_values):
        value = tip_values[tip]
        if not value:
            continue
        color = color_map.get(value, "#cccccc")
        lines.append(f"{tip}\t{color}\t{value}")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_binary(
    out_path: Path,
    label: str,
    field_labels: list[str],
    tip_sets: dict[str, set[str]],
    group_color: str,
) -> None:
    """Write a DATASET_BINARY file (heatmap as on/off per atomic value)."""
    lines = [
        "DATASET_BINARY",
        "SEPARATOR TAB",
        f"DATASET_LABEL\t{label}",
        f"COLOR\t{group_color}",
        "FIELD_SHAPES\t" + "\t".join("1" for _ in field_labels),
        "FIELD_LABELS\t" + "\t".join(field_labels),
        "FIELD_COLORS\t" + "\t".join(group_color for _ in field_labels),
        "SHOW_LABELS\t1",
        "SYMBOL_SPACING\t10",
        f"LEGEND_TITLE\t{label}",
        "LEGEND_SHAPES\t1",
        f"LEGEND_COLORS\t{group_color}",
        f"LEGEND_LABELS\t{label} (present)",
        "DATA",
    ]
    for tip in sorted(tip_sets):
        values = tip_sets[tip]
        row = ["1" if fl in values else "0" for fl in field_labels]
        lines.append(f"{tip}\t" + "\t".join(row))
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_symbol(
    out_path: Path,
    label: str,
    tips: list[str],
    color: str = CHARACTERIZED_COLOR,
    symbol_code: int = 3,  # 3 = star
    size: int = 15,
) -> None:
    """Write a DATASET_SYMBOL file (used for characterized-gene landmarks)."""
    lines = [
        "DATASET_SYMBOL",
        "SEPARATOR TAB",
        f"DATASET_LABEL\t{label}",
        f"COLOR\t{color}",
        "MAXIMUM_SIZE\t20",
        f"LEGEND_TITLE\t{label}",
        "LEGEND_SHAPES\t3",
        f"LEGEND_COLORS\t{color}",
        f"LEGEND_LABELS\t{label}",
        "DATA",
    ]
    # Format: ID  symbol  size  color  fill  position
    for tip in sorted(tips):
        lines.append(f"{tip}\t{symbol_code}\t{size}\t{color}\t1\t1")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Per-OG iTOL rendering
# ---------------------------------------------------------------------------

def _render_og_itol(
    tree: dendropy.Tree,
    og_name: str,
    og_out_dir: Path,
    taxonomy: dict[str, dict[str, str]],
    characterized: dict[str, dict[str, str]],
    global_colors: dict,
    tax_levels: list[str],
    color_bar_groups: list[str],
    heatmap_groups: list[str],
    delimiter: str,
) -> list[str]:
    """Write all iTOL annotation files for one OG. Returns list of written files."""
    og_out_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []

    tip_labels = _tree_tip_labels(tree)

    # Tree copy (numbered newick)
    tree_out = og_out_dir / "tree.nwk"
    _write_numbered_newick(tree, tree_out)
    written.append(tree_out.name)

    # Taxonomy color strips
    for rank in tax_levels:
        tip_values: dict[str, str] = {}
        for tip in tip_labels:
            sp = _tip_to_species(tip, delimiter)
            val = taxonomy.get(sp, {}).get(rank, "").strip()
            if val:
                tip_values[tip] = val
        if tip_values:
            path = og_out_dir / f"dataset_tax_{rank}.txt"
            _write_colorstrip(
                path,
                label=f"Taxonomy: {rank}",
                tip_values=tip_values,
                color_map=global_colors["tax"][rank],
            )
            written.append(path.name)

    # --color-bar groups
    for gcol in color_bar_groups:
        tip_values = {}
        for tip in tip_labels:
            sp = _tip_to_species(tip, delimiter)
            val = (characterized.get(sp, {}).get(gcol) or "").strip()
            if val:
                tip_values[tip] = val
        if tip_values:
            path = og_out_dir / f"dataset_colorbar_{gcol}.txt"
            _write_colorstrip(
                path,
                label=gcol.replace("group_", ""),
                tip_values=tip_values,
                color_map=global_colors["color_bar"][gcol],
            )
            written.append(path.name)

    # --heatmap groups
    for gcol in heatmap_groups:
        meta = global_colors["heatmap"][gcol]
        fields = meta["fields"]
        if not fields:
            continue
        tip_sets: dict[str, set[str]] = {}
        for tip in tip_labels:
            sp = _tip_to_species(tip, delimiter)
            raw = (characterized.get(sp, {}).get(gcol) or "").strip()
            if raw:
                tip_sets[tip] = {p.strip() for p in raw.split(";") if p.strip()}
        if tip_sets:
            path = og_out_dir / f"dataset_heatmap_{gcol}.txt"
            _write_binary(
                path,
                label=gcol.replace("group_", ""),
                field_labels=fields,
                tip_sets=tip_sets,
                group_color=meta["color"],
            )
            written.append(path.name)

    # Characterized-gene landmarks
    landmark_tips = [
        tip for tip in tip_labels if _tip_to_species(tip, delimiter) in characterized
    ]
    if landmark_tips:
        path = og_out_dir / "dataset_landmarks.txt"
        _write_symbol(path, label="Characterized", tips=landmark_tips)
        written.append(path.name)

    return written


def _write_upload_readme(out_dir: Path, og_names: list[str]) -> None:
    """Write top-level instructions for uploading to iTOL."""
    lines = [
        "# Uploading to iTOL",
        "",
        "1. Go to https://itol.embl.de/ and sign in (free account).",
        "2. Click **Upload new tree** and upload `itol/<OG>/tree.nwk`.",
        "3. On the tree page, drag all `itol/<OG>/dataset_*.txt` files onto the browser",
        "   window to load annotations. A panel will open on the right.",
        "4. In the **Datasets** panel you can toggle tracks on/off, reorder, and",
        "   adjust sizes. Each dataset has a built-in legend.",
        "5. Use **Export** (top-right) to download PDF/SVG/PNG.",
        "",
        "## Identifying clades",
        "",
        "Internal node labels in the tree are formatted as `<UFBoot>/<SH-aLRT>/N<idx>`",
        "(IQ-TREE support values plus a node index). To mark a clade, read the node",
        "index off the tree and add a row to your `clades.tsv`:",
        "",
        "```",
        "clade_name\tog_id\tnode_number",
        "my_clade\tOG0000001\t42",
        "```",
        "",
        "Then run `fungalphylo protsetphylo clade-mark`.",
        "",
        "## OGs included",
        "",
    ]
    for og in og_names:
        lines.append(f"- `itol/{og}/`")
    lines.append("")
    (out_dir / "iTOL_UPLOAD.md").write_text("\n".join(lines), encoding="utf-8")


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
        help="Taxonomy rank for a color strip (repeatable, e.g. --tax-level order --tax-level family).",
    ),
    color_bar: Optional[list[str]] = typer.Option(
        None, "--color-bar",
        help="group_* column to render as a single color strip (repeatable).",
    ),
    heatmap: Optional[list[str]] = typer.Option(
        None, "--heatmap",
        help="group_* column to render as a binary heatmap of atomic values (repeatable).",
    ),
    delimiter: str = typer.Option("|", "--delimiter", help="Tip label delimiter."),
    out_dir: Path | None = typer.Option(
        None, "--out-dir", help="Output directory (default: families/<id>/tree_export/).",
    ),
) -> None:
    """Generate iTOL annotation files for gene trees in a phylo run.

    Writes one folder per OG under ``tree_export/itol/<OG>/`` containing
    ``tree.nwk`` and one ``dataset_*.txt`` per annotation track. Upload the
    tree to iTOL, then drag the dataset files onto the tree page.
    """
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    tax_levels = list(tax_level or [])
    color_bar_groups = list(color_bar or [])
    heatmap_groups = list(heatmap or [])

    # Load annotation tables
    characterized = _load_characterized(paths, family_id)
    taxonomy = _load_taxonomy(paths, family_id) if tax_levels else {}

    # Validate group columns exist
    available_groups = _available_group_columns(characterized)
    _validate_groups(color_bar_groups, available_groups, "--color-bar")
    _validate_groups(heatmap_groups, available_groups, "--heatmap")
    if tax_levels and not taxonomy:
        raise typer.BadParameter(
            "--tax-level was requested but no taxonomy.tsv found. "
            "Run `fungalphylo taxonomy export/apply --family-id` first."
        )

    # Find gene trees
    gene_trees_dir, resolved_run_id = _find_gene_trees_dir(paths, run_id)
    tree_files = sorted(gene_trees_dir.glob("*/*.treefile"))
    if not tree_files:
        raise typer.BadParameter(f"No .treefile files found in {gene_trees_dir}")

    # Output directory layout
    if out_dir is None:
        out_dir = paths.family_dir(family_id) / "tree_export"
    else:
        out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    numbered_dir = out_dir / "numbered_newick"
    numbered_dir.mkdir(parents=True, exist_ok=True)
    itol_root = out_dir / "itol"
    itol_root.mkdir(parents=True, exist_ok=True)

    # Global color assignment (one palette for the whole family)
    global_colors = _compute_global_colors(
        characterized=characterized,
        taxonomy=taxonomy,
        tax_levels=tax_levels,
        color_bar_groups=color_bar_groups,
        heatmap_groups=heatmap_groups,
    )

    og_names: list[str] = []
    n_rendered = 0
    n_skipped = 0

    for tf in tree_files:
        og_name = tf.parent.name
        try:
            tree = _load_tree(tf)
        except Exception as e:
            typer.echo(f"  SKIP {og_name}: {e}")
            n_skipped += 1
            continue

        tree = _number_internal_nodes(tree)

        # Write shared numbered newick (used by clade-mark)
        _write_numbered_newick(tree, numbered_dir / f"{og_name}.nwk")

        # Write iTOL dataset folder
        _render_og_itol(
            tree=tree,
            og_name=og_name,
            og_out_dir=itol_root / og_name,
            taxonomy=taxonomy,
            characterized=characterized,
            global_colors=global_colors,
            tax_levels=tax_levels,
            color_bar_groups=color_bar_groups,
            heatmap_groups=heatmap_groups,
            delimiter=delimiter,
        )

        og_names.append(og_name)
        n_rendered += 1

    if not og_names:
        raise typer.BadParameter("No trees could be loaded.")

    _write_upload_readme(out_dir, og_names)

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
            "color_bar_groups": color_bar_groups,
            "heatmap_groups": heatmap_groups,
            "out_dir": str(out_dir),
        },
    )

    typer.echo(f"Rendered iTOL datasets for {n_rendered} OGs ({n_skipped} skipped)")
    typer.echo(f"  iTOL datasets:    {itol_root}/")
    typer.echo(f"  Numbered Newick:  {numbered_dir}/")
    typer.echo(f"  Upload guide:     {out_dir / 'iTOL_UPLOAD.md'}")
    if tax_levels:
        typer.echo(f"  Tax levels:       {', '.join(tax_levels)}")
    if color_bar_groups:
        typer.echo(f"  Color bars:       {', '.join(color_bar_groups)}")
    if heatmap_groups:
        heatmap_summary = ", ".join(
            f"{g} ({len(global_colors['heatmap'][g]['fields'])} fields)"
            for g in heatmap_groups
        )
        typer.echo(f"  Heatmaps:         {heatmap_summary}")
