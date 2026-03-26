from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db

app = typer.Typer(
    help="Filter OrthoFinder orthogroups by single-copy species occupancy."
)


def _find_of_results_dir(results_root: Path) -> Path:
    """Find the latest Results_* directory inside an OrthoFinder output root."""
    candidates = sorted(results_root.glob("Results_*"), reverse=True)
    for c in candidates:
        if (c / "Orthogroups").is_dir():
            return c
    raise typer.BadParameter(
        f"No OrthoFinder Results_* directory found in {results_root}"
    )


def _parse_gene_counts(gene_count_tsv: Path) -> tuple[list[str], dict[str, list[int]]]:
    """Parse Orthogroups.GeneCount.tsv.

    Returns (species_names, {og_id: [count_per_species]}).
    The last column ('Total') is excluded.
    """
    with gene_count_tsv.open(encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        header = next(reader)
        # Header: Orthogroup \t Species0 \t Species1 \t ... \t Total
        species = header[1:-1]  # drop Orthogroup and Total
        og_counts: dict[str, list[int]] = {}
        for row in reader:
            if not row or not row[0].strip():
                continue
            og_id = row[0]
            counts = [int(c) for c in row[1:-1]]
            og_counts[og_id] = counts
    return species, og_counts


def _filter_orthogroups(
    og_counts: dict[str, list[int]],
    n_species: int,
    min_single_copy: float,
) -> list[tuple[str, int, int, int]]:
    """Return list of (og_id, single_copy, multi_copy, missing) for passing OGs."""
    threshold = min_single_copy * n_species
    selected: list[tuple[str, int, int, int]] = []
    for og_id, counts in og_counts.items():
        single = sum(1 for c in counts if c == 1)
        multi = sum(1 for c in counts if c > 1)
        missing = sum(1 for c in counts if c == 0)
        if single >= threshold:
            selected.append((og_id, single, multi, missing))
    selected.sort(key=lambda x: x[0])
    return selected


@app.callback(invoke_without_command=True)
def filter_orthogroups_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(
        ..., help="Project directory."
    ),
    run_id: str | None = typer.Option(
        None, "--run-id",
        help="OrthoFinder run ID to read results from.",
    ),
    results_dir: Path | None = typer.Option(
        None, "--results-dir",
        help="Explicit path to OrthoFinder results root (overrides --run-id).",
    ),
    min_single_copy: float = typer.Option(
        0.75, "--min-single-copy",
        help="Minimum fraction of species with exactly 1 copy (default: 0.75).",
    ),
    output_dir: Path | None = typer.Option(
        None, "--output-dir",
        help="Output directory for selected OGs (default: runs/<run_id>/filtered_orthogroups/).",
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    # Resolve OrthoFinder results
    if results_dir is not None:
        of_root = results_dir.expanduser().resolve()
    elif run_id is not None:
        of_root = paths.run_dir(run_id) / "orthofinder_results"
    else:
        # Try to find latest orthofinder run from manifests
        of_runs = []
        for manifest_path in paths.runs_root.glob("*/manifest.json"):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                if data.get("kind") == "orthofinder":
                    of_runs.append((data.get("created_at", ""), data["run_id"]))
            except (json.JSONDecodeError, KeyError):
                continue
        if not of_runs:
            raise typer.BadParameter(
                "No OrthoFinder runs found. Provide --run-id or --results-dir."
            )
        of_runs.sort(reverse=True)
        run_id = of_runs[0][1]
        of_root = paths.run_dir(run_id) / "orthofinder_results"
        typer.echo(f"Using latest OrthoFinder run: {run_id}")

    if not of_root.is_dir():
        raise typer.BadParameter(f"Results directory does not exist: {of_root}")

    of_results = _find_of_results_dir(of_root)
    gene_count_tsv = of_results / "Orthogroups" / "Orthogroups.GeneCount.tsv"
    og_sequences_dir = of_results / "Orthogroup_Sequences"

    if not gene_count_tsv.is_file():
        raise typer.BadParameter(f"Missing gene count file: {gene_count_tsv}")
    if not og_sequences_dir.is_dir():
        raise typer.BadParameter(f"Missing orthogroup sequences dir: {og_sequences_dir}")

    # Parse and filter
    species, og_counts = _parse_gene_counts(gene_count_tsv)
    n_species = len(species)
    selected = _filter_orthogroups(og_counts, n_species, min_single_copy)

    typer.echo(f"Species:              {n_species}")
    typer.echo(f"Total orthogroups:    {len(og_counts)}")
    typer.echo(f"Threshold:            ≥{min_single_copy:.0%} single-copy ({int(min_single_copy * n_species)}/{n_species} species)")
    typer.echo(f"Selected orthogroups: {len(selected)}")

    if not selected:
        typer.echo("No orthogroups passed the filter.")
        raise typer.Exit(code=0)

    # Resolve output directory
    if output_dir is not None:
        out = output_dir.expanduser().resolve()
    elif run_id is not None:
        out = paths.run_dir(run_id) / "filtered_orthogroups"
    else:
        out = of_root / "filtered_orthogroups"
    out.mkdir(parents=True, exist_ok=True)

    # Copy selected OG FASTA files
    copied = 0
    missing_fas: list[str] = []
    for og_id, _, _, _ in selected:
        src = og_sequences_dir / f"{og_id}.fa"
        if src.is_file():
            shutil.copy2(src, out / f"{og_id}.fa")
            copied += 1
        else:
            missing_fas.append(og_id)

    if missing_fas:
        typer.echo(f"WARNING: {len(missing_fas)} selected OGs missing from Orthogroup_Sequences/")

    # Write summary TSV
    summary_path = out / "filter_summary.tsv"
    with summary_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(["orthogroup", "single_copy", "multi_copy", "missing", "total_species"])
        for og_id, single, multi, miss in selected:
            writer.writerow([og_id, single, multi, miss, n_species])

    typer.echo(f"Copied {copied} FASTA files to: {out}")
    typer.echo(f"Summary: {summary_path}")

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "filter_orthogroups",
            "run_id": run_id,
            "results_dir": str(of_root),
            "n_species": n_species,
            "n_total_ogs": len(og_counts),
            "n_selected": len(selected),
            "min_single_copy": min_single_copy,
            "output_dir": str(out),
        },
    )
