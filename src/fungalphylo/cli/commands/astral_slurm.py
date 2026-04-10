from __future__ import annotations

import json
import re
from pathlib import Path

import dendropy
import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import register_run, resolve_account, submit_sbatch
from fungalphylo.db.db import init_db

app = typer.Typer(
    help="Collect gene trees, rename tips to species, and generate ASTRAL-Pro SLURM script."
)

# Match tip labels in Newick: any run of characters that isn't (),:; or whitespace
_TIP_RE = re.compile(r"(?:^|[,(])\s*([^():,;\s]+)")


def _extract_tips(newick: str) -> list[str]:
    """Extract tip labels from a Newick string."""
    return _TIP_RE.findall(newick)


def _rename_tips_to_species(newick: str, delimiter: str) -> tuple[str, set[str]]:
    """Rename tip labels to species names using dendropy.

    Returns (renamed_newick, set_of_species_names).
    """
    tree = dendropy.Tree.get(data=newick, schema="newick")
    species: set[str] = set()
    for leaf in tree.leaf_node_iter():
        label = leaf.taxon.label
        sp = label.split(delimiter, 1)[0]
        leaf.taxon.label = sp
        species.add(sp)
    renamed = tree.as_string(schema="newick").strip()
    return renamed, species


def _render_astral_script(
    *,
    acct: str,
    rid: str,
    logs_dir: Path,
    time: str,
    cpus: int,
    mem: str,
    partition: str,
    gene_trees_file: Path,
    output_tree: Path,
    astral_cmd: str,
    extra_args: str,
) -> str:
    return f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=astral_{rid}
#SBATCH --output={logs_dir.as_posix()}/astral_{rid}_%j.out
#SBATCH --error={logs_dir.as_posix()}/astral_{rid}_%j.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --partition={partition}

set -euo pipefail

module load aster/1.23

echo "=== ASTRAL-Pro ==="
echo "Gene trees: {gene_trees_file.as_posix()}"
echo "Output:     {output_tree.as_posix()}"

{astral_cmd} \\
  -i "{gene_trees_file.as_posix()}" \\
  -o "{output_tree.as_posix()}" \\
  {extra_args}

echo "=== Done ==="
"""


@app.callback(invoke_without_command=True)
def astral_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(..., help="Project directory."),
    run_id: str | None = typer.Option(
        None, "--run-id",
        help="Phylo run ID to collect gene trees from.",
    ),
    output_run_id: str | None = typer.Option(
        None, "--output-run-id",
        help="Run ID for this ASTRAL run (default: astral_<timestamp>).",
    ),
    delimiter: str = typer.Option(
        "|", "--delimiter",
        help="Delimiter separating species from gene ID in tip labels.",
    ),
    min_taxa: int = typer.Option(
        4, "--min-taxa",
        help="Minimum number of species in a gene tree to include it.",
    ),
    account: str | None = typer.Option(
        None, "--account", help="SLURM account (overrides auto-detect)"
    ),
    no_confirm: bool = typer.Option(
        False, "--no-confirm", help="Do not prompt to confirm detected account"
    ),
    time: str = typer.Option("04:00:00", "--time", help="SLURM time"),
    cpus: int = typer.Option(4, "--cpus", help="CPUs"),
    mem: str = typer.Option("16G", "--mem", help="Memory"),
    partition: str = typer.Option("small", "--partition", help="SLURM partition"),
    astral_cmd: str = typer.Option(
        "astral-pro3", "--astral-cmd",
        help="ASTRAL-Pro executable name (default: astral-pro3 from aster module).",
    ),
    extra_args: str = typer.Option(
        "", "--extra-args",
        help="Additional arguments to pass to ASTRAL-Pro.",
    ),
    submit: bool = typer.Option(
        False, "--submit", help="Submit with sbatch after writing script"
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    # Find phylo run with gene trees
    if run_id is not None:
        gene_trees_dir = paths.run_dir(run_id) / "gene_trees"
        source_run_id = run_id
    else:
        candidates = []
        for manifest_path in paths.runs_root.glob("*/manifest.json"):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                if data.get("kind") == "phylo":
                    gt_dir = manifest_path.parent / "gene_trees"
                    if gt_dir.is_dir() and any(gt_dir.rglob("*.treefile")):
                        candidates.append((data.get("created_at", ""), data["run_id"]))
            except (json.JSONDecodeError, KeyError):
                continue
        if not candidates:
            raise typer.BadParameter(
                "No phylo runs with gene trees found. "
                "Run `phylo-slurm` first, or provide --run-id."
            )
        candidates.sort(reverse=True)
        source_run_id = candidates[0][1]
        gene_trees_dir = paths.run_dir(source_run_id) / "gene_trees"
        typer.echo(f"Using gene trees from run: {source_run_id}")

    if not gene_trees_dir.is_dir():
        raise typer.BadParameter(f"Gene trees directory not found: {gene_trees_dir}")

    tree_files = sorted(gene_trees_dir.rglob("*.treefile"))
    if not tree_files:
        raise typer.BadParameter(f"No .treefile files found in {gene_trees_dir}")

    # Account
    acct = resolve_account(project_dir, account, no_confirm)

    # Set up run directory
    rid = output_run_id or f"astral_{now_tag()}"
    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    logs_dir = paths.logs_dir / "slurm"
    for d in (slurm_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    # Collect gene trees, rename tips to species, filter by min_taxa
    all_species: set[str] = set()
    kept_trees: list[str] = []
    skipped = 0

    for tf in tree_files:
        newick = tf.read_text(encoding="utf-8").strip()
        if not newick:
            skipped += 1
            continue
        tips = _extract_tips(newick)
        # Count unique species before renaming
        species = {t.split(delimiter, 1)[0] for t in tips}
        if len(species) < min_taxa:
            skipped += 1
            continue
        # Rename tips to species names using dendropy
        renamed, tree_species = _rename_tips_to_species(newick, delimiter)
        all_species.update(tree_species)
        kept_trees.append(renamed)

    if not kept_trees:
        raise typer.BadParameter(
            f"No gene trees passed --min-taxa {min_taxa} filter "
            f"({len(tree_files)} trees found, all skipped)."
        )

    # Write concatenated gene trees (tips already renamed to species)
    gene_trees_file = slurm_dir / "gene_trees.nwk"
    gene_trees_file.write_text(
        "\n".join(kept_trees) + "\n", encoding="utf-8"
    )

    # Output tree path
    output_tree = run_root / "species_tree.nwk"

    # Generate SLURM script (no mapping file needed — tips are species names)
    script = _render_astral_script(
        acct=acct,
        rid=rid,
        logs_dir=logs_dir,
        time=time,
        cpus=cpus,
        mem=mem,
        partition=partition,
        gene_trees_file=gene_trees_file,
        output_tree=output_tree,
        astral_cmd=astral_cmd,
        extra_args=extra_args,
    )

    script_path = slurm_dir / "astral.sbatch"
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    # Manifest
    created_at = now_iso()
    manifest_data = {
        "run_id": rid,
        "kind": "astral",
        "created_at": created_at,
        "source_run_id": source_run_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "script": str(script_path.relative_to(project_dir)),
            "gene_trees": str(gene_trees_file.relative_to(project_dir)),
            "output_tree": str(output_tree.relative_to(project_dir)),
        },
        "parameters": {
            "delimiter": delimiter,
            "min_taxa": min_taxa,
            "astral_cmd": astral_cmd,
            "extra_args": extra_args,
        },
        "stats": {
            "total_tree_files": len(tree_files),
            "kept_trees": len(kept_trees),
            "skipped_trees": skipped,
            "total_species": len(all_species),
        },
        "slurm": {
            "account": acct,
            "partition": partition,
            "time": time,
            "cpus": cpus,
            "mem": mem,
            "submit": submit,
        },
    }
    register_run(paths, project_dir, rid, "astral", created_at, manifest_data)

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "astral_prep_write",
            "run_id": rid,
            "source_run_id": source_run_id,
            "kept_trees": len(kept_trees),
            "skipped_trees": skipped,
            "total_species": len(all_species),
            "script": str(script_path),
            "submit": submit,
        },
    )

    typer.echo(f"ASTRAL-Pro preparation complete:")
    typer.echo(f"  Gene trees:   {len(kept_trees)} kept, {skipped} skipped (of {len(tree_files)})")
    typer.echo(f"  Species:      {len(all_species)}")
    typer.echo(f"  Trees file:   {gene_trees_file}")
    typer.echo(f"  SLURM script: {script_path}")
    typer.echo(f"  Output tree:  {output_tree}")
    typer.echo(f"  Tips renamed to species (no mapping file needed)")

    if submit:
        stdout = submit_sbatch(script_path)
        typer.echo(stdout or "Submitted.")
        log_event(
            project_dir,
            {
                "ts": now_iso(),
                "event": "astral_submit",
                "run_id": rid,
                "script": str(script_path),
                "sbatch_stdout": stdout,
            },
        )
