from __future__ import annotations

import csv
import json
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.fasta import FastaRecord, iter_fasta, write_fasta
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import register_run, resolve_account, submit_sbatch
from fungalphylo.core.tools import bin_dir_export_lines, load_tools
from fungalphylo.db.db import connect, init_db


def _load_outgroup_fasta(path: Path) -> dict[str, FastaRecord]:
    records: dict[str, FastaRecord] = {}
    for rec in iter_fasta(path):
        key = rec.header.split()[0]
        if key in records:
            raise typer.BadParameter(
                f"Duplicate outgroup header {key!r} in {path}"
            )
        records[key] = rec
    if not records:
        raise typer.BadParameter(f"No records found in outgroup FASTA: {path}")
    return records


def _load_outgroup_map(path: Path) -> dict[str, list[str]]:
    """Parse outgroup mapping TSV. Columns: og_id, outgroup_id."""
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if reader.fieldnames is None:
            raise typer.BadParameter(f"Empty outgroup map TSV: {path}")
        required = {"og_id", "outgroup_id"}
        missing = required - set(reader.fieldnames)
        if missing:
            raise typer.BadParameter(
                f"Missing columns in outgroup map TSV: {', '.join(sorted(missing))}. "
                f"Found: {', '.join(reader.fieldnames)}"
            )
        mapping: dict[str, list[str]] = {}
        for row in reader:
            og_id = (row.get("og_id") or "").strip()
            og_id = og_id.removesuffix(".fa")
            outgroup_id = (row.get("outgroup_id") or "").strip()
            if not og_id or not outgroup_id:
                continue
            lst = mapping.setdefault(og_id, [])
            if outgroup_id not in lst:
                lst.append(outgroup_id)
    if not mapping:
        raise typer.BadParameter(f"No (og_id, outgroup_id) rows in: {path}")
    return mapping


def _prepare_outgrouped_inputs(
    og_files: list[Path],
    outgroup_records: dict[str, FastaRecord],
    outgroup_map: dict[str, list[str]],
    augmented_dir: Path,
) -> tuple[list[Path], dict[str, list[str]]]:
    """Return (final_og_files, outgroup_tips_per_og).

    For each OG with a mapping, write an augmented FASTA (original + mapped
    outgroup records) into ``augmented_dir`` and substitute the path. OGs
    without a mapping are passed through unchanged. Validates that every OG
    referenced in the map exists and that every outgroup_id is in the FASTA.
    """
    og_name_to_path = {p.stem: p for p in og_files}

    unknown_ogs = sorted(set(outgroup_map) - set(og_name_to_path))
    if unknown_ogs:
        raise typer.BadParameter(
            "Outgroup map references OGs not present in input dir: "
            + ", ".join(unknown_ogs)
        )

    all_tip_ids = {
        tip for tips in outgroup_map.values() for tip in tips
    }
    unknown_tips = sorted(all_tip_ids - set(outgroup_records))
    if unknown_tips:
        raise typer.BadParameter(
            "Outgroup map references outgroup_id(s) not in outgroup FASTA: "
            + ", ".join(unknown_tips)
        )

    augmented_dir.mkdir(parents=True, exist_ok=True)
    final_files: list[Path] = []
    tips_per_og: dict[str, list[str]] = {}
    for og_path in og_files:
        og_name = og_path.stem
        if og_name not in outgroup_map:
            final_files.append(og_path)
            continue
        tip_ids = outgroup_map[og_name]
        merged = list(iter_fasta(og_path))
        existing_headers = {r.header.split()[0] for r in merged}
        for tid in tip_ids:
            rec = outgroup_records[tid]
            if tid in existing_headers:
                # Already present in the OG — skip, still root on it.
                continue
            merged.append(rec)
        out_path = augmented_dir / f"{og_name}.fa"
        write_fasta(merged, out_path)
        final_files.append(out_path)
        tips_per_og[og_name] = [outgroup_records[t].header.split()[0] for t in tip_ids]
    return final_files, tips_per_og


def _write_outgroup_tips_tsv(tips_per_og: dict[str, list[str]], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        for og_name in sorted(tips_per_og):
            w.writerow([og_name, ",".join(tips_per_og[og_name])])


def _find_og_input_dir(paths: ProjectPaths, family_id: str) -> Path:
    """Find the best OG FASTA directory for a family: og_placed > og_selected."""
    og_placed = paths.family_og_placed_dir(family_id)
    if og_placed.is_dir() and any(og_placed.glob("*.fa")):
        return og_placed
    og_selected = paths.family_og_selected_dir(family_id)
    if og_selected.is_dir() and any(og_selected.glob("*.fa")):
        return og_selected
    raise typer.BadParameter(
        f"No OG FASTA files found for family {family_id!r}. "
        "Run `protsetphylo og-apply` first, or provide --input-dir."
    )


def _render_orchestrator(
    *,
    filelist_path: Path,
    output_root: Path,
    worker_script: Path,
    max_array_size: int,
    max_concurrent: int,
    acct: str,
    # Per-step SLURM overrides
    align_time: str,
    align_cpus: int,
    align_mem_per_cpu: str,
    trim_time: str,
    trim_cpus: int,
    trim_mem_per_cpu: str,
    tree_time: str,
    tree_cpus: int,
    tree_mem_per_cpu: str,
) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob
export LC_ALL=C

FILELIST="{filelist_path.as_posix()}"
OUTPUT_ROOT="{output_root.as_posix()}"
WORKER="{worker_script.as_posix()}"
MAX_ARRAY_SIZE={max_array_size}
MAX_CONCURRENT={max_concurrent}

# Clean up empty treefiles from failed runs
find "$OUTPUT_ROOT" -name "*.treefile" -type f -empty -delete 2>/dev/null || true

# Compute pending OGs (those without a non-empty treefile)
PENDING=$(mktemp)
trap 'rm -f "$PENDING"' EXIT

while IFS= read -r og_path; do
    [ -z "$og_path" ] && continue
    OG_NAME=$(basename "$og_path" .fa)
    TREEFILE="$OUTPUT_ROOT/$OG_NAME/$OG_NAME.treefile"
    if [ ! -s "$TREEFILE" ]; then
        echo "$og_path" >> "$PENDING"
    fi
done < "$FILELIST"

NPENDING=$(wc -l < "$PENDING" 2>/dev/null || echo 0)
NPENDING=$((NPENDING + 0))

if [ "$NPENDING" -eq 0 ]; then
    echo "All orthogroups have completed trees."
    exit 0
fi

echo "$NPENDING orthogroups pending."

# Cap array size to respect SLURM limits
if [ "$NPENDING" -gt "$MAX_ARRAY_SIZE" ]; then
    echo "Capping array to $MAX_ARRAY_SIZE tasks (rerun to process remaining)."
    head -n "$MAX_ARRAY_SIZE" "$PENDING" > "${{PENDING}}.cap"
    mv "${{PENDING}}.cap" "$PENDING"
    NPENDING=$MAX_ARRAY_SIZE
fi

# Write the pending list for the worker
PENDING_LIST="${{FILELIST}}.pending"
cp "$PENDING" "$PENDING_LIST"

echo "Submitting chained array jobs: align -> trim -> tree"
echo "Array size: $NPENDING, max concurrent: $MAX_CONCURRENT"
echo ""

# Step 1: Alignment
ALIGN_JOB=$(sbatch --parsable \\
    --array="1-${{NPENDING}}%${{MAX_CONCURRENT}}" \\
    --account={acct} \\
    --time={align_time} --cpus-per-task={align_cpus} --mem-per-cpu={align_mem_per_cpu} \\
    "$WORKER" --step align "$PENDING_LIST")
echo "Submitted align  array: $ALIGN_JOB"

# Step 2: Trimming (depends on alignment)
TRIM_JOB=$(sbatch --parsable \\
    --dependency=afterok:${{ALIGN_JOB}} \\
    --array="1-${{NPENDING}}%${{MAX_CONCURRENT}}" \\
    --account={acct} \\
    --time={trim_time} --cpus-per-task={trim_cpus} --mem-per-cpu={trim_mem_per_cpu} \\
    "$WORKER" --step trim "$PENDING_LIST")
echo "Submitted trim   array: $TRIM_JOB (depends on $ALIGN_JOB)"

# Step 3: Tree building (depends on trimming)
TREE_JOB=$(sbatch --parsable \\
    --dependency=afterok:${{TRIM_JOB}} \\
    --array="1-${{NPENDING}}%${{MAX_CONCURRENT}}" \\
    --account={acct} \\
    --time={tree_time} --cpus-per-task={tree_cpus} --mem-per-cpu={tree_mem_per_cpu} \\
    "$WORKER" --step tree "$PENDING_LIST")
echo "Submitted tree   array: $TREE_JOB (depends on $TRIM_JOB)"

echo ""
echo "Chain: align($ALIGN_JOB) -> trim($TRIM_JOB) -> tree($TREE_JOB)"
echo "Rerun this script after completion to process any remaining OGs."
"""


def _render_worker(
    *,
    rid: str,
    logs_dir: Path,
    partition: str,
    output_root: Path,
    bin_export: str,
    mafft_cmd: str,
    trimal_cmd: str,
    iqtree_cmd: str,
    trimal_gt: float,
    trimal_cons: float,
    iqtree_model: str,
    iqtree_bootstrap: int,
    iqtree_alrt: int,
    iqtree_fast: bool,
    outgroup_tips_path: Path | None,
) -> str:
    fast_flag = " -fast" if iqtree_fast else ""
    outgroup_tips_literal = (
        outgroup_tips_path.as_posix() if outgroup_tips_path is not None else ""
    )
    return f"""#!/bin/bash
#SBATCH --job-name=phylo_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%A_%a.out
#SBATCH --error={logs_dir.as_posix()}/%x_%A_%a.err
#SBATCH --partition={partition}

set -euo pipefail

# Parse arguments: --step {{align,trim,tree}} <filelist>
STEP=""
FILELIST=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --step) STEP="$2"; shift 2;;
        *) FILELIST="$1"; shift;;
    esac
done

if [[ -z "$STEP" || -z "$FILELIST" ]]; then
    echo "Usage: $0 --step {{align,trim,tree}} <filelist>" >&2
    exit 1
fi

module load mafft

{bin_export}
THREADS="${{SLURM_CPUS_PER_TASK:-1}}"

# Read the OG file for this array task
OG_FASTA=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" "$FILELIST")
OG_NAME=$(basename "$OG_FASTA" .fa)

OUTDIR="{output_root.as_posix()}/$OG_NAME"
mkdir -p "$OUTDIR"

ALIGNED="$OUTDIR/$OG_NAME.aligned.faa"
TRIMMED="$OUTDIR/$OG_NAME.trimmed.faa"

# Resolve outgroup tips for this OG (if any)
OUTGROUP_TIPS_FILE="{outgroup_tips_literal}"
OG_OUTGROUPS=""
if [ -n "$OUTGROUP_TIPS_FILE" ] && [ -s "$OUTGROUP_TIPS_FILE" ]; then
    OG_OUTGROUPS=$(awk -F'\\t' -v og="$OG_NAME" '$1==og{{print $2; exit}}' "$OUTGROUP_TIPS_FILE")
fi

echo "=== Task $SLURM_ARRAY_TASK_ID | Step: $STEP | OG: $OG_NAME ==="

case "$STEP" in
    align)
        if [ ! -s "$ALIGNED" ]; then
            echo "--- MAFFT (--auto) ---"
            "{mafft_cmd}" --auto --thread "$THREADS" "$OG_FASTA" > "$ALIGNED"
            if [ ! -s "$ALIGNED" ]; then
                echo "ERROR: MAFFT produced empty alignment for $OG_NAME" >&2
                exit 1
            fi
        else
            echo "Alignment exists, skipping."
        fi
        ;;
    trim)
        if [ ! -s "$TRIMMED" ]; then
            if [ ! -s "$ALIGNED" ]; then
                echo "ERROR: Alignment missing for $OG_NAME, cannot trim" >&2
                exit 1
            fi
            echo "--- trimAl ---"
            "{trimal_cmd}" -in "$ALIGNED" -out "$TRIMMED" \\
              -gt {trimal_gt} -cons {trimal_cons}
            if [ ! -s "$TRIMMED" ]; then
                echo "ERROR: trimAl produced empty output for $OG_NAME" >&2
                exit 1
            fi
        else
            echo "Trimmed alignment exists, skipping."
        fi
        ;;
    tree)
        if [ ! -s "$OUTDIR/$OG_NAME.treefile" ]; then
            if [ ! -s "$TRIMMED" ]; then
                echo "ERROR: Trimmed alignment missing for $OG_NAME, cannot build tree" >&2
                exit 1
            fi
            echo "--- IQ-TREE ---"
            IQTREE_O_FLAG=""
            if [ -n "$OG_OUTGROUPS" ]; then
                IQTREE_O_FLAG="-o $OG_OUTGROUPS"
                echo "Rooting on outgroup(s): $OG_OUTGROUPS"
            fi
            "{iqtree_cmd}" -s "$TRIMMED" \\
              -m {iqtree_model} -B {iqtree_bootstrap} -alrt {iqtree_alrt} \\
              -T "$THREADS"{fast_flag} $IQTREE_O_FLAG --prefix "$OUTDIR/$OG_NAME"
        else
            echo "Tree exists, skipping."
        fi
        ;;
    *)
        echo "ERROR: Unknown step '$STEP'. Use align, trim, or tree." >&2
        exit 1
        ;;
esac

echo "=== Done: $OG_NAME ($STEP) ==="
"""


def phylo_slurm_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Family to run phylo pipeline on"),
    input_dir: Path | None = typer.Option(
        None, "--input-dir",
        help="Explicit directory of .fa OG files (overrides auto-detection).",
    ),
    output_run_id: str | None = typer.Option(
        None, "--output-run-id", help="Run ID (default: phylo_<family>_<timestamp>)"
    ),
    account: str | None = typer.Option(None, "--account", help="SLURM account"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Skip account confirmation"),
    partition: str = typer.Option("small", "--partition", help="SLURM partition"),
    max_concurrent: int | None = typer.Option(
        None, "--max-concurrent", help="Max concurrent array tasks (default: 100)"
    ),
    max_array_size: int | None = typer.Option(
        None, "--max-array-size", help="Max array tasks per submission (default: 380)"
    ),
    # Per-step SLURM resource overrides
    align_time: str = typer.Option("04:00:00", "--align-time", help="Time for alignment step"),
    align_cpus: int = typer.Option(8, "--align-cpus", help="CPUs for alignment step"),
    align_mem_per_cpu: str = typer.Option(
        "4G", "--align-mem-per-cpu", help="Memory per CPU for alignment step"
    ),
    trim_time: str = typer.Option("00:15:00", "--trim-time", help="Time for trimming step"),
    trim_cpus: int = typer.Option(1, "--trim-cpus", help="CPUs for trimming step"),
    trim_mem_per_cpu: str = typer.Option(
        "4G", "--trim-mem-per-cpu", help="Memory per CPU for trimming step"
    ),
    tree_time: str = typer.Option("08:00:00", "--tree-time", help="Time for tree building step"),
    tree_cpus: int = typer.Option(8, "--tree-cpus", help="CPUs for tree building step"),
    tree_mem_per_cpu: str = typer.Option(
        "4G", "--tree-mem-per-cpu", help="Memory per CPU for tree building step"
    ),
    # Tool parameters
    trimal_gt: float = typer.Option(0.8, "--trimal-gt", help="trimAl gap threshold"),
    trimal_cons: float = typer.Option(10.0, "--trimal-cons", help="trimAl conservation threshold"),
    iqtree_model: str = typer.Option("TEST", "--iqtree-model", help="IQ-TREE model selection"),
    iqtree_bootstrap: int = typer.Option(
        1000, "--iqtree-bootstrap", help="IQ-TREE ultrafast bootstrap replicates"
    ),
    iqtree_alrt: int = typer.Option(1000, "--iqtree-alrt", help="IQ-TREE SH-aLRT replicates"),
    iqtree_fast: bool = typer.Option(
        False, "--iqtree-fast", help="Use IQ-TREE -fast mode for quicker tree inference"
    ),
    outgroup_fasta: Path | None = typer.Option(
        None, "--outgroup-fasta",
        help="FASTA of outgroup sequences (headers become iqtree -o tip labels).",
    ),
    outgroup_map: Path | None = typer.Option(
        None, "--outgroup-map",
        help="TSV mapping outgroups to OGs. Columns: og_id, outgroup_id.",
    ),
    submit: bool = typer.Option(
        False, "--submit", help="Run orchestrator after writing scripts"
    ),
) -> None:
    """Generate chained SLURM array jobs for family OGs: MAFFT --auto -> trimAl -> IQ-TREE."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)
    tools = load_tools(project_dir)

    # Verify family exists
    conn = connect(paths.db_path)
    try:
        family_row = conn.execute(
            "SELECT * FROM families WHERE family_id = ?", (family_id,)
        ).fetchone()
    finally:
        conn.close()
    if family_row is None:
        raise typer.BadParameter(f"Family not found: {family_id!r}")

    # Resolve input directory
    if input_dir is not None:
        og_dir = input_dir.expanduser().resolve()
    else:
        og_dir = _find_og_input_dir(paths, family_id)
    typer.echo(f"Using OG FASTAs from: {og_dir}")

    if not og_dir.is_dir():
        raise typer.BadParameter(f"Input directory does not exist: {og_dir}")

    og_files = sorted(og_dir.glob("*.fa"))
    if not og_files:
        raise typer.BadParameter(f"No .fa files in {og_dir}")

    # Validate outgroup arguments: both or neither.
    if (outgroup_fasta is None) != (outgroup_map is None):
        raise typer.BadParameter(
            "--outgroup-fasta and --outgroup-map must be provided together."
        )

    outgroup_records: dict[str, FastaRecord] = {}
    outgroup_mapping: dict[str, list[str]] = {}
    if outgroup_fasta is not None and outgroup_map is not None:
        outgroup_fasta = outgroup_fasta.expanduser().resolve()
        outgroup_map = outgroup_map.expanduser().resolve()
        if not outgroup_fasta.exists():
            raise typer.BadParameter(f"Outgroup FASTA not found: {outgroup_fasta}")
        if not outgroup_map.exists():
            raise typer.BadParameter(f"Outgroup map TSV not found: {outgroup_map}")
        outgroup_records = _load_outgroup_fasta(outgroup_fasta)
        outgroup_mapping = _load_outgroup_map(outgroup_map)

    n_ogs = len(og_files)

    # Account
    acct = resolve_account(project_dir, account, no_confirm)

    # Defaults
    rid = output_run_id or f"phylo_{family_id}_{now_tag()}"
    max_concurrent = max_concurrent if max_concurrent is not None else 100
    max_array_size = max_array_size if max_array_size is not None else 380

    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    output_root = run_root / "gene_trees"
    logs_dir = paths.logs_dir / "slurm"

    slurm_dir.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Preprocess outgroups: write augmented FASTAs into the run dir,
    # substitute paths in the filelist, and emit the per-OG tips TSV.
    outgroup_tips_path: Path | None = None
    tips_per_og: dict[str, list[str]] = {}
    if outgroup_mapping:
        augmented_dir = run_root / "augmented_input"
        og_files, tips_per_og = _prepare_outgrouped_inputs(
            og_files, outgroup_records, outgroup_mapping, augmented_dir
        )
        outgroup_tips_path = slurm_dir / "outgroup_tips.tsv"
        _write_outgroup_tips_tsv(tips_per_og, outgroup_tips_path)

    # Write file list
    filelist_path = slurm_dir / "og_filelist.txt"
    filelist_path.write_text(
        "\n".join(str(f) for f in og_files) + "\n", encoding="utf-8"
    )

    bin_export = bin_dir_export_lines([
        tools.mafft.bin_dir,
        tools.trimal.bin_dir,
        tools.iqtree.bin_dir,
    ])

    worker_path = slurm_dir / "phylo_worker.sbatch"
    orchestrator_path = slurm_dir / "phylo_orchestrate.sh"

    worker_script = _render_worker(
        rid=rid,
        logs_dir=logs_dir,
        partition=partition,
        output_root=output_root,
        bin_export=bin_export,
        mafft_cmd=tools.mafft.command,
        trimal_cmd=tools.trimal.command,
        iqtree_cmd=tools.iqtree.command,
        trimal_gt=trimal_gt,
        trimal_cons=trimal_cons,
        iqtree_model=iqtree_model,
        iqtree_bootstrap=iqtree_bootstrap,
        iqtree_alrt=iqtree_alrt,
        iqtree_fast=iqtree_fast,
        outgroup_tips_path=outgroup_tips_path,
    )

    orchestrator_script = _render_orchestrator(
        filelist_path=filelist_path,
        output_root=output_root,
        worker_script=worker_path,
        max_array_size=max_array_size,
        max_concurrent=max_concurrent,
        acct=acct,
        align_time=align_time,
        align_cpus=align_cpus,
        align_mem_per_cpu=align_mem_per_cpu,
        trim_time=trim_time,
        trim_cpus=trim_cpus,
        trim_mem_per_cpu=trim_mem_per_cpu,
        tree_time=tree_time,
        tree_cpus=tree_cpus,
        tree_mem_per_cpu=tree_mem_per_cpu,
    )

    worker_path.write_text(worker_script, encoding="utf-8")
    worker_path.chmod(0o755)
    orchestrator_path.write_text(orchestrator_script, encoding="utf-8")
    orchestrator_path.chmod(0o755)

    # Manifest + DB
    created_at = now_iso()
    manifest_data = {
        "run_id": rid,
        "kind": "family_phylo",
        "created_at": created_at,
        "family_id": family_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "orchestrator": str(orchestrator_path.relative_to(project_dir)),
            "worker": str(worker_path.relative_to(project_dir)),
            "filelist_path": str(filelist_path.relative_to(project_dir)),
            "output_root": str(output_root.relative_to(project_dir)),
            "input_dir": str(og_dir),
            "logs_dir": str(logs_dir.relative_to(project_dir)),
        },
        "parameters": {
            "trimal_gt": trimal_gt,
            "trimal_cons": trimal_cons,
            "iqtree_model": iqtree_model,
            "iqtree_bootstrap": iqtree_bootstrap,
            "iqtree_alrt": iqtree_alrt,
            "iqtree_fast": iqtree_fast,
            "mafft_mode": "auto",
            "outgroup_fasta": str(outgroup_fasta) if outgroup_fasta else None,
            "outgroup_map": str(outgroup_map) if outgroup_map else None,
            "n_outgrouped_ogs": len(tips_per_og),
        },
        "slurm": {
            "account": acct,
            "partition": partition,
            "max_concurrent": max_concurrent,
            "max_array_size": max_array_size,
            "total_ogs": n_ogs,
            "align": {
                "time": align_time,
                "cpus": align_cpus,
                "mem_per_cpu": align_mem_per_cpu,
            },
            "trim": {
                "time": trim_time,
                "cpus": trim_cpus,
                "mem_per_cpu": trim_mem_per_cpu,
            },
            "tree": {
                "time": tree_time,
                "cpus": tree_cpus,
                "mem_per_cpu": tree_mem_per_cpu,
            },
            "submit": submit,
        },
    }
    register_run(paths, project_dir, rid, "family_phylo", created_at, manifest_data)

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "protsetphylo_phylo_write",
            "family_id": family_id,
            "run_id": rid,
            "n_ogs": n_ogs,
            "orchestrator": str(orchestrator_path),
            "worker": str(worker_path),
            "account": acct,
            "submit": submit,
        },
    )

    typer.echo(f"Wrote family phylo SLURM scripts:")
    typer.echo(f"  Family:       {family_id}")
    typer.echo(f"  Orchestrator: {orchestrator_path}")
    typer.echo(f"  Worker:       {worker_path}")
    typer.echo(f"  Orthogroups:  {n_ogs} total (max {max_array_size} per submission)")
    if tips_per_og:
        typer.echo(f"  Outgrouped:   {len(tips_per_og)} OGs (augmented FASTAs in {run_root / 'augmented_input'})")
    typer.echo(f"  Output:       {output_root}")
    typer.echo(f"  Steps:        align ({align_time}, {align_cpus}cpu, {align_mem_per_cpu}/cpu)")
    typer.echo(f"                trim  ({trim_time}, {trim_cpus}cpu, {trim_mem_per_cpu}/cpu)")
    typer.echo(f"                tree  ({tree_time}, {tree_cpus}cpu, {tree_mem_per_cpu}/cpu)")
    typer.echo("")
    typer.echo("To run: bash " + str(orchestrator_path))
    typer.echo("Rerun the orchestrator after completion to process remaining OGs.")

    if submit:
        stdout = submit_sbatch(orchestrator_path, use_bash=True)
        typer.echo(stdout or "Submitted.")
        log_event(
            project_dir,
            {
                "ts": now_iso(),
                "event": "protsetphylo_phylo_submit",
                "family_id": family_id,
                "run_id": rid,
                "orchestrator": str(orchestrator_path),
                "sbatch_stdout": stdout,
            },
        )
