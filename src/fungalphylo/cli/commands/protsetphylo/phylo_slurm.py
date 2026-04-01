from __future__ import annotations

import json
import subprocess
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.hash import hash_json
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import infer_account_from_project_dir
from fungalphylo.core.tools import bin_dir_export_lines, load_tools
from fungalphylo.db.db import connect, init_db


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


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
) -> str:
    fast_flag = " -fast" if iqtree_fast else ""
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
            "{iqtree_cmd}" -s "$TRIMMED" \\
              -m {iqtree_model} -B {iqtree_bootstrap} -alrt {iqtree_alrt} \\
              -T "$THREADS"{fast_flag} --prefix "$OUTDIR/$OG_NAME"
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

    n_ogs = len(og_files)

    # Account
    acct = account or infer_account_from_project_dir(project_dir)
    if not acct:
        raise typer.BadParameter(
            "Could not infer SLURM account. Provide --account explicitly."
        )
    if not no_confirm and account is None:
        ok = typer.confirm(
            f"Detected SLURM account '{acct}' from project_dir. Use this account?",
            default=True,
        )
        if not ok:
            raise typer.BadParameter("Account not confirmed.")

    # Defaults
    rid = output_run_id or f"phylo_{family_id}_{now_tag()}"
    max_concurrent = max_concurrent if max_concurrent is not None else 100
    max_array_size = max_array_size if max_array_size is not None else 380

    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    output_root = run_root / "gene_trees"
    logs_dir = paths.logs_dir / "slurm"

    _ensure_dir(slurm_dir)
    _ensure_dir(output_root)
    _ensure_dir(logs_dir)

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

    # Manifest
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
    manifest_path = paths.run_manifest(rid)
    write_manifest(manifest_path, manifest_data)
    manifest_sha256 = hash_json(manifest_data)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) "
            "VALUES('__family__', ?, '__family__', '__family__')",
            (created_at,),
        )
        conn.execute(
            "INSERT OR REPLACE INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?,?,?)",
            (
                rid,
                "__family__",
                "family_phylo",
                created_at,
                str(manifest_path.relative_to(project_dir)),
                manifest_sha256,
            ),
        )
        conn.commit()
    finally:
        conn.close()

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
    typer.echo(f"  Output:       {output_root}")
    typer.echo(f"  Steps:        align ({align_time}, {align_cpus}cpu, {align_mem_per_cpu}/cpu)")
    typer.echo(f"                trim  ({trim_time}, {trim_cpus}cpu, {trim_mem_per_cpu}/cpu)")
    typer.echo(f"                tree  ({tree_time}, {tree_cpus}cpu, {tree_mem_per_cpu}/cpu)")
    typer.echo("")
    typer.echo("To run: bash " + str(orchestrator_path))
    typer.echo("Rerun the orchestrator after completion to process remaining OGs.")

    if submit:
        try:
            res = subprocess.run(
                ["bash", str(orchestrator_path)], check=True, capture_output=True, text=True
            )
            typer.echo(res.stdout.strip() or "Submitted.")
            log_event(
                project_dir,
                {
                    "ts": now_iso(),
                    "event": "protsetphylo_phylo_submit",
                    "family_id": family_id,
                    "run_id": rid,
                    "orchestrator": str(orchestrator_path),
                    "sbatch_stdout": res.stdout.strip(),
                },
            )
        except FileNotFoundError:
            raise RuntimeError("bash not found on PATH.") from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Orchestrator failed: {e.stderr.strip() if e.stderr else str(e)}"
            ) from e
