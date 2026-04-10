from __future__ import annotations

import json
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import register_run, resolve_account, submit_sbatch
from fungalphylo.core.tools import bin_dir_export_lines, load_tools
from fungalphylo.db.db import init_db

app = typer.Typer(
    help="Generate a SLURM array job: MAFFT -> trimAl -> IQ-TREE per orthogroup."
)


def _render_orchestrator(
    *,
    filelist_path: Path,
    output_root: Path,
    worker_script: Path,
    max_array_size: int,
    max_concurrent: int,
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

echo "Submitting array of $NPENDING tasks..."
jobid=$(sbatch --parsable --array="1-${{NPENDING}}%${{MAX_CONCURRENT}}" "$WORKER" "$PENDING_LIST")
echo "Submitted job $jobid (array 1-${{NPENDING}}%${{MAX_CONCURRENT}})"
echo "Rerun this script after completion to process any remaining OGs."
"""


def _render_worker(
    *,
    acct: str,
    rid: str,
    logs_dir: Path,
    time: str,
    cpus: int,
    mem_per_cpu: str,
    partition: str,
    output_root: Path,
    bin_export: str,
    mafft_cmd: str,
    trimal_cmd: str,
    iqtree_cmd: str,
    mafft_retree: int,
    mafft_maxiterate: int,
    trimal_gt: float,
    trimal_cons: float,
    iqtree_model: str,
    iqtree_bootstrap: int,
    iqtree_alrt: int,
) -> str:
    return f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=phylo_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%A_%a.out
#SBATCH --error={logs_dir.as_posix()}/%x_%A_%a.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --partition={partition}

set -euo pipefail

module load mafft

{bin_export}
THREADS="${{SLURM_CPUS_PER_TASK:-1}}"

# Read the OG file for this array task
FILELIST="$1"
OG_FASTA=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" "$FILELIST")
OG_NAME=$(basename "$OG_FASTA" .fa)

OUTDIR="{output_root.as_posix()}/$OG_NAME"
mkdir -p "$OUTDIR"

ALIGNED="$OUTDIR/$OG_NAME.aligned.faa"
TRIMMED="$OUTDIR/$OG_NAME.trimmed.faa"

echo "=== Array task $SLURM_ARRAY_TASK_ID: $OG_NAME ==="
echo "Input:   $OG_FASTA"
echo "Output:  $OUTDIR"
echo "Threads: $THREADS"

# Step 1: MAFFT alignment (skip if output exists)
if [ ! -s "$ALIGNED" ]; then
    echo "--- MAFFT ---"
    "{mafft_cmd}" --retree {mafft_retree} --maxiterate {mafft_maxiterate} \\
      --thread "$THREADS" "$OG_FASTA" > "$ALIGNED"
    if [ ! -s "$ALIGNED" ]; then
        echo "ERROR: MAFFT produced empty alignment for $OG_NAME" >&2
        exit 1
    fi
else
    echo "Alignment exists, skipping MAFFT."
fi

# Step 2: trimAl (skip if output exists)
if [ ! -s "$TRIMMED" ]; then
    echo "--- trimAl ---"
    "{trimal_cmd}" -in "$ALIGNED" -out "$TRIMMED" \\
      -gt {trimal_gt} -cons {trimal_cons}
    if [ ! -s "$TRIMMED" ]; then
        echo "ERROR: trimAl produced empty output for $OG_NAME" >&2
        exit 1
    fi
else
    echo "Trimmed alignment exists, skipping trimAl."
fi

# Step 3: IQ-TREE (skip if treefile exists)
if [ ! -s "$OUTDIR/$OG_NAME.treefile" ]; then
    echo "--- IQ-TREE ---"
    "{iqtree_cmd}" -s "$TRIMMED" \\
      -m {iqtree_model} -B {iqtree_bootstrap} -alrt {iqtree_alrt} \\
      -T "$THREADS" --prefix "$OUTDIR/$OG_NAME"
else
    echo "Tree exists, skipping IQ-TREE."
fi

echo "=== Done: $OG_NAME ==="
"""


@app.callback(invoke_without_command=True)
def phylo_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(
        ..., help="Project directory."
    ),
    run_id: str | None = typer.Option(
        None, "--run-id",
        help="OrthoFinder run ID to read filtered orthogroups from.",
    ),
    input_dir: Path | None = typer.Option(
        None, "--input-dir",
        help="Explicit directory of .fa orthogroup files (overrides --run-id).",
    ),
    output_run_id: str | None = typer.Option(
        None, "--output-run-id",
        help="Run ID for this phylo run (default: phylo_<timestamp>).",
    ),
    time: str | None = typer.Option(None, "--time", help="SLURM time per task (default: 12:00:00)"),
    cpus: int | None = typer.Option(None, "--cpus", help="CPUs per task (default: 8)"),
    mem_per_cpu: str | None = typer.Option(
        None, "--mem-per-cpu", help="Memory per CPU (default: 2G)"
    ),
    partition: str | None = typer.Option(None, "--partition", help="SLURM partition"),
    max_concurrent: int | None = typer.Option(
        None, "--max-concurrent", help="Max concurrent array tasks (default: 100)"
    ),
    max_array_size: int | None = typer.Option(
        None, "--max-array-size", help="Max array tasks per submission (default: 380)"
    ),
    account: str | None = typer.Option(
        None, "--account", help="SLURM account (overrides auto-detect)"
    ),
    no_confirm: bool = typer.Option(
        False, "--no-confirm", help="Do not prompt to confirm detected account"
    ),
    mafft_retree: int = typer.Option(2, "--mafft-retree", help="MAFFT --retree value"),
    mafft_maxiterate: int = typer.Option(
        1000, "--mafft-maxiterate", help="MAFFT --maxiterate value"
    ),
    trimal_gt: float = typer.Option(0.8, "--trimal-gt", help="trimAl gap threshold"),
    trimal_cons: float = typer.Option(10.0, "--trimal-cons", help="trimAl conservation threshold"),
    iqtree_model: str = typer.Option("TEST", "--iqtree-model", help="IQ-TREE model selection"),
    iqtree_bootstrap: int = typer.Option(
        1000, "--iqtree-bootstrap", help="IQ-TREE ultrafast bootstrap replicates"
    ),
    iqtree_alrt: int = typer.Option(1000, "--iqtree-alrt", help="IQ-TREE SH-aLRT replicates"),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    tools = load_tools(project_dir)

    # Resolve input directory of OG .fa files
    if input_dir is not None:
        og_dir = input_dir.expanduser().resolve()
        source_run_id = None
    elif run_id is not None:
        og_dir = paths.run_dir(run_id) / "filtered_orthogroups"
        source_run_id = run_id
    else:
        # Find latest orthofinder run with filtered_orthogroups
        candidates = []
        for manifest_path in paths.runs_root.glob("*/manifest.json"):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                if data.get("kind") == "orthofinder":
                    filtered = manifest_path.parent / "filtered_orthogroups"
                    if filtered.is_dir() and any(filtered.glob("*.fa")):
                        candidates.append((data.get("created_at", ""), data["run_id"]))
            except (json.JSONDecodeError, KeyError):
                continue
        if not candidates:
            raise typer.BadParameter(
                "No OrthoFinder runs with filtered orthogroups found. "
                "Run `filter-orthogroups` first, or provide --run-id / --input-dir."
            )
        candidates.sort(reverse=True)
        source_run_id = candidates[0][1]
        og_dir = paths.run_dir(source_run_id) / "filtered_orthogroups"
        typer.echo(f"Using filtered OGs from run: {source_run_id}")

    if not og_dir.is_dir():
        raise typer.BadParameter(f"Input directory does not exist: {og_dir}")

    og_files = sorted(og_dir.glob("*.fa"))
    if not og_files:
        raise typer.BadParameter(f"No .fa files in {og_dir}")

    n_ogs = len(og_files)

    # Account
    acct = resolve_account(project_dir, account, no_confirm)

    # Defaults
    rid = output_run_id or f"phylo_{now_tag()}"
    time = time or "12:00:00"
    cpus = cpus if cpus is not None else 8
    mem_per_cpu = mem_per_cpu or "2G"
    partition = partition or "small"
    max_concurrent = max_concurrent if max_concurrent is not None else 100
    max_array_size = max_array_size if max_array_size is not None else 380

    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    output_root = run_root / "gene_trees"
    logs_dir = paths.logs_dir / "slurm"

    slurm_dir.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Write file list (one OG FASTA path per line)
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
        acct=acct,
        rid=rid,
        logs_dir=logs_dir,
        time=time,
        cpus=cpus,
        mem_per_cpu=mem_per_cpu,
        partition=partition,
        output_root=output_root,
        bin_export=bin_export,
        mafft_cmd=tools.mafft.command,
        trimal_cmd=tools.trimal.command,
        iqtree_cmd=tools.iqtree.command,
        mafft_retree=mafft_retree,
        mafft_maxiterate=mafft_maxiterate,
        trimal_gt=trimal_gt,
        trimal_cons=trimal_cons,
        iqtree_model=iqtree_model,
        iqtree_bootstrap=iqtree_bootstrap,
        iqtree_alrt=iqtree_alrt,
    )

    orchestrator_script = _render_orchestrator(
        filelist_path=filelist_path,
        output_root=output_root,
        worker_script=worker_path,
        max_array_size=max_array_size,
        max_concurrent=max_concurrent,
    )

    worker_path.write_text(worker_script, encoding="utf-8")
    worker_path.chmod(0o755)
    orchestrator_path.write_text(orchestrator_script, encoding="utf-8")
    orchestrator_path.chmod(0o755)

    # Manifest + DB
    created_at = now_iso()
    manifest_data = {
        "run_id": rid,
        "kind": "phylo",
        "created_at": created_at,
        "source_run_id": source_run_id,
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
            "mafft_retree": mafft_retree,
            "mafft_maxiterate": mafft_maxiterate,
            "trimal_gt": trimal_gt,
            "trimal_cons": trimal_cons,
            "iqtree_model": iqtree_model,
            "iqtree_bootstrap": iqtree_bootstrap,
            "iqtree_alrt": iqtree_alrt,
        },
        "slurm": {
            "account": acct,
            "partition": partition,
            "time": time,
            "cpus": cpus,
            "mem_per_cpu": mem_per_cpu,
            "max_concurrent": max_concurrent,
            "max_array_size": max_array_size,
            "total_ogs": n_ogs,
            "submit": submit,
        },
    }
    register_run(paths, project_dir, rid, "phylo", created_at, manifest_data)

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "slurm_phylo_write",
            "run_id": rid,
            "source_run_id": source_run_id,
            "input_dir": str(og_dir),
            "n_ogs": n_ogs,
            "orchestrator": str(orchestrator_path),
            "worker": str(worker_path),
            "account": acct,
            "submit": submit,
        },
    )

    typer.echo(f"Wrote phylo SLURM scripts:")
    typer.echo(f"  Orchestrator: {orchestrator_path}")
    typer.echo(f"  Worker:       {worker_path}")
    typer.echo(f"  Orthogroups:  {n_ogs} total (max {max_array_size} per submission)")
    typer.echo(f"  Output:       {output_root}")
    typer.echo(f"  File list:    {filelist_path}")
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
                "event": "slurm_phylo_submit",
                "run_id": rid,
                "orchestrator": str(orchestrator_path),
                "sbatch_stdout": stdout,
            },
        )
