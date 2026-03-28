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

app = typer.Typer(
    help="Generate a SLURM array job: MAFFT → trimAl → IQ-TREE per orthogroup."
)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _render_phylo_script(
    *,
    acct: str,
    rid: str,
    logs_dir: Path,
    time: str,
    cpus: int,
    mem_per_cpu: str,
    partition: str,
    max_concurrent: int,
    n_ogs: int,
    filelist_path: Path,
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
    array_spec = f"0-{n_ogs - 1}%{max_concurrent}"

    return f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=phylo_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%A_%a.out
#SBATCH --error={logs_dir.as_posix()}/%x_%A_%a.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --partition={partition}
#SBATCH --array={array_spec}

set -euo pipefail

module load mafft

{bin_export}
THREADS="${{SLURM_CPUS_PER_TASK:-1}}"

# Read the OG file for this array task
FILELIST="{filelist_path.as_posix()}"
OG_FASTA=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$FILELIST")
OG_NAME=$(basename "$OG_FASTA" .fa)

OUTDIR="{output_root.as_posix()}/$OG_NAME"
mkdir -p "$OUTDIR"

ALIGNED="$OUTDIR/$OG_NAME.aligned.faa"
TRIMMED="$OUTDIR/$OG_NAME.trimmed.faa"

echo "=== Array task $SLURM_ARRAY_TASK_ID: $OG_NAME ==="
echo "Input:   $OG_FASTA"
echo "Output:  $OUTDIR"
echo "Threads: $THREADS"

# Step 1: MAFFT alignment
echo "--- MAFFT ---"
"{mafft_cmd}" --retree {mafft_retree} --maxiterate {mafft_maxiterate} \\
  --thread "$THREADS" "$OG_FASTA" > "$ALIGNED"

if [ ! -s "$ALIGNED" ]; then
  echo "ERROR: MAFFT produced empty alignment for $OG_NAME" >&2
  exit 1
fi

# Step 2: trimAl
echo "--- trimAl ---"
"{trimal_cmd}" -in "$ALIGNED" -out "$TRIMMED" \\
  -gt {trimal_gt} -cons {trimal_cons}

if [ ! -s "$TRIMMED" ]; then
  echo "ERROR: trimAl produced empty output for $OG_NAME" >&2
  exit 1
fi

# Step 3: IQ-TREE
echo "--- IQ-TREE ---"
"{iqtree_cmd}" -s "$TRIMMED" \\
  -m {iqtree_model} -B {iqtree_bootstrap} -alrt {iqtree_alrt} \\
  -T "$THREADS" --prefix "$OUTDIR/$OG_NAME"

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
    cpus: int | None = typer.Option(None, "--cpus", help="CPUs per task (default: 16)"),
    mem_per_cpu: str | None = typer.Option(
        None, "--mem-per-cpu", help="Memory per CPU (default: 2G)"
    ),
    partition: str | None = typer.Option(None, "--partition", help="SLURM partition"),
    max_concurrent: int | None = typer.Option(
        None, "--max-concurrent", help="Max concurrent array tasks (default: 380)"
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
    inferred = infer_account_from_project_dir(project_dir)
    acct = account or inferred
    if not acct:
        raise typer.BadParameter(
            "Could not infer SLURM account from project_dir. Provide --account explicitly."
        )
    if not no_confirm and account is None:
        ok = typer.confirm(
            f"Detected SLURM account '{acct}' from project_dir. Use this account?",
            default=True,
        )
        if not ok:
            raise typer.BadParameter(
                "Account not confirmed. Re-run with --account <account> or --no-confirm."
            )

    # Defaults
    rid = output_run_id or f"phylo_{now_tag()}"
    time = time or "12:00:00"
    cpus = cpus if cpus is not None else 16
    mem_per_cpu = mem_per_cpu or "2G"
    partition = partition or "small"
    max_concurrent = max_concurrent if max_concurrent is not None else 380

    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    output_root = run_root / "gene_trees"
    logs_dir = paths.logs_dir / "slurm"

    _ensure_dir(slurm_dir)
    _ensure_dir(output_root)
    _ensure_dir(logs_dir)

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

    script = _render_phylo_script(
        acct=acct,
        rid=rid,
        logs_dir=logs_dir,
        time=time,
        cpus=cpus,
        mem_per_cpu=mem_per_cpu,
        partition=partition,
        max_concurrent=max_concurrent,
        n_ogs=n_ogs,
        filelist_path=filelist_path,
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

    script_path = slurm_dir / "phylo_array.sbatch"
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    # Manifest
    manifest_data = {
        "run_id": rid,
        "kind": "phylo",
        "created_at": now_iso(),
        "source_run_id": source_run_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "script_path": str(script_path.relative_to(project_dir)),
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
            "array_size": n_ogs,
            "submit": submit,
        },
    }
    manifest_path = paths.run_manifest(rid)
    write_manifest(manifest_path, manifest_data)
    manifest_sha256 = hash_json(manifest_data)

    # DB row — use __family__ sentinel since this isn't tied to a staging snapshot
    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?)",
            ("__family__", now_iso(), "__family__", "__family__"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?,?,?)",
            (
                rid,
                "__family__",
                "phylo",
                manifest_data["created_at"],
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
            "ts": now_iso(),
            "event": "slurm_phylo_write",
            "run_id": rid,
            "source_run_id": source_run_id,
            "input_dir": str(og_dir),
            "n_ogs": n_ogs,
            "script_path": str(script_path),
            "account": acct,
            "submit": submit,
        },
    )

    typer.echo(f"Wrote phylo array SLURM script: {script_path}")
    typer.echo(f"Orthogroups: {n_ogs} (array 0-{n_ogs - 1}%{max_concurrent})")
    typer.echo(f"Output:      {output_root}")
    typer.echo(f"File list:   {filelist_path}")

    if submit:
        try:
            res = subprocess.run(
                ["sbatch", str(script_path)], check=True, capture_output=True, text=True
            )
            typer.echo(res.stdout.strip() or "Submitted.")
            log_event(
                project_dir,
                {
                    "ts": now_iso(),
                    "event": "slurm_phylo_submit",
                    "run_id": rid,
                    "script_path": str(script_path),
                    "sbatch_stdout": res.stdout.strip(),
                },
            )
        except FileNotFoundError:
            raise RuntimeError(
                "sbatch not found on PATH. Submit manually with: sbatch <script>"
            ) from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}"
            ) from e
