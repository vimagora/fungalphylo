from __future__ import annotations

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


def tree_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Family to build tree for"),
    account: str | None = typer.Option(None, "--account", help="SLURM account"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Skip account confirmation"),
    run_id: str | None = typer.Option(None, "--run-id", help="Run identifier"),
    tree_method: str = typer.Option("iqtree", "--tree-method", help="Tree method: iqtree or fasttree"),
    model: str = typer.Option("MFP", "--model", help="Substitution model (IQ-TREE)"),
    bootstrap: int = typer.Option(1000, "--bootstrap", help="Bootstrap replicates (IQ-TREE -bb)"),
    input_alignment: str | None = typer.Option(None, "--input-alignment", help="Override input alignment path"),
    partition: str = typer.Option("small", "--partition", help="SLURM partition"),
    time: str = typer.Option("24:00:00", "--time", help="SLURM time limit"),
    cpus: int = typer.Option(8, "--cpus", help="CPUs per task"),
    mem: str = typer.Option("16G", "--mem", help="Memory"),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
) -> None:
    """Generate SLURM script for phylogenetic tree building."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)
    tools = load_tools(project_dir)

    if tree_method not in ("iqtree", "fasttree"):
        raise typer.BadParameter(f"--tree-method must be iqtree or fasttree. Got: {tree_method!r}")

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

    # Resolve input alignment
    alignment_dir = paths.family_alignment_dir(family_id)
    if input_alignment:
        trimmed_aln = Path(input_alignment).expanduser().resolve()
        if not trimmed_aln.exists():
            raise typer.BadParameter(f"Input alignment not found: {trimmed_aln}")
    else:
        trimmed_aln = alignment_dir / "combined.trimmed.aln"
        if not trimmed_aln.exists():
            raise typer.BadParameter(
                f"Trimmed alignment not found: {trimmed_aln}\n"
                "Run `protsetphylo align` first."
            )

    # Resolve account
    acct = account or infer_account_from_project_dir(project_dir)
    if not acct:
        raise typer.BadParameter("Could not infer SLURM account. Provide --account explicitly.")
    if not no_confirm and account is None:
        ok = typer.confirm(
            f"Detected SLURM account '{acct}' from project_dir. Use this account?",
            default=True,
        )
        if not ok:
            raise typer.BadParameter("Account not confirmed.")

    # Set up directories
    tree_dir = paths.family_tree_dir(family_id)
    tree_dir.mkdir(parents=True, exist_ok=True)

    rid = run_id or f"tree_{family_id}_{now_tag()}"
    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    logs_dir = project_dir / "logs" / "slurm"
    for d in (slurm_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    treefile = tree_dir / "combined.treefile"

    if tree_method == "iqtree":
        iqtree_cmd = tools.iqtree.command
        path_export = bin_dir_export_lines([tools.iqtree.bin_dir])
        module_lines = "" if tools.iqtree.bin_dir else "module load iqtree\n"
        tree_commands = f"""\
{module_lines}{path_export}
echo "Running IQ-TREE..."
{iqtree_cmd} \\
  -s "{trimmed_aln.as_posix()}" \\
  -m {model} \\
  -bb {bootstrap} \\
  -nt AUTO \\
  --prefix "{(tree_dir / 'combined').as_posix()}"

echo "Tree building complete."
"""
    else:  # fasttree
        fasttree_cmd = tools.fasttree.command
        path_export = bin_dir_export_lines([tools.fasttree.bin_dir])
        module_lines = "" if tools.fasttree.bin_dir else "module load fasttree\n"
        tree_commands = f"""\
{module_lines}{path_export}
echo "Running FastTree..."
{fasttree_cmd} -gamma < "{trimmed_aln.as_posix()}" > "{treefile.as_posix()}"

echo "Tree building complete."
"""

    script = f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=tree_{family_id}
#SBATCH --output={logs_dir.as_posix()}/tree_{rid}_%j.out
#SBATCH --error={logs_dir.as_posix()}/tree_{rid}_%j.err
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}

set -euo pipefail

{tree_commands}"""

    script_path = slurm_dir / "tree.sbatch"
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    # Write manifest
    created_at = now_iso()
    manifest_data = {
        "run_id": rid,
        "kind": "family_tree",
        "created_at": created_at,
        "family_id": family_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "script": str(script_path.relative_to(project_dir)),
            "input_alignment": str(trimmed_aln.relative_to(project_dir)),
            "treefile": str(treefile.relative_to(project_dir)),
        },
        "tree": {
            "method": tree_method,
            "model": model if tree_method == "iqtree" else None,
            "bootstrap": bootstrap if tree_method == "iqtree" else None,
        },
        "slurm": {
            "account": acct,
            "partition": partition,
            "time": time,
            "cpus": cpus,
            "mem": mem,
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
            (rid, "__family__", "family_tree", created_at, str(manifest_path.relative_to(project_dir)), manifest_sha256),
        )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "protsetphylo_tree_write",
            "family_id": family_id,
            "run_id": rid,
            "tree_method": tree_method,
            "script": str(script_path),
            "submit": submit,
        },
    )

    typer.echo(f"Wrote tree script: {script_path}")
    typer.echo(f"  Family:   {family_id}")
    typer.echo(f"  Run ID:   {rid}")
    typer.echo(f"  Method:   {tree_method}")
    typer.echo(f"  Output:   {treefile}")

    if submit:
        try:
            res = subprocess.run(
                ["sbatch", str(script_path)], check=True, capture_output=True, text=True
            )
            typer.echo(res.stdout.strip() or "Submitted.")
        except FileNotFoundError:
            raise RuntimeError("sbatch not found on PATH. Submit manually.") from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}") from e
