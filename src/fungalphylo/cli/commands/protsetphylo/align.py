from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.cli.commands.busco_slurm import infer_account_from_project_dir
from fungalphylo.core.events import log_event
from fungalphylo.core.hash import hash_json
from fungalphylo.core.manifest import read_manifest, write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def align_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Family to align"),
    account: Optional[str] = typer.Option(None, "--account", help="SLURM account"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Skip account confirmation"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier"),
    partition: str = typer.Option("small", "--partition", help="SLURM partition"),
    time: str = typer.Option("12:00:00", "--time", help="SLURM time limit"),
    cpus: int = typer.Option(8, "--cpus", help="CPUs per task"),
    mem: str = typer.Option("16G", "--mem", help="Memory"),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
) -> None:
    """Generate SLURM script for MAFFT alignment + trimAl trimming."""
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

    # Verify combined FASTA exists
    fasta_dir = paths.family_fasta_dir(family_id)
    combined_fasta = fasta_dir / "combined.faa"
    if not combined_fasta.exists():
        raise typer.BadParameter(
            f"Combined FASTA not found: {combined_fasta}\n"
            "Run `protsetphylo build-fasta` first."
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
    alignment_dir = paths.family_alignment_dir(family_id)
    alignment_dir.mkdir(parents=True, exist_ok=True)

    rid = run_id or f"align_{family_id}_{_now_tag()}"
    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    logs_dir = project_dir / "logs" / "slurm"
    for d in (slurm_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    mafft_cmd = tools.mafft.command
    trimal_cmd = tools.trimal.command

    aln_output = alignment_dir / "combined.aln"
    trimmed_output = alignment_dir / "combined.trimmed.aln"

    script = f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=align_{family_id}
#SBATCH --output={logs_dir.as_posix()}/align_{rid}_%j.out
#SBATCH --error={logs_dir.as_posix()}/align_{rid}_%j.err
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}

set -euo pipefail

module load mafft

echo "Running MAFFT alignment..."
{mafft_cmd} --auto --thread $SLURM_CPUS_PER_TASK \\
  "{combined_fasta.as_posix()}" \\
  > "{aln_output.as_posix()}"

echo "Running trimAl..."
{trimal_cmd} \\
  -in "{aln_output.as_posix()}" \\
  -out "{trimmed_output.as_posix()}" \\
  -automated1

echo "Alignment complete."
"""

    script_path = slurm_dir / "align.sbatch"
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    # Write manifest
    created_at = _now_iso()
    manifest_data = {
        "run_id": rid,
        "kind": "family_align",
        "created_at": created_at,
        "family_id": family_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "script": str(script_path.relative_to(project_dir)),
            "input_fasta": str(combined_fasta.relative_to(project_dir)),
            "alignment": str(aln_output.relative_to(project_dir)),
            "trimmed_alignment": str(trimmed_output.relative_to(project_dir)),
        },
        "tools": {
            "mafft": mafft_cmd,
            "trimal": trimal_cmd,
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
            (rid, "__family__", "family_align", created_at, str(manifest_path.relative_to(project_dir)), manifest_sha256),
        )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "protsetphylo_align_write",
            "family_id": family_id,
            "run_id": rid,
            "script": str(script_path),
            "submit": submit,
        },
    )

    typer.echo(f"Wrote alignment script: {script_path}")
    typer.echo(f"  Family:   {family_id}")
    typer.echo(f"  Run ID:   {rid}")
    typer.echo(f"  Output:   {aln_output}")
    typer.echo(f"  Trimmed:  {trimmed_output}")

    if submit:
        try:
            res = subprocess.run(
                ["sbatch", str(script_path)], check=True, capture_output=True, text=True
            )
            typer.echo(res.stdout.strip() or "Submitted.")
        except FileNotFoundError:
            raise RuntimeError("sbatch not found on PATH. Submit manually.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}")
