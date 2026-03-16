from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.hash import hash_json
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def shlex_quote(text: str) -> str:
    return "'" + text.replace("'", "'\"'\"'") + "'"


def interproscan_command(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    family_id: str = typer.Option(..., "--family-id", help="Family to run InterProScan on"),
    account: Optional[str] = typer.Option(None, "--account", help="SLURM account"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Skip account confirmation"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier"),
    application: list[str] = typer.Option(
        ["Pfam"],
        "--application",
        help="InterProScan application (repeatable).",
    ),
    fmt: list[str] = typer.Option(["TSV"], "--format", help="InterProScan output format."),
    partition: str = typer.Option("small", "--partition", help="SLURM partition"),
    time: str = typer.Option("12:00:00", "--time", help="SLURM time limit"),
    cpus: int = typer.Option(4, "--cpus", help="CPUs per task"),
    mem: str = typer.Option("8G", "--mem", help="Memory"),
    interproscan_bin_dir: Optional[Path] = typer.Option(
        None, "--interproscan-bin-dir", help="Override InterProScan bin dir"
    ),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
) -> None:
    """Run InterProScan on a family's characterized proteins."""
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)
    tools = load_tools(project_dir)

    # Verify family exists
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT * FROM families WHERE family_id = ?", (family_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise typer.BadParameter(f"Family not found: {family_id!r}")

    char_fasta = paths.family_characterized_dir(family_id) / "characterized.faa"
    if not char_fasta.exists():
        raise typer.BadParameter(f"Characterized FASTA not found: {char_fasta}")

    # Resolve account
    from fungalphylo.cli.commands.busco_slurm import infer_account_from_project_dir

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

    bin_dir = (
        interproscan_bin_dir.expanduser().resolve()
        if interproscan_bin_dir
        else tools.interproscan.bin_dir
    )
    interproscan_cmd = tools.interproscan.command or "cluster_interproscan"

    applications = list(dict.fromkeys([a.strip() for a in application if a.strip()])) or ["Pfam"]
    format_aliases = {"tsv": "TSV", "xml": "XML", "gff3": "GFF3"}
    formats = list(
        dict.fromkeys(
            [format_aliases.get(f.strip().lower(), f.strip()) for f in fmt if f.strip()]
        )
    ) or ["TSV"]

    rid = run_id or f"family_ipr_{family_id}_{_now_tag()}"

    # Set up run directories
    ipr_output_dir = paths.family_characterized_dir(family_id) / "interproscan"
    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    logs_dir = project_dir / "logs" / "slurm"
    output_tsv = ipr_output_dir / "characterized.tsv"

    for d in (ipr_output_dir, slurm_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    app_args = " ".join(f"-appl {shlex_quote(a)}" for a in applications)
    fmt_args = " ".join(f"-f {shlex_quote(f)}" for f in formats)

    worker_script = f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=fipr_{family_id}
#SBATCH --output={logs_dir.as_posix()}/fipr_{rid}_%j.out
#SBATCH --error={logs_dir.as_posix()}/fipr_{rid}_%j.err
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}

set -euo pipefail

module load biokit
module load interproscan

"""
    if bin_dir is not None:
        worker_script += f'export PATH="{bin_dir.as_posix()}:$PATH"\n\n'

    worker_script += f"""\
if ! command -v "{interproscan_cmd}" >/dev/null 2>&1; then
  echo "ERROR: '{interproscan_cmd}' not found on PATH after module load." >&2
  exit 127
fi

"{interproscan_cmd}" \\
  -i "{char_fasta.as_posix()}" \\
  -o "{output_tsv.as_posix()}" \\
  -T "{ipr_output_dir.as_posix()}/tmp" \\
  {app_args} \\
  {fmt_args}
"""

    worker_path = slurm_dir / "family_interproscan.sbatch"
    worker_path.write_text(worker_script, encoding="utf-8")
    worker_path.chmod(0o755)

    # Write manifest and DB row
    created_at = _now_iso()
    manifest_data = {
        "run_id": rid,
        "kind": "family_interproscan",
        "created_at": created_at,
        "family_id": family_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "worker_script": str(worker_path.relative_to(project_dir)),
            "output_tsv": str(output_tsv.relative_to(project_dir)),
            "ipr_output_dir": str(ipr_output_dir.relative_to(project_dir)),
        },
        "interproscan": {
            "applications": applications,
            "formats": formats,
            "command": interproscan_cmd,
            "bin_dir": str(bin_dir) if bin_dir else None,
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

    # We need a staging_id for runs table — use a sentinel for family runs
    conn = connect(paths.db_path)
    try:
        # Ensure a sentinel staging exists for family runs
        conn.execute(
            """
            INSERT OR IGNORE INTO stagings(staging_id, created_at, manifest_path, manifest_sha256)
            VALUES('__family__', ?, '__family__', '__family__')
            """,
            (created_at,),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256)
            VALUES(?,?,?,?,?,?)
            """,
            (
                rid,
                "__family__",
                "family_interproscan",
                created_at,
                str(manifest_path.relative_to(project_dir)),
                manifest_sha256,
            ),
        )
        conn.execute(
            "UPDATE families SET ipr_run_id = ? WHERE family_id = ?",
            (rid, family_id),
        )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": created_at,
            "event": "protsetphylo_interproscan_write",
            "family_id": family_id,
            "run_id": rid,
            "worker_script": str(worker_path),
            "submit": submit,
        },
    )

    typer.echo(f"Wrote InterProScan script: {worker_path}")
    typer.echo(f"  Family:     {family_id}")
    typer.echo(f"  Run ID:     {rid}")
    typer.echo(f"  Output TSV: {output_tsv}")

    if submit:
        try:
            res = subprocess.run(
                ["sbatch", str(worker_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            typer.echo(res.stdout.strip() or "Submitted.")
        except FileNotFoundError:
            raise RuntimeError("sbatch not found on PATH. Submit manually.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}")
