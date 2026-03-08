from __future__ import annotations

import csv
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.cli.commands.busco_slurm import infer_account_from_project_dir
from fungalphylo.cli.commands.busco_slurm import resolve_staging_id
from fungalphylo.core.events import log_event
from fungalphylo.core.hash import hash_json
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db

app = typer.Typer(help="Generate launcher-based SLURM scripts for InterProScan on staged proteomes.")

SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_name(text: str) -> str:
    return SAFE_ID_RE.sub("_", text)[:200] or "proteome"


def _write_queue_tsv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(
            [
                "portal_id",
                "input_fasta",
                "status",
                "submitted_job_id",
                "results_dir",
                "tsv_path",
                "note",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["portal_id"],
                    row["input_fasta"],
                    row["status"],
                    row["submitted_job_id"],
                    row["results_dir"],
                    row["tsv_path"],
                    row["note"],
                ]
            )


@app.callback(invoke_without_command=True)
def interproscan_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(..., help="Project directory"),
    staging_id: Optional[str] = typer.Option(None, "--staging-id", help="Staging snapshot to use (default: latest)."),
    account: Optional[str] = typer.Option(None, "--account", help="SLURM account (overrides auto-detect)"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Do not prompt to confirm detected account"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier (default: interproscan_<timestamp>)"),
    application: list[str] = typer.Option(
        ["pfam"],
        "--application",
        help="InterProScan application to enable. Repeat the flag to request multiple applications.",
    ),
    fmt: list[str] = typer.Option(
        ["tsv"],
        "--format",
        help="InterProScan output format. Repeat the flag to request multiple formats.",
    ),
    partition: str = typer.Option("small", "--partition", help="SLURM partition for the launcher job"),
    time: str = typer.Option("48:00:00", "--time", help="SLURM time limit for the launcher job"),
    cpus: int = typer.Option(1, "--cpus", help="CPUs for the launcher job"),
    mem: str = typer.Option("1G", "--mem", help="Memory for the launcher job"),
    poll_seconds: int = typer.Option(300, "--poll-seconds", help="Polling interval between proteome submissions"),
    interproscan_bin_dir: Optional[Path] = typer.Option(
        None, "--interproscan-bin-dir", help="Override InterProScan bin dir (else uses tools.yaml: interproscan.bin_dir)"
    ),
    submit: bool = typer.Option(False, "--submit", help="Submit the launcher with sbatch after writing scripts"),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)
    selected_staging_id = resolve_staging_id(project_dir, staging_id)

    staged_proteomes = paths.staging_proteomes_dir(selected_staging_id)
    if not staged_proteomes.exists():
        raise typer.BadParameter(f"Missing staged proteomes dir for {selected_staging_id}: {staged_proteomes}")

    proteomes = sorted(staged_proteomes.glob("*.faa"))
    if not proteomes:
        raise typer.BadParameter(f"No staged proteome FASTA files found in {staged_proteomes}")

    inferred = infer_account_from_project_dir(project_dir)
    acct = account or inferred
    if not acct:
        raise typer.BadParameter(
            "Could not infer SLURM account from project_dir (expected /scratch/<account>/...). "
            "Provide --account explicitly."
        )
    if not no_confirm and account is None:
        ok = typer.confirm(f"Detected SLURM account '{acct}' from project_dir. Use this account?", default=True)
        if not ok:
            raise typer.BadParameter("Account not confirmed. Re-run with --account <account> or --no-confirm.")

    tools = load_tools(project_dir)
    bin_dir = interproscan_bin_dir.expanduser().resolve() if interproscan_bin_dir else tools.interproscan.bin_dir
    interproscan_cmd = tools.interproscan.command or "cluster_interproscan"
    if bin_dir is None or not bin_dir.exists():
        raise typer.BadParameter(
            "InterProScan path not configured or does not exist.\n"
            "Set tools.yaml:\n"
            "  interproscan:\n"
            "    bin_dir: /path/to/bin\n"
            "or pass --interproscan-bin-dir /path/to/bin"
        )

    applications = list(dict.fromkeys([a.strip() for a in application if a.strip()]))
    if not applications:
        applications = ["pfam"]
    formats = list(dict.fromkeys([f.strip() for f in fmt if f.strip()]))
    if not formats:
        formats = ["tsv"]
    if "tsv" not in formats:
        raise typer.BadParameter("At least one InterProScan output format must be 'tsv' for downstream parsing.")

    rid = run_id or f"interproscan_{_now_tag()}"
    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    scripts_dir = run_root / "scripts"
    inputs_dir = paths.run_inputs_dir(rid)
    work_dir = paths.run_work_dir(rid)
    results_root = run_root / "interproscan_results"
    logs_dir = project_dir / "logs" / "slurm"
    queue_path = run_root / "queue.tsv"
    launcher_path = slurm_dir / "interproscan_launcher.sbatch"
    worker_path = scripts_dir / "run_one_interproscan.sh"

    for d in (slurm_dir, scripts_dir, inputs_dir, work_dir, results_root, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    queue_rows = []
    for proteome in proteomes:
        portal_id = proteome.stem
        result_dir = results_root / portal_id
        queue_rows.append(
            {
                "portal_id": portal_id,
                "input_fasta": str(proteome),
                "status": "pending",
                "submitted_job_id": "",
                "results_dir": str(result_dir),
                "tsv_path": str(result_dir / f"{portal_id}.tsv"),
                "note": "",
            }
        )
    _write_queue_tsv(queue_path, queue_rows)

    app_args = " ".join(f"--applications {shlex_quote(a)}" for a in applications)
    fmt_args = " ".join(f"--formats {shlex_quote(f)}" for f in formats)

    worker_script = f"""#!/bin/bash
set -euo pipefail

PORTAL_ID="$1"
INPUT_FASTA="$2"
RESULT_DIR="$3"
WORK_DIR="$4"
IPR_CMD="$5"

mkdir -p "$RESULT_DIR" "$WORK_DIR"
cd "$RESULT_DIR"

export PATH="{bin_dir.as_posix()}:$PATH"

if ! command -v "$IPR_CMD" >/dev/null 2>&1; then
  echo "ERROR: '$IPR_CMD' not found on PATH after prepending {bin_dir.as_posix()}." >&2
  exit 127
fi

"$IPR_CMD" \\
  --input "$INPUT_FASTA" \\
  --output-dir "$RESULT_DIR" \\
  --tempdir "$WORK_DIR/$PORTAL_ID" \\
  {app_args} \\
  {fmt_args}
"""
    worker_path.write_text(worker_script, encoding="utf-8")
    worker_path.chmod(0o755)

    launcher_script = f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=ipr_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%j.out
#SBATCH --error={logs_dir.as_posix()}/%x_%j.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --partition={partition}

set -euo pipefail

QUEUE_TSV="{queue_path.as_posix()}"
WORKER="{worker_path.as_posix()}"
WORK_DIR="{work_dir.as_posix()}"
IPR_CMD="{interproscan_cmd}"
POLL_SECONDS="{poll_seconds}"

tail -n +2 "$QUEUE_TSV" | while IFS=$'\\t' read -r PORTAL_ID INPUT_FASTA STATUS JOB_ID RESULT_DIR TSV_PATH NOTE; do
  if [[ "$STATUS" == "completed" ]]; then
    continue
  fi
  bash "$WORKER" "$PORTAL_ID" "$INPUT_FASTA" "$RESULT_DIR" "$WORK_DIR" "$IPR_CMD"
  sleep "$POLL_SECONDS"
done
"""
    launcher_path.write_text(launcher_script, encoding="utf-8")

    manifest_data = {
        "run_id": rid,
        "kind": "interproscan",
        "created_at": _now_iso(),
        "staging_id": selected_staging_id,
        "project_dir": str(project_dir),
        "paths": {
            "run_dir": str(run_root.relative_to(project_dir)),
            "launcher_script": str(launcher_path.relative_to(project_dir)),
            "worker_script": str(worker_path.relative_to(project_dir)),
            "queue_tsv": str(queue_path.relative_to(project_dir)),
            "results_root": str(results_root.relative_to(project_dir)),
        },
        "interproscan": {
            "applications": applications,
            "formats": formats,
            "command": interproscan_cmd,
            "bin_dir": str(bin_dir),
            "poll_seconds": poll_seconds,
            "n_proteomes": len(queue_rows),
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
    manifest_path = paths.run_manifest(rid)
    write_manifest(manifest_path, manifest_data)
    manifest_sha256 = hash_json(manifest_data)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256)
            VALUES(?,?,?,?,?,?)
            """,
            (
                rid,
                selected_staging_id,
                "interproscan",
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
            "ts": manifest_data["created_at"],
            "event": "slurm_interproscan_write",
            "run_id": rid,
            "staging_id": selected_staging_id,
            "launcher_script": str(launcher_path),
            "worker_script": str(worker_path),
            "queue_tsv": str(queue_path),
            "applications": applications,
            "formats": formats,
            "submit": submit,
        },
    )

    typer.echo(f"Wrote InterProScan launcher: {launcher_path}")
    typer.echo(f"Wrote InterProScan worker:   {worker_path}")
    typer.echo(f"Wrote InterProScan queue:    {queue_path}")

    if submit:
        try:
            res = subprocess.run(["sbatch", str(launcher_path)], check=True, capture_output=True, text=True)
            typer.echo(res.stdout.strip() or "Submitted.")
            log_event(
                project_dir,
                {
                    "ts": _now_iso(),
                    "event": "slurm_interproscan_submit",
                    "run_id": rid,
                    "staging_id": selected_staging_id,
                    "launcher_script": str(launcher_path),
                    "sbatch_stdout": res.stdout.strip(),
                },
            )
        except FileNotFoundError:
            raise RuntimeError("sbatch not found on PATH. Submit manually with: sbatch <script>")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}")


def shlex_quote(text: str) -> str:
    return "'" + text.replace("'", "'\"'\"'") + "'"
