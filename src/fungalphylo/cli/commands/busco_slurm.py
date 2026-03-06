from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.tools import load_tools

app = typer.Typer(help="Generate (and optionally submit) a SLURM job for BUSCO on staged proteomes (directory mode).")

_SCRATCH_ACCOUNT_RE = re.compile(r"^/scratch/([^/]+)/")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def infer_account_from_project_dir(project_dir: Path) -> Optional[str]:
    p = str(project_dir.resolve()).replace("\\", "/")
    m = _SCRATCH_ACCOUNT_RE.match(p)
    return m.group(1) if m else None


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


@app.callback(invoke_without_command=True)
def busco_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(..., help="Project directory (typically under /scratch/<account>/...)"),
    lineage: str = typer.Option("fungi_odb12", "--lineage", help="BUSCO lineage dataset name, e.g. fungi_odb12"),
    time: str = typer.Option("24:00:00", "--time", help="SLURM time limit, e.g. 24:00:00"),
    cpus: int = typer.Option(8, "--cpus", help="CPUs per task"),
    mem_per_cpu: str = typer.Option("2G", "--mem-per-cpu", help="Memory per CPU, e.g. 2G"),
    partition: str = typer.Option("small", "--partition", help="SLURM partition"),
    account: Optional[str] = typer.Option(None, "--account", help="SLURM account (overrides auto-detect)"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Do not prompt to confirm detected account"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier (default: busco_<timestamp>)"),
    force: bool = typer.Option(False, "--force", help="Pass -f to BUSCO (overwrite existing output)"),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
    busco_bin_dir: Optional[Path] = typer.Option(
        None, "--busco-bin-dir", help="Override BUSCO bin dir (else uses tools.yaml: busco.bin_dir)"
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    # Validate staged input exists
    staged_proteomes = project_dir / "staged" / "proteomes"
    if not staged_proteomes.exists():
        raise typer.BadParameter(f"Missing staged proteomes dir: {staged_proteomes}")

    # Determine account
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

    # Load tools.yaml
    tools = load_tools(project_dir)
    bin_dir = busco_bin_dir.expanduser().resolve() if busco_bin_dir else tools.busco.bin_dir
    busco_cmd = tools.busco.command or "busco"

    if bin_dir is None or not bin_dir.exists():
        raise typer.BadParameter(
            "BUSCO path not configured or does not exist.\n"
            "Set tools.yaml:\n"
            "  busco:\n"
            "    bin_dir: /scratch/<account>/.../busco_env/bin\n"
            "or pass --busco-bin-dir /path/to/bin"
        )

    rid = run_id or f"busco_{_now_tag()}"

    run_root = project_dir / "runs" / rid
    slurm_dir = run_root / "slurm"
    out_dir = run_root / "busco_results"
    db_dir = project_dir / "cache" / "busco_downloads"
    logs_dir = project_dir / "logs" / "slurm"

    ensure_dir(slurm_dir)
    ensure_dir(out_dir)
    ensure_dir(db_dir)
    ensure_dir(logs_dir)

    script_path = slurm_dir / "busco.sbatch"
    run_name = f"busco_staged_{rid}"
    busco_force_flag = "-f" if force else ""

    script = f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=busco_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%j.out
#SBATCH --error={logs_dir.as_posix()}/%x_%j.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --partition={partition}

set -euo pipefail

PROJECT_DIR="{project_dir.as_posix()}"
SEQ_DIR="{staged_proteomes.as_posix()}"
OUT_DIR="{out_dir.as_posix()}"
DB_DIR="{db_dir.as_posix()}"
LINEAGE="{lineage}"
RUN_NAME="{run_name}"

mkdir -p "$OUT_DIR" "$DB_DIR"

# tools.yaml
export PATH="{bin_dir.as_posix()}:$PATH"

if ! command -v "{busco_cmd}" >/dev/null 2>&1; then
  echo "ERROR: '{busco_cmd}' not found on PATH after prepending {bin_dir.as_posix()}." >&2
  exit 127
fi

THREADS="${{SLURM_CPUS_PER_TASK:-1}}"

# Skip if already completed
if ls "$OUT_DIR"/short_summary*.txt >/dev/null 2>&1; then
  echo "BUSCO already completed in $OUT_DIR (short_summary found)."
  exit 0
fi

echo "Running BUSCO directory mode"
echo "Input dir: $SEQ_DIR"
echo "Output dir: $OUT_DIR"
echo "Lineage: $LINEAGE"
echo "Threads: $THREADS"
echo "Download dir: $DB_DIR"

"{busco_cmd}" -c "$THREADS" \\
  -i "$SEQ_DIR" \\
  -m proteins \\
  -l "$LINEAGE" \\
  --out "$RUN_NAME" \\
  --out_path "$OUT_DIR" \\
  --download_path "$DB_DIR" \\
  {busco_force_flag}

echo "Done."
"""

    script_path.write_text(script, encoding="utf-8")

    log_event(
        project_dir,
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": "slurm_busco_write",
            "run_id": rid,
            "script_path": str(script_path),
            "account": acct,
            "partition": partition,
            "cpus": cpus,
            "mem_per_cpu": mem_per_cpu,
            "time": time,
            "lineage": lineage,
            "busco_bin_dir": str(bin_dir),
            "busco_command": busco_cmd,
            "submit": submit,
        },
    )

    typer.echo(f"Wrote BUSCO SLURM script: {script_path}")

    if submit:
        try:
            res = subprocess.run(["sbatch", str(script_path)], check=True, capture_output=True, text=True)
            typer.echo(res.stdout.strip() or "Submitted.")
            log_event(
                project_dir,
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "event": "slurm_busco_submit",
                    "run_id": rid,
                    "script_path": str(script_path),
                    "sbatch_stdout": res.stdout.strip(),
                },
            )
        except FileNotFoundError:
            raise RuntimeError("sbatch not found on PATH. Submit manually with: sbatch <script>")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}")