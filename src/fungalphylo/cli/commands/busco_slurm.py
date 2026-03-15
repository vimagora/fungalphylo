from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.busco import batch_root_name
from fungalphylo.core.events import log_event
from fungalphylo.core.hash import hash_json
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db

app = typer.Typer(help="Generate (and optionally submit) a SLURM job for BUSCO on a staging snapshot.")

_SCRATCH_ACCOUNT_RE = re.compile(r"^/scratch/([^/]+)/")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def infer_account_from_project_dir(project_dir: Path) -> Optional[str]:
    p = str(project_dir.resolve()).replace("\\", "/")
    m = _SCRATCH_ACCOUNT_RE.match(p)
    return m.group(1) if m else None


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def resolve_staging_id(project_dir: Path, explicit: Optional[str]) -> str:
    if explicit:
        return explicit

    paths = ProjectPaths(project_dir)
    init_db(paths.db_path)
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT staging_id FROM stagings ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        raise typer.BadParameter("No staging snapshot found. Run `fungalphylo stage` first or pass --staging-id.")
    return row["staging_id"]


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"Missing required file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON in {path}: {exc}") from exc


def _render_busco_script(
    *,
    acct: str,
    rid: str,
    logs_dir: Path,
    time: str,
    cpus: int,
    mem_per_cpu: str,
    partition: str,
    project_dir: Path,
    selected_staging_id: str,
    staged_proteomes: Path,
    out_dir: Path,
    db_dir: Path,
    lineage: str,
    run_name: str,
    bin_dir: Path,
    busco_cmd: str,
    force: bool,
) -> str:
    busco_force_flag = "-f" if force else ""
    return f"""#!/bin/bash
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
STAGING_ID="{selected_staging_id}"
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

# Skip if already completed in batch mode
if [ -f "$OUT_DIR/$RUN_NAME/batch_summary.txt" ]; then
  echo "BUSCO already completed in $OUT_DIR/$RUN_NAME (batch_summary.txt found)."
  exit 0
fi

# Run from OUT_DIR so BUSCO writes its log file there, not in the sbatch cwd
# Also override SLURM_SUBMIT_DIR which some tools use instead of the process cwd
cd "$OUT_DIR"
export SLURM_SUBMIT_DIR="$OUT_DIR"

echo "Running BUSCO directory mode"
echo "Staging ID: $STAGING_ID"
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


def _submit_script(script_path: Path, project_dir: Path, rid: str, selected_staging_id: str) -> None:
    try:
        res = subprocess.run(["sbatch", str(script_path)], check=True, capture_output=True, text=True)
        typer.echo(res.stdout.strip() or "Submitted.")
        log_event(
            project_dir,
            {
                "ts": _now_iso(),
                "event": "slurm_busco_submit",
                "run_id": rid,
                "staging_id": selected_staging_id,
                "script_path": str(script_path),
                "sbatch_stdout": res.stdout.strip(),
            },
        )
    except FileNotFoundError:
        raise RuntimeError("sbatch not found on PATH. Submit manually with: sbatch <script>")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}")


@app.callback(invoke_without_command=True)
def busco_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(..., help="Project directory (typically under /scratch/<account>/...)"),
    staging_id: Optional[str] = typer.Option(None, "--staging-id", help="Staging snapshot to use (default: latest)."),
    lineage: Optional[str] = typer.Option(None, "--lineage", help="BUSCO lineage dataset name, e.g. fungi_odb12"),
    time: Optional[str] = typer.Option(None, "--time", help="SLURM time limit, e.g. 24:00:00"),
    cpus: Optional[int] = typer.Option(None, "--cpus", help="CPUs per task"),
    mem_per_cpu: Optional[str] = typer.Option(None, "--mem-per-cpu", help="Memory per CPU, e.g. 2G"),
    partition: Optional[str] = typer.Option(None, "--partition", help="SLURM partition"),
    account: Optional[str] = typer.Option(None, "--account", help="SLURM account (overrides auto-detect)"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Do not prompt to confirm detected account"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Run identifier (default: busco_<timestamp>)"),
    resume_run_id: Optional[str] = typer.Option(
        None,
        "--resume-run-id",
        help="Resume an existing BUSCO run: refresh the script and optionally resubmit.",
    ),
    force: bool = typer.Option(False, "--force", help="Pass -f to BUSCO (overwrite existing output)"),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
    busco_bin_dir: Optional[Path] = typer.Option(
        None, "--busco-bin-dir", help="Override BUSCO bin dir (else uses tools.yaml: busco.bin_dir)"
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    if run_id and resume_run_id:
        raise typer.BadParameter("Use either --run-id for a new run or --resume-run-id for an existing run, not both.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    resume_mode = resume_run_id is not None

    if resume_mode:
        rid = resume_run_id or ""
        manifest_path = paths.run_manifest(rid)
        manifest_data = _load_json(manifest_path)
        if manifest_data.get("kind") != "busco":
            raise typer.BadParameter(f"Run {rid!r} is not a BUSCO run: {manifest_path}")

        selected_staging_id = str(manifest_data["staging_id"])
        acct = account or str(manifest_data.get("slurm", {}).get("account") or "")
        lineage = lineage or str(manifest_data.get("busco", {}).get("lineage") or "fungi_odb12")
        busco_cmd = str(manifest_data.get("busco", {}).get("command") or "busco")
        run_name = str(manifest_data.get("busco", {}).get("run_name") or "")

        # CLI value wins if provided (non-None), otherwise fall back to manifest
        time = time or str(manifest_data.get("slurm", {}).get("time") or "24:00:00")
        cpus = cpus if cpus is not None else int(manifest_data.get("slurm", {}).get("cpus") or 8)
        mem_per_cpu = mem_per_cpu or str(manifest_data.get("slurm", {}).get("mem_per_cpu") or "2G")
        partition = partition or str(manifest_data.get("slurm", {}).get("partition") or "small")

        manifest_bin_dir = manifest_data.get("busco", {}).get("bin_dir")
        if busco_bin_dir is not None:
            bin_dir = busco_bin_dir.expanduser().resolve()
        elif manifest_bin_dir:
            bin_dir = Path(str(manifest_bin_dir)).expanduser().resolve()
        else:
            tools = load_tools(project_dir)
            bin_dir = tools.busco.bin_dir

        # Preserve original force flag unless explicitly overridden
        if not force:
            force = bool(manifest_data.get("busco", {}).get("force", False))

    else:
        selected_staging_id = resolve_staging_id(project_dir, staging_id)

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
        bin_dir = busco_bin_dir.expanduser().resolve() if busco_bin_dir else tools.busco.bin_dir
        busco_cmd = tools.busco.command or "busco"
        rid = run_id or f"busco_{_now_tag()}"

        # Apply defaults for new-run mode
        lineage = lineage or "fungi_odb12"
        time = time or "24:00:00"
        cpus = cpus if cpus is not None else 8
        mem_per_cpu = mem_per_cpu or "2G"
        partition = partition or "small"

    if not acct:
        raise typer.BadParameter(
            "Could not determine SLURM account. Provide --account explicitly."
        )

    if bin_dir is None or not bin_dir.exists():
        raise typer.BadParameter(
            "BUSCO path not configured or does not exist.\n"
            "Set tools.yaml:\n"
            "  busco:\n"
            "    bin_dir: /scratch/<account>/.../busco_env/bin\n"
            "or pass --busco-bin-dir /path/to/bin"
        )

    # Validate staged input exists
    staged_proteomes = paths.staging_proteomes_dir(selected_staging_id)
    if not staged_proteomes.exists():
        raise typer.BadParameter(f"Missing staged proteomes dir for {selected_staging_id}: {staged_proteomes}")

    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    out_dir = run_root / "busco_results"
    db_dir = paths.cache_dir / "busco_downloads"
    logs_dir = paths.logs_dir / "slurm"

    ensure_dir(slurm_dir)
    ensure_dir(out_dir)
    ensure_dir(db_dir)
    ensure_dir(logs_dir)

    script_path = slurm_dir / "busco.sbatch"
    if not resume_mode:
        run_name = batch_root_name(selected_staging_id, rid)

    script = _render_busco_script(
        acct=acct,
        rid=rid,
        logs_dir=logs_dir,
        time=time,
        cpus=cpus,
        mem_per_cpu=mem_per_cpu,
        partition=partition,
        project_dir=project_dir,
        selected_staging_id=selected_staging_id,
        staged_proteomes=staged_proteomes,
        out_dir=out_dir,
        db_dir=db_dir,
        lineage=lineage,
        run_name=run_name,
        bin_dir=bin_dir,
        busco_cmd=busco_cmd,
        force=force,
    )
    script_path.write_text(script, encoding="utf-8")

    if not resume_mode:
        manifest_data = {
            "run_id": rid,
            "kind": "busco",
            "created_at": _now_iso(),
            "staging_id": selected_staging_id,
            "project_dir": str(project_dir),
            "paths": {
                "run_dir": str(run_root.relative_to(project_dir)),
                "script_path": str(script_path.relative_to(project_dir)),
                "results_dir": str(out_dir.relative_to(project_dir)),
                "batch_root": str((out_dir / run_name).relative_to(project_dir)),
                "batch_summary": str((out_dir / run_name / "batch_summary.txt").relative_to(project_dir)),
                "download_cache_dir": str(db_dir.relative_to(project_dir)),
                "logs_dir": str(logs_dir.relative_to(project_dir)),
            },
            "busco": {
                "lineage": lineage,
                "command": busco_cmd,
                "bin_dir": str(bin_dir),
                "run_name": run_name,
                "force": force,
            },
            "slurm": {
                "account": acct,
                "partition": partition,
                "time": time,
                "cpus": cpus,
                "mem_per_cpu": mem_per_cpu,
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
                    "busco",
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
            "ts": _now_iso(),
            "event": "slurm_busco_resume" if resume_mode else "slurm_busco_write",
            "run_id": rid,
            "staging_id": selected_staging_id,
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
            "resume": resume_mode,
        },
    )

    if resume_mode:
        typer.echo(f"Resuming BUSCO run:     {rid}")
        typer.echo(f"Refreshed script:       {script_path}")
    else:
        typer.echo(f"Wrote BUSCO SLURM script: {script_path}")

    if submit:
        _submit_script(script_path, project_dir, rid, selected_staging_id)
