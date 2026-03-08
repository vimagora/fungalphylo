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
        ["PfamA"],
        "--application",
        help="InterProScan application to enable. Repeat the flag to request multiple applications.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", min=1, help="Only include the first N staged proteomes in the queue (debugging aid)."
    ),
    fmt: list[str] = typer.Option(
        ["TSV"],
        "--format",
        help="InterProScan output format. For the Puhti cluster_interproscan wrapper, only TSV is currently supported.",
    ),
    partition: str = typer.Option("small", "--partition", help="SLURM partition for the launcher job"),
    time: str = typer.Option("48:00:00", "--time", help="SLURM time limit for the launcher job"),
    cpus: int = typer.Option(1, "--cpus", help="CPUs for the launcher job"),
    mem: str = typer.Option("1G", "--mem", help="Memory for the launcher job"),
    worker_partition: Optional[str] = typer.Option(
        None, "--worker-partition", help="SLURM partition for per-proteome worker jobs (default: launcher partition)"
    ),
    worker_time: Optional[str] = typer.Option(
        None, "--worker-time", help="SLURM time limit for per-proteome worker jobs (default: launcher time)"
    ),
    worker_cpus: Optional[int] = typer.Option(
        None, "--worker-cpus", help="CPUs for per-proteome worker jobs (default: launcher CPUs)"
    ),
    worker_mem: Optional[str] = typer.Option(
        None, "--worker-mem", help="Memory for per-proteome worker jobs (default: launcher memory)"
    ),
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
    if limit is not None:
        proteomes = proteomes[:limit]

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
    if bin_dir is not None and not bin_dir.exists():
        raise typer.BadParameter(
            "InterProScan bin_dir does not exist.\n"
            "Set tools.yaml:\n"
            "  interproscan:\n"
            "    bin_dir: /path/to/bin\n"
            "or pass --interproscan-bin-dir /path/to/bin"
        )

    applications = list(dict.fromkeys([a.strip() for a in application if a.strip()]))
    if not applications:
        applications = ["PfamA"]
    format_aliases = {"tsv": "TSV", "xml": "XML", "gff3": "GFF3"}
    formats = []
    for raw_format in fmt:
        value = raw_format.strip()
        if not value:
            continue
        formats.append(format_aliases.get(value.lower(), value))
    formats = list(dict.fromkeys(formats))
    if not formats:
        formats = ["TSV"]
    if len(formats) != 1 or formats[0] != "TSV":
        raise typer.BadParameter(
            "Puhti cluster_interproscan currently supports only a single explicit TSV output for this command."
        )
    effective_worker_partition = worker_partition or partition
    effective_worker_time = worker_time or time
    effective_worker_cpus = worker_cpus or cpus
    effective_worker_mem = worker_mem or mem

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
    worker_path = slurm_dir / "interproscan_worker.sbatch"
    controller_path = scripts_dir / "interproscan_controller.py"

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

    app_args = " ".join(f"-appl {shlex_quote(a)}" for a in applications)
    fmt_args = " ".join(f"-f {shlex_quote(f)}" for f in formats)

    worker_script = f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --partition={effective_worker_partition}
#SBATCH --time={effective_worker_time}
#SBATCH --cpus-per-task={effective_worker_cpus}
#SBATCH --mem={effective_worker_mem}

set -euo pipefail

PORTAL_ID="${{PORTAL_ID:?missing PORTAL_ID}}"
INPUT_FASTA="${{INPUT_FASTA:?missing INPUT_FASTA}}"
RESULT_DIR="${{RESULT_DIR:?missing RESULT_DIR}}"
OUTPUT_TSV="${{OUTPUT_TSV:?missing OUTPUT_TSV}}"
WORK_DIR="${{WORK_DIR:?missing WORK_DIR}}"
IPR_CMD="${{IPR_CMD:?missing IPR_CMD}}"

mkdir -p "$RESULT_DIR" "$WORK_DIR"
cd "$RESULT_DIR"

module load biokit
module load interproscan

"""
    if bin_dir is not None:
        worker_script += f"""export PATH="{bin_dir.as_posix()}:$PATH"

"""
    worker_script += f"""\

if ! command -v "$IPR_CMD" >/dev/null 2>&1; then
  echo "ERROR: '$IPR_CMD' not found on PATH after module load." >&2
  exit 127
fi

"$IPR_CMD" \\
  -i "$INPUT_FASTA" \\
  -o "$OUTPUT_TSV" \\
  -T "$WORK_DIR/$PORTAL_ID" \\
  {app_args} \\
  {fmt_args}
"""
    worker_path.write_text(worker_script, encoding="utf-8")
    worker_path.chmod(0o755)

    controller_script = f"""#!/usr/bin/env python3
from __future__ import annotations

import csv
import subprocess
import sys
import time
from pathlib import Path

QUEUE_PATH = Path({json.dumps(str(queue_path))})
WORKER_SCRIPT = Path({json.dumps(str(worker_path))})
WORK_DIR = Path({json.dumps(str(work_dir))})
LOGS_DIR = Path({json.dumps(str(logs_dir))})
IPR_CMD = {json.dumps(interproscan_cmd)}
POLL_SECONDS = {poll_seconds}
RUN_ID = {json.dumps(rid)}
ACCOUNT = {json.dumps(acct)}
PARTITION = {json.dumps(effective_worker_partition)}


def read_queue() -> list[dict[str, str]]:
    with QUEUE_PATH.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\\t"))


def write_queue(rows: list[dict[str, str]]) -> None:
    tmp = QUEUE_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "portal_id",
                "input_fasta",
                "status",
                "submitted_job_id",
                "results_dir",
                "tsv_path",
                "note",
            ],
            delimiter="\\t",
        )
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(QUEUE_PATH)


def update_row(portal_id: str, **updates: str) -> dict[str, str]:
    rows = read_queue()
    target = None
    for row in rows:
        if row["portal_id"] == portal_id:
            row.update(updates)
            target = row
            break
    if target is None:
        raise RuntimeError(f"Portal not found in queue: {{portal_id}}")
    write_queue(rows)
    return target


def running_state(job_id: str) -> str | None:
    res = subprocess.run(
        ["squeue", "-h", "-j", job_id, "-o", "%T"],
        check=False,
        capture_output=True,
        text=True,
    )
    state = res.stdout.strip()
    return state or None


def terminal_state(job_id: str) -> str | None:
    res = subprocess.run(
        ["sacct", "-n", "-P", "-j", job_id, "--format", "JobIDRaw,State"],
        check=False,
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return None
    for line in res.stdout.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 2:
            continue
        if parts[0] == job_id:
            return parts[1].split()[0]
    return None


def wait_for_terminal_state(job_id: str) -> str:
    while True:
        queued = running_state(job_id)
        if queued is not None:
            print(f"Job {{job_id}} active: {{queued}}", flush=True)
            time.sleep(POLL_SECONDS)
            continue
        state = terminal_state(job_id)
        if state:
            return state
        print(f"Job {{job_id}} not yet visible in sacct; sleeping {{POLL_SECONDS}}s", flush=True)
        time.sleep(POLL_SECONDS)


def submit_row(row: dict[str, str]) -> str:
    portal_id = row["portal_id"]
    output_log = LOGS_DIR / f"ipr_{{RUN_ID}}_{{portal_id}}_%j.out"
    error_log = LOGS_DIR / f"ipr_{{RUN_ID}}_{{portal_id}}_%j.err"
    export_env = ",".join(
        [
            "ALL",
            f"PORTAL_ID={{portal_id}}",
            f"INPUT_FASTA={{row['input_fasta']}}",
            f"RESULT_DIR={{row['results_dir']}}",
            f"OUTPUT_TSV={{row['tsv_path']}}",
            f"WORK_DIR={{WORK_DIR}}",
            f"IPR_CMD={{IPR_CMD}}",
        ]
    )
    res = subprocess.run(
        [
            "sbatch",
            "--parsable",
            "--account",
            ACCOUNT,
            "--partition",
            PARTITION,
            "--job-name",
            f"ipr_{{RUN_ID}}_{{portal_id}}",
            "--output",
            str(output_log),
            "--error",
            str(error_log),
            "--export",
            export_env,
            str(WORKER_SCRIPT),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = res.stdout.strip()
    job_id = stdout.split(";", 1)[0]
    if not job_id:
        raise RuntimeError(f"sbatch returned no job id for {{portal_id}}: {{stdout!r}}")
    return job_id


def main() -> int:
    for row in read_queue():
        status = row["status"].strip().lower()
        if status == "completed":
            continue
        if status == "submitted" and row["submitted_job_id"].strip():
            job_id = row["submitted_job_id"].strip()
        else:
            update_row(row["portal_id"], status="submitting", note="", submitted_job_id="")
            job_id = submit_row(row)
            row = update_row(
                row["portal_id"],
                status="submitted",
                submitted_job_id=job_id,
                note="",
            )
            print(f"Submitted {{row['portal_id']}} as job {{job_id}}", flush=True)

        state = wait_for_terminal_state(job_id)
        tsv_path = Path(row["tsv_path"])
        if state == "COMPLETED" and tsv_path.exists():
            update_row(row["portal_id"], status="completed", note="", submitted_job_id=job_id)
            print(f"Completed {{row['portal_id']}} as job {{job_id}}", flush=True)
            continue

        note = f"job {{job_id}} finished with state={{state}}"
        if state == "COMPLETED" and not tsv_path.exists():
            note += "; missing TSV output"
        update_row(row["portal_id"], status="failed", note=note, submitted_job_id=job_id)
        print(f"Failed {{row['portal_id']}}: {{note}}", file=sys.stderr, flush=True)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
    controller_path.write_text(controller_script, encoding="utf-8")
    controller_path.chmod(0o755)

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

python3 "{controller_path.as_posix()}"
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
            "controller_script": str(controller_path.relative_to(project_dir)),
            "queue_tsv": str(queue_path.relative_to(project_dir)),
            "results_root": str(results_root.relative_to(project_dir)),
        },
        "interproscan": {
            "applications": applications,
            "formats": formats,
            "command": interproscan_cmd,
            "bin_dir": (str(bin_dir) if bin_dir is not None else None),
            "module_loads": ["biokit", "interproscan"],
            "limit": limit,
            "poll_seconds": poll_seconds,
            "n_proteomes": len(queue_rows),
            "controller_mode": "submit_and_poll",
        },
        "slurm": {
            "account": acct,
            "partition": partition,
            "time": time,
            "cpus": cpus,
            "mem": mem,
            "submit": submit,
            "worker_partition": effective_worker_partition,
            "worker_time": effective_worker_time,
            "worker_cpus": effective_worker_cpus,
            "worker_mem": effective_worker_mem,
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
            "controller_script": str(controller_path),
            "queue_tsv": str(queue_path),
            "applications": applications,
            "formats": formats,
            "limit": limit,
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
