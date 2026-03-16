from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.hash import hash_json
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import infer_account_from_project_dir, resolve_staging_id, shlex_quote
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db

app = typer.Typer(help="Generate launcher-based SLURM scripts for InterProScan on staged proteomes.")

SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


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


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"Missing required file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON in {path}: {exc}") from exc


@app.callback(invoke_without_command=True)
def interproscan_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(..., help="Project directory"),
    staging_id: str | None = typer.Option(None, "--staging-id", help="Staging snapshot to use (default: latest)."),
    account: str | None = typer.Option(None, "--account", help="SLURM account (overrides auto-detect)"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Do not prompt to confirm detected account"),
    run_id: str | None = typer.Option(None, "--run-id", help="Run identifier (default: interproscan_<timestamp>)"),
    resume_run_id: str | None = typer.Option(
        None,
        "--resume-run-id",
        help="Resume an existing InterProScan run without rewriting its queue ledger.",
    ),
    application: list[str] = typer.Option(
        ["PfamA"],
        "--application",
        help="InterProScan application to enable. Repeat the flag to request multiple applications.",
    ),
    limit: int | None = typer.Option(
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
    worker_partition: str | None = typer.Option(
        None, "--worker-partition", help="SLURM partition for per-proteome worker jobs (default: launcher partition)"
    ),
    worker_time: str | None = typer.Option(
        None, "--worker-time", help="SLURM time limit for per-proteome worker jobs (default: launcher time)"
    ),
    worker_cpus: int | None = typer.Option(
        None, "--worker-cpus", help="CPUs for per-proteome worker jobs (default: launcher CPUs)"
    ),
    worker_mem: str | None = typer.Option(
        None, "--worker-mem", help="Memory for per-proteome worker jobs (default: launcher memory)"
    ),
    poll_seconds: int = typer.Option(300, "--poll-seconds", help="Polling interval between proteome submissions"),
    interproscan_bin_dir: Path | None = typer.Option(
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
    tools = load_tools(project_dir)
    if run_id and resume_run_id:
        raise typer.BadParameter("Use either --run-id for a new run or --resume-run-id for an existing run, not both.")

    resume_mode = resume_run_id is not None
    format_aliases = {"tsv": "TSV", "xml": "XML", "gff3": "GFF3"}

    if resume_mode:
        rid = resume_run_id or ""
        manifest_path = paths.run_manifest(rid)
        manifest_data = _load_json(manifest_path)
        if manifest_data.get("kind") != "interproscan":
            raise typer.BadParameter(f"Run {rid!r} is not an InterProScan run: {manifest_path}")

        queue_path = project_dir / manifest_data["paths"]["queue_tsv"]
        if not queue_path.exists():
            raise typer.BadParameter(f"Cannot resume InterProScan run {rid!r}: missing queue ledger {queue_path}")

        selected_staging_id = str(manifest_data["staging_id"])
        applications = list(manifest_data.get("interproscan", {}).get("applications") or ["PfamA"])
        formats = list(manifest_data.get("interproscan", {}).get("formats") or ["TSV"])
        interproscan_cmd = manifest_data.get("interproscan", {}).get("command") or tools.interproscan.command or "cluster_interproscan"
        poll_seconds = int(manifest_data.get("interproscan", {}).get("poll_seconds") or poll_seconds)
        acct = str(manifest_data.get("slurm", {}).get("account") or "")
        partition = str(manifest_data.get("slurm", {}).get("partition") or partition)
        time = str(manifest_data.get("slurm", {}).get("time") or time)
        cpus = int(manifest_data.get("slurm", {}).get("cpus") or cpus)
        mem = str(manifest_data.get("slurm", {}).get("mem") or mem)
        effective_worker_partition = str(manifest_data.get("slurm", {}).get("worker_partition") or partition)
        effective_worker_time = str(manifest_data.get("slurm", {}).get("worker_time") or time)
        effective_worker_cpus = int(manifest_data.get("slurm", {}).get("worker_cpus") or cpus)
        effective_worker_mem = str(manifest_data.get("slurm", {}).get("worker_mem") or mem)
        manifest_bin_dir = manifest_data.get("interproscan", {}).get("bin_dir")
        if interproscan_bin_dir is not None:
            bin_dir = interproscan_bin_dir.expanduser().resolve()
        elif manifest_bin_dir:
            bin_dir = Path(str(manifest_bin_dir)).expanduser().resolve()
        else:
            bin_dir = tools.interproscan.bin_dir
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

        bin_dir = interproscan_bin_dir.expanduser().resolve() if interproscan_bin_dir else tools.interproscan.bin_dir
        interproscan_cmd = tools.interproscan.command or "cluster_interproscan"
        applications = list(dict.fromkeys([a.strip() for a in application if a.strip()]))
        if not applications:
            applications = ["PfamA"]
        formats = []
        for raw_format in fmt:
            value = raw_format.strip()
            if not value:
                continue
            formats.append(format_aliases.get(value.lower(), value))
        formats = list(dict.fromkeys(formats))
        if not formats:
            formats = ["TSV"]
        effective_worker_partition = worker_partition or partition
        effective_worker_time = worker_time or time
        effective_worker_cpus = worker_cpus or cpus
        effective_worker_mem = worker_mem or mem
        rid = run_id or f"interproscan_{now_tag()}"
        manifest_data = {}

    if bin_dir is not None and not bin_dir.exists():
        raise typer.BadParameter(
            "InterProScan bin_dir does not exist.\n"
            "Set tools.yaml:\n"
            "  interproscan:\n"
            "    bin_dir: /path/to/bin\n"
            "or pass --interproscan-bin-dir /path/to/bin"
        )
    if len(formats) != 1 or formats[0] != "TSV":
        raise typer.BadParameter(
            "Puhti cluster_interproscan currently supports only a single explicit TSV output for this command."
        )

    staged_proteomes = paths.staging_proteomes_dir(selected_staging_id)
    if not staged_proteomes.exists():
        raise typer.BadParameter(f"Missing staged proteomes dir for {selected_staging_id}: {staged_proteomes}")

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

    if resume_mode:
        queue_rows = []
        with queue_path.open("r", encoding="utf-8", newline="") as f:
            queue_rows = list(csv.DictReader(f, delimiter="\t"))
        if not queue_rows:
            raise typer.BadParameter(f"Cannot resume InterProScan run {rid!r}: queue ledger is empty: {queue_path}")
    else:
        proteomes = sorted(staged_proteomes.glob("*.faa"))
        if not proteomes:
            raise typer.BadParameter(f"No staged proteome FASTA files found in {staged_proteomes}")
        if limit is not None:
            proteomes = proteomes[:limit]
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
    python_exec = shlex_quote(sys.executable or "python3")

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


def retry_failed_sequences() -> int:
    \"\"\"Scan completed rows for .failed_sequences files and retry them.\"\"\"
    rows = read_queue()
    retries = []
    for row in rows:
        if row["status"].strip().lower() != "completed":
            continue
        tsv_path = Path(row["tsv_path"])
        failed_path = Path(f"{{tsv_path}}.failed_sequences")
        if not failed_path.exists():
            continue
        retries.append(row)

    if not retries:
        print("No failed sequences to retry.", flush=True)
        return 0

    print(f"Retrying failed sequences for {{len(retries)}} proteome(s).", flush=True)
    had_errors = False

    for row in retries:
        portal_id = row["portal_id"]
        tsv_path = Path(row["tsv_path"])
        failed_path = Path(f"{{tsv_path}}.failed_sequences")

        # Rotate to numbered backup for traceability
        n = 1
        while Path(f"{{tsv_path}}.failed_sequences.{{n}}").exists():
            n += 1
        rotated = Path(f"{{tsv_path}}.failed_sequences.{{n}}")
        failed_path.rename(rotated)
        print(f"Rotated {{failed_path.name}} -> {{rotated.name}}", flush=True)

        # Submit worker with failed sequences as input, temp output
        retry_output = Path(f"{{tsv_path}}.retry_{{n}}")
        retry_row = dict(row)
        retry_row["input_fasta"] = str(rotated)
        retry_row["tsv_path"] = str(retry_output)

        update_row(portal_id, status="retrying", note=f"retry {{n}}", submitted_job_id="")
        job_id = submit_row(retry_row)
        update_row(portal_id, status="retrying", note=f"retry {{n}} job {{job_id}}", submitted_job_id=job_id)
        print(f"Submitted retry {{n}} for {{portal_id}} as job {{job_id}}", flush=True)

        state = wait_for_terminal_state(job_id)

        if state == "COMPLETED" and retry_output.exists():
            # Append retry results to main TSV
            with open(tsv_path, "a") as main_f, open(retry_output, "r") as retry_f:
                content = retry_f.read()
                if content:
                    main_f.write(content)
            retry_output.unlink()

            # Check if cluster_interproscan produced new failed sequences
            new_failed = Path(f"{{retry_output}}.failed_sequences")
            if new_failed.exists():
                new_failed.rename(failed_path)
                update_row(portal_id, status="completed",
                           note=f"retry {{n}} done; some sequences still failed",
                           submitted_job_id=job_id)
                print(f"Retry {{n}} for {{portal_id}}: recovered some sequences, "
                      f"but new failed sequences remain.", flush=True)
            else:
                update_row(portal_id, status="completed",
                           note=f"retry {{n}} done; all sequences recovered",
                           submitted_job_id=job_id)
                print(f"Retry {{n}} for {{portal_id}}: all failed sequences recovered.",
                      flush=True)
        else:
            note = f"retry {{n}} job {{job_id}} state={{state}}"
            if state == "COMPLETED" and not retry_output.exists():
                note += "; missing output"
            # Restore failed_sequences so a future resume can retry again
            if not failed_path.exists():
                rotated.rename(failed_path)
            update_row(portal_id, status="completed", note=note, submitted_job_id=job_id)
            print(f"Retry {{n}} for {{portal_id}} failed: {{note}}", file=sys.stderr,
                  flush=True)
            had_errors = True
            # Continue with other retries — don't abort

    return 1 if had_errors else 0


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

    # After all primary jobs complete, retry any failed sequences
    return retry_failed_sequences()


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

{python_exec} "{controller_path.as_posix()}"
"""
    launcher_path.write_text(launcher_script, encoding="utf-8")

    if not resume_mode:
        manifest_data = {
            "run_id": rid,
            "kind": "interproscan",
            "created_at": now_iso(),
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
            "ts": now_iso(),
            "event": "slurm_interproscan_resume" if resume_mode else "slurm_interproscan_write",
            "run_id": rid,
            "staging_id": selected_staging_id,
            "launcher_script": str(launcher_path),
            "worker_script": str(worker_path),
            "controller_script": str(controller_path),
            "queue_tsv": str(queue_path),
            "applications": applications,
            "formats": formats,
            "limit": None if resume_mode else limit,
            "resume": resume_mode,
            "submit": submit,
        },
    )

    if resume_mode:
        typer.echo(f"Resuming InterProScan run:  {rid}")
        typer.echo(f"Refreshed launcher:         {launcher_path}")
        typer.echo(f"Refreshed worker:           {worker_path}")
        typer.echo(f"Reused queue ledger:        {queue_path}")
    else:
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
                    "ts": now_iso(),
                    "event": "slurm_interproscan_submit",
                    "run_id": rid,
                    "staging_id": selected_staging_id,
                    "launcher_script": str(launcher_path),
                    "sbatch_stdout": res.stdout.strip(),
                },
            )
        except FileNotFoundError:
            raise RuntimeError("sbatch not found on PATH. Submit manually with: sbatch <script>") from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}") from e


