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
from fungalphylo.core.slurm import infer_account_from_project_dir, resolve_staging_id
from fungalphylo.core.tools import load_tools
from fungalphylo.db.db import connect, init_db

app = typer.Typer(
    help="Generate (and optionally submit) a SLURM job for OrthoFinder."
)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"Missing required file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON in {path}: {exc}") from exc


def _render_orthofinder_script(
    *,
    acct: str,
    rid: str,
    logs_dir: Path,
    time: str,
    cpus: int,
    mem_per_cpu: str,
    partition: str,
    project_dir: Path,
    input_dir: Path,
    results_dir: Path,
    env_activate: Path | None,
    of_cmd: str,
    msa_program: str,
    resume_from: Path | None,
    og_only: bool,
) -> str:
    # Environment setup: module purge → StdEnv → python-data → source env → load MSA tool
    if env_activate is not None:
        env_lines = (
            "module purge\n"
            "module load StdEnv\n"
            "module load python-data\n"
            f'source "{env_activate.as_posix()}"\n'
            f"module load {msa_program}\n"
        )
    else:
        env_lines = (
            "# No env_activate configured; assuming orthofinder is on PATH\n"
            f"module load {msa_program}\n"
        )

    # OrthoFinder command
    # When og_only: use -M dendroblast to skip MSA/gene trees entirely.
    # Orthogroups are assigned by MCL before MSA, so results are identical.
    if og_only:
        msa_opts = " -M dendroblast"
    else:
        msa_opts = f" -A {msa_program}"

    if resume_from is not None:
        of_invocation = (
            f'"{of_cmd}" -b "{resume_from.as_posix()}"'
            f"{msa_opts}"
            f' -t "$THREADS"'
        )
    else:
        of_invocation = (
            f'"{of_cmd}" -f "{input_dir.as_posix()}"'
            f"{msa_opts}"
            f' -t "$THREADS"'
        )

    return f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=orthofinder_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%j.out
#SBATCH --error={logs_dir.as_posix()}/%x_%j.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --partition={partition}

set -euo pipefail

{env_lines}
if ! command -v "{of_cmd}" >/dev/null 2>&1; then
  echo "ERROR: '{of_cmd}' not found on PATH." >&2
  exit 127
fi

THREADS="${{SLURM_CPUS_PER_TASK:-1}}"

echo "Running OrthoFinder"
echo "Input dir:   {input_dir.as_posix()}"
echo "Results dir: {results_dir.as_posix()}"
echo "MSA program: {msa_program}"
echo "Threads:     $THREADS"

{of_invocation}

echo "Done."
"""


def _submit_script(script_path: Path, project_dir: Path, rid: str) -> None:
    try:
        res = subprocess.run(
            ["sbatch", str(script_path)], check=True, capture_output=True, text=True
        )
        typer.echo(res.stdout.strip() or "Submitted.")
        log_event(
            project_dir,
            {
                "ts": now_iso(),
                "event": "slurm_orthofinder_submit",
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


def _resolve_input_dir(
    *,
    input_dir: Path | None,
    family_id: str | None,
    staging_id: str | None,
    project_dir: Path,
    paths: ProjectPaths,
) -> tuple[Path, str, str]:
    """Resolve the input directory for OrthoFinder.

    Returns (input_dir, source_kind, source_id).
    """
    if input_dir is not None:
        input_dir = input_dir.expanduser().resolve()
        if not input_dir.is_dir():
            raise typer.BadParameter(f"Input directory does not exist: {input_dir}")
        return input_dir, "custom", str(input_dir)

    if family_id is not None:
        selected_dir = paths.family_selected_dir(family_id)
        if not selected_dir.is_dir():
            raise typer.BadParameter(
                f"Family selected directory does not exist: {selected_dir}\n"
                f"Run `fungalphylo protsetphylo select --family-id {family_id}` first."
            )
        faa_files = list(selected_dir.glob("*.faa"))
        if not faa_files:
            raise typer.BadParameter(f"No .faa files in {selected_dir}")
        return selected_dir, "family", family_id

    resolved_staging = resolve_staging_id(project_dir, staging_id)
    proteomes_dir = paths.staging_proteomes_dir(resolved_staging)
    if not proteomes_dir.is_dir():
        raise typer.BadParameter(
            f"Missing staged proteomes dir for {resolved_staging}: {proteomes_dir}"
        )
    faa_files = list(proteomes_dir.glob("*.faa"))
    if not faa_files:
        raise typer.BadParameter(f"No .faa files in {proteomes_dir}")
    return proteomes_dir, "staging", resolved_staging


@app.callback(invoke_without_command=True)
def orthofinder_slurm_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(
        ..., help="Project directory (typically under /scratch/<account>/...)"
    ),
    staging_id: str | None = typer.Option(
        None, "--staging-id", help="Staging snapshot to use (default: latest)."
    ),
    family_id: str | None = typer.Option(
        None, "--family-id",
        help="Use selected/ FASTAs from this gene family instead of staging proteomes.",
    ),
    input_dir: Path | None = typer.Option(
        None, "--input-dir",
        help="Explicit input directory of .faa files (overrides --staging-id and --family-id).",
    ),
    time: str | None = typer.Option(None, "--time", help="SLURM time limit (default: 48:00:00)"),
    cpus: int | None = typer.Option(None, "--cpus", help="CPUs per task (default: 16)"),
    mem_per_cpu: str | None = typer.Option(
        None, "--mem-per-cpu", help="Memory per CPU (default: 4G)"
    ),
    partition: str | None = typer.Option(None, "--partition", help="SLURM partition"),
    account: str | None = typer.Option(
        None, "--account", help="SLURM account (overrides auto-detect)"
    ),
    no_confirm: bool = typer.Option(
        False, "--no-confirm", help="Do not prompt to confirm detected account"
    ),
    run_id: str | None = typer.Option(
        None, "--run-id", help="Run identifier (default: orthofinder_<timestamp>)"
    ),
    resume_run_id: str | None = typer.Option(
        None, "--resume-run-id",
        help="Resume an existing OrthoFinder run: reuse BLAST results and resubmit.",
    ),
    msa_program: str | None = typer.Option(
        None, "--msa-program",
        help="MSA program for gene trees (default: from tools.yaml, typically mafft).",
    ),
    og_only: bool = typer.Option(
        False, "--og-only",
        help="Use -M dendroblast to skip MSA/gene trees (orthogroups are identical).",
    ),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    if run_id and resume_run_id:
        raise typer.BadParameter(
            "Use either --run-id for a new run or --resume-run-id for an existing run, not both."
        )

    if family_id and staging_id:
        raise typer.BadParameter(
            "Use either --family-id or --staging-id, not both."
        )

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    tools = load_tools(project_dir)
    resume_mode = resume_run_id is not None

    if resume_mode:
        rid = resume_run_id or ""
        manifest_path = paths.run_manifest(rid)
        manifest_data = _load_json(manifest_path)
        if manifest_data.get("kind") != "orthofinder":
            raise typer.BadParameter(f"Run {rid!r} is not an OrthoFinder run: {manifest_path}")

        acct = account or str(manifest_data.get("slurm", {}).get("account") or "")
        of_cmd = str(manifest_data.get("orthofinder", {}).get("command") or tools.orthofinder.command)
        selected_msa = msa_program or str(
            manifest_data.get("orthofinder", {}).get("msa_program") or tools.orthofinder.msa_program
        )

        # og_only: CLI wins if set, otherwise fall back to manifest
        if not og_only:
            og_only = bool(manifest_data.get("orthofinder", {}).get("og_only", False))

        # Resolve env_activate from manifest or tools.yaml
        manifest_env = manifest_data.get("orthofinder", {}).get("env_activate")
        if manifest_env:
            env_activate = Path(str(manifest_env)).expanduser().resolve()
        else:
            env_activate = tools.orthofinder.env_activate

        # SLURM params: CLI wins, then manifest, then defaults
        time = time or str(manifest_data.get("slurm", {}).get("time") or "48:00:00")
        cpus = cpus if cpus is not None else int(manifest_data.get("slurm", {}).get("cpus") or 16)
        mem_per_cpu = mem_per_cpu or str(
            manifest_data.get("slurm", {}).get("mem_per_cpu") or "4G"
        )
        partition = partition or str(manifest_data.get("slurm", {}).get("partition") or "small")

        # Input dir from manifest
        source_kind = str(manifest_data.get("source_kind", "staging"))
        source_id = str(manifest_data.get("source_id", ""))
        resolved_input_dir = Path(str(manifest_data["paths"]["input_dir"]))

        # Resume from previous OrthoFinder results
        results_dir = paths.run_dir(rid) / "orthofinder_results"
        # Find the OrthoFinder WorkingDirectory for -b
        resume_from = results_dir
        # OrthoFinder creates Results_<date>/ under the input dir or results dir
        # For -b, we point at the WorkingDirectory inside Results_*
        for results_sub in sorted(results_dir.glob("Results_*"), reverse=True):
            wd = results_sub / "WorkingDirectory"
            if wd.is_dir():
                resume_from = wd
                break

    else:
        resolved_input_dir, source_kind, source_id = _resolve_input_dir(
            input_dir=input_dir,
            family_id=family_id,
            staging_id=staging_id,
            project_dir=project_dir,
            paths=paths,
        )

        inferred = infer_account_from_project_dir(project_dir)
        acct = account or inferred
        if not acct:
            raise typer.BadParameter(
                "Could not infer SLURM account from project_dir "
                "(expected /scratch/<account>/...). Provide --account explicitly."
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

        of_cmd = tools.orthofinder.command
        selected_msa = msa_program or tools.orthofinder.msa_program
        env_activate = tools.orthofinder.env_activate
        rid = run_id or f"orthofinder_{now_tag()}"
        resume_from = None

        # Apply defaults
        time = time or "48:00:00"
        cpus = cpus if cpus is not None else 16
        mem_per_cpu = mem_per_cpu or "4G"
        partition = partition or "small"

    if not acct:
        raise typer.BadParameter(
            "Could not determine SLURM account. Provide --account explicitly."
        )

    run_root = paths.run_dir(rid)
    slurm_dir = run_root / "slurm"
    results_dir = run_root / "orthofinder_results"
    logs_dir = paths.logs_dir / "slurm"

    _ensure_dir(slurm_dir)
    _ensure_dir(results_dir)
    _ensure_dir(logs_dir)

    script_path = slurm_dir / "orthofinder.sbatch"

    script = _render_orthofinder_script(
        acct=acct,
        rid=rid,
        logs_dir=logs_dir,
        time=time,
        cpus=cpus,
        mem_per_cpu=mem_per_cpu,
        partition=partition,
        project_dir=project_dir,
        input_dir=resolved_input_dir,
        results_dir=results_dir,
        env_activate=env_activate,
        of_cmd=of_cmd,
        msa_program=selected_msa,
        resume_from=resume_from,
        og_only=og_only,
    )
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    if not resume_mode:
        manifest_data = {
            "run_id": rid,
            "kind": "orthofinder",
            "created_at": now_iso(),
            "source_kind": source_kind,
            "source_id": source_id,
            "project_dir": str(project_dir),
            "paths": {
                "run_dir": str(run_root.relative_to(project_dir)),
                "script_path": str(script_path.relative_to(project_dir)),
                "results_dir": str(results_dir.relative_to(project_dir)),
                "input_dir": str(resolved_input_dir),
                "logs_dir": str(logs_dir.relative_to(project_dir)),
            },
            "orthofinder": {
                "command": of_cmd,
                "env_activate": str(env_activate) if env_activate else None,
                "msa_program": selected_msa,
                "og_only": og_only,
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

        # For family runs, use __family__ sentinel; for staging, use the staging_id
        db_staging_id = source_id if source_kind == "staging" else "__family__"

        conn = connect(paths.db_path)
        try:
            if db_staging_id == "__family__":
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
                    db_staging_id,
                    "orthofinder",
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
            "event": "slurm_orthofinder_resume" if resume_mode else "slurm_orthofinder_write",
            "run_id": rid,
            "source_kind": source_kind if not resume_mode else manifest_data.get("source_kind"),
            "source_id": source_id if not resume_mode else manifest_data.get("source_id"),
            "input_dir": str(resolved_input_dir),
            "script_path": str(script_path),
            "account": acct,
            "partition": partition,
            "cpus": cpus,
            "mem_per_cpu": mem_per_cpu,
            "time": time,
            "msa_program": selected_msa,
            "submit": submit,
            "resume": resume_mode,
        },
    )

    n_faa = len(list(resolved_input_dir.glob("*.faa")))
    if resume_mode:
        typer.echo(f"Resuming OrthoFinder run: {rid}")
        typer.echo(f"Refreshed script:         {script_path}")
    else:
        typer.echo(f"Wrote OrthoFinder SLURM script: {script_path}")
    typer.echo(f"Input dir ({n_faa} .faa files):  {resolved_input_dir}")

    if submit:
        _submit_script(script_path, project_dir, rid)
