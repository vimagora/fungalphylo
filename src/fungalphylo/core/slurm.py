from __future__ import annotations

import re
import subprocess
from pathlib import Path

import typer

from fungalphylo.core.hash import hash_json
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect, init_db

_SCRATCH_ACCOUNT_RE = re.compile(r"^/scratch/([^/]+)/")


def infer_account_from_project_dir(project_dir: Path) -> str | None:
    """Extract the SLURM account from a CSC /scratch/<account>/... path."""
    p = str(project_dir.resolve()).replace("\\", "/")
    m = _SCRATCH_ACCOUNT_RE.match(p)
    return m.group(1) if m else None


def resolve_account(
    project_dir: Path,
    account: str | None,
    no_confirm: bool,
) -> str:
    """Resolve and optionally confirm the SLURM account.

    Returns the account string or raises typer.BadParameter.
    """
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
    return acct


def register_run(
    paths: ProjectPaths,
    project_dir: Path,
    run_id: str,
    kind: str,
    created_at: str,
    manifest_data: dict,
    staging_id: str | None = None,
) -> None:
    """Write manifest and insert run into the database."""
    manifest_path = paths.run_manifest(run_id)
    write_manifest(manifest_path, manifest_data)
    manifest_sha256 = hash_json(manifest_data)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO runs"
            "(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256) "
            "VALUES(?,?,?,?,?,?)",
            (
                run_id,
                staging_id,
                kind,
                created_at,
                str(manifest_path.relative_to(project_dir)),
                manifest_sha256,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def submit_sbatch(script_path: Path, *, use_bash: bool = False) -> str:
    """Submit a SLURM script via sbatch (or bash for orchestrators).

    Returns the stdout from the submission command.
    Raises RuntimeError on failure.
    """
    cmd = ["bash", str(script_path)] if use_bash else ["sbatch", str(script_path)]
    try:
        res = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return res.stdout.strip()
    except FileNotFoundError:
        raise RuntimeError(
            f"{cmd[0]} not found on PATH. Submit manually."
        ) from None
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{cmd[0]} failed: {e.stderr.strip() if e.stderr else str(e)}"
        ) from e


def resolve_staging_id(project_dir: Path, explicit: str | None) -> str:
    """Return the explicit staging_id or the most recent one from the DB."""
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
        raise typer.BadParameter(
            "No staging snapshot found. Run `fungalphylo stage` first or pass --staging-id."
        )
    return row["staging_id"]


def shlex_quote(text: str) -> str:
    """Shell-escape a string using single quotes."""
    return "'" + text.replace("'", "'\"'\"'") + "'"
