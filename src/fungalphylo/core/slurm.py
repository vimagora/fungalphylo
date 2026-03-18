from __future__ import annotations

import re
from pathlib import Path

import typer

from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect, init_db

_SCRATCH_ACCOUNT_RE = re.compile(r"^/scratch/([^/]+)/")


def infer_account_from_project_dir(project_dir: Path) -> str | None:
    """Extract the SLURM account from a CSC /scratch/<account>/... path."""
    p = str(project_dir.resolve()).replace("\\", "/")
    m = _SCRATCH_ACCOUNT_RE.match(p)
    return m.group(1) if m else None


def resolve_staging_id(project_dir: Path, explicit: str | None) -> str:
    """Return the explicit staging_id or the most recent one from the DB."""
    if explicit:
        return explicit

    paths = ProjectPaths(project_dir)
    init_db(paths.db_path)
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            "SELECT staging_id FROM stagings WHERE staging_id != '__family__' ORDER BY created_at DESC LIMIT 1"
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
