from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import typer

from fungalphylo.core.paths import ProjectPaths

app = typer.Typer(help="Run read-only SQL queries against the project database.")
READ_ONLY_SQL_RE = re.compile(r"^\s*(select|with|pragma|explain)\b", re.IGNORECASE)
FORBIDDEN_SQL_RE = re.compile(
    r"\b(insert|update|delete|alter|drop|create|replace|vacuum|attach|detach|reindex|analyze)\b",
    re.IGNORECASE,
)


def _validate_read_only_sql(sql: str) -> None:
    if not READ_ONLY_SQL_RE.match(sql):
        raise typer.BadParameter("Only read-only SELECT/CTE/PRAGMA/EXPLAIN statements are allowed.")
    if FORBIDDEN_SQL_RE.search(sql):
        raise typer.BadParameter("Write or schema-changing SQL is not allowed.")


@app.callback(invoke_without_command=True)
def db_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    sql: str = typer.Option(..., "--sql", help="SQL to execute (read-only SELECT recommended)"),
    limit: int = typer.Option(50, "--limit", help="Max rows to print"),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    paths = ProjectPaths(project_dir.expanduser().resolve())
    _validate_read_only_sql(sql)

    con = sqlite3.connect(f"file:{paths.db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(sql).fetchmany(limit)
    finally:
        con.close()

    for r in rows:
        print(dict(r))
