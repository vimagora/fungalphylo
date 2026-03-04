from __future__ import annotations

import sqlite3
from pathlib import Path

import typer
from fungalphylo.core.paths import ProjectPaths

app = typer.Typer(help="Run read-only SQL queries against the project database.")


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
    con = sqlite3.connect(paths.db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(sql).fetchmany(limit)
    finally:
        con.close()

    for r in rows:
        print(dict(r))