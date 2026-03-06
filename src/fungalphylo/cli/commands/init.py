from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import typer

from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.config import write_default_config
from fungalphylo.core.events import log_event
from fungalphylo.db.db import init_db
from fungalphylo.core.tools import TOOLS_YAML_TEMPLATE

app = typer.Typer(help="Initialize a new fungalphylo project directory.")


@app.callback(invoke_without_command=True)
def init_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Path to create/use as a fungalphylo project directory."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing config.yaml if it exists."),
) -> None:
    """
    Initialize a new fungalphylo project directory.

    Creates:
      - config.yaml (default configuration)
      - db/fungalphylo.sqlite (SQLite database)
      - raw/, staging/, runs/, logs/
      - logs/events.jsonl
    """
    if ctx.invoked_subcommand is not None:
        return
    
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required when calling `fungalphylo init` without a subcommand.")
    
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    ensure_project_dirs(paths)

    # config.yaml
    if paths.config_yaml.exists() and not force:
        typer.echo(f"config.yaml already exists: {paths.config_yaml} (use --force to overwrite)")
    else:
        write_default_config(paths.config_yaml)

    # 
    if paths.tools_yaml.exists() and not force:
        typer.echo(f"tools.yaml already exists: {paths.tools_yaml} (use --force to overwrite)")
    else:
        paths.tools_yaml.write_text(TOOLS_YAML_TEMPLATE, encoding="utf-8")

    # db
    init_db(paths.db_path)

    # event log
    log_event(
        project_dir,
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": "init",
            "project_dir": str(project_dir),
            "config_path": str(paths.config_yaml),
            "db_path": str(paths.db_path),
        },
    )

    typer.echo(f"Initialized project at: {project_dir}")
    typer.echo(f"Config: {paths.config_yaml}")
    typer.echo(f"DB:     {paths.db_path}")