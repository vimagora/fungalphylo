from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def test_db_command_allows_select_queries(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portals(portal_id, name, created_at, published_text, published_url, is_published, meta_json)
            VALUES(?,?,?,?,?,?,?)
            """,
            ("PortalA", "Species A", _now(), "paper", "https://example.org", 1, json.dumps({})),
        )
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["db", "--sql", "SELECT portal_id FROM portals", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "PortalA" in result.output


def test_db_command_rejects_write_queries(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(app, ["db", "--sql", "DELETE FROM portals", str(project_dir)])
    assert result.exit_code != 0
    assert "Only read-only" in result.output or "Write or schema-changing SQL is not allowed" in result.output
