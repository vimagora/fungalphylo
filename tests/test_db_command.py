from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

import sqlite3

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import _column_notnull, connect, init_db

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


def test_init_db_migrates_legacy_runs_staging_id_not_null(tmp_path: Path) -> None:
    """init_db should upgrade old DBs where runs.staging_id was NOT NULL.

    Older schemas marked runs.staging_id as NOT NULL, which breaks family-level
    compute runs (no staging involved). The migration must drop the constraint
    and preserve existing rows.
    """
    db_path = tmp_path / "legacy.db"
    legacy_schema = """
    CREATE TABLE stagings (staging_id TEXT PRIMARY KEY);
    CREATE TABLE runs (
      run_id          TEXT PRIMARY KEY,
      staging_id      TEXT NOT NULL,
      kind            TEXT NOT NULL,
      created_at      TEXT NOT NULL,
      manifest_path   TEXT NOT NULL,
      manifest_sha256 TEXT NOT NULL,
      FOREIGN KEY (staging_id) REFERENCES stagings(staging_id)
    );
    INSERT INTO stagings(staging_id) VALUES('stg_legacy');
    INSERT INTO runs VALUES('run_old', 'stg_legacy', 'orthofinder', '2026-01-01', 'runs/run_old/manifest.json', 'abc');
    """
    raw = sqlite3.connect(str(db_path))
    try:
        raw.executescript(legacy_schema)
        raw.commit()
    finally:
        raw.close()

    init_db(db_path)

    conn = connect(db_path)
    try:
        assert _column_notnull(conn, "runs", "staging_id") is False
        row = conn.execute(
            "SELECT staging_id, kind FROM runs WHERE run_id = 'run_old'"
        ).fetchone()
        assert row["staging_id"] == "stg_legacy"
        assert row["kind"] == "orthofinder"
        # Nullable insert now works (what family_phylo needs)
        conn.execute(
            "INSERT INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256) "
            "VALUES('run_fam', NULL, 'family_phylo', '2026-04-15', 'runs/run_fam/manifest.json', 'def')"
        )
        conn.commit()
    finally:
        conn.close()
