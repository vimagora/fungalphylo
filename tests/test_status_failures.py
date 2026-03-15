"""Tests for the `status failures` subcommand."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

runner = CliRunner()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def test_failures_reports_no_failures_on_clean_project(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)

    result = runner.invoke(app, ["failures", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "No failures found" in result.output


def test_failures_reports_error_log_entries(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    errors_log = paths.errors_log
    errors_log.parent.mkdir(parents=True, exist_ok=True)
    for i in range(3):
        record = {
            "ts": _now(),
            "event": "fetch_index_error",
            "portal_id": f"Portal{i}",
            "exc_type": "HTTPError",
            "exc_msg": f"500 Server Error for portal {i}",
        }
        with errors_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    result = runner.invoke(app, ["failures", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "fetch_index_error" in result.output
    assert "3" in result.output
    assert "HTTPError" in result.output


def test_failures_reports_partial_download_batch(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO download_requests(
                request_id, created_at, request_dir, dry_run, status,
                n_payloads, n_payload_ok, n_errors, moved_files, missing_files,
                max_chars, timeout_seconds, continue_on_error,
                skip_if_raw_present, overwrite_staged, retain
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "req_001", _now(), "download_requests/req_001", 0, "partial",
                5, 3, 2, 10, 4,
                3500, 300, 1, 0, 0, "all",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["failures", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "partial" in result.output
    assert "req_001" in result.output


def test_failures_reports_staging_manifest_failures(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    staging_id = "20260315T000000Z"
    manifest_rel = f"staging/{staging_id}/manifest.json"
    manifest_path = project_dir / manifest_rel
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps({
            "staging_id": staging_id,
            "actions": [],
            "failures": [
                {"portal_id": "BadPortal1", "reason": "FileNotFoundError: missing raw"},
                {"portal_id": "BadPortal2", "reason": "ValueError: empty FASTA"},
            ],
        }),
        encoding="utf-8",
    )

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256) VALUES(?,?,?,?)",
            (staging_id, _now(), manifest_rel, "placeholder_sha256"),
        )
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["failures", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "BadPortal1" in result.output
    assert "BadPortal2" in result.output
    assert "missing raw" in result.output
