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


def test_fetch_index_ingest_from_cache_does_not_require_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("JGI_TOKEN", raising=False)

    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portals(portal_id, name, created_at, published_text, published_url, is_published, meta_json)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                "TestPortal",
                "Test species",
                _now(),
                "paper",
                "https://example.org/paper",
                1,
                json.dumps({}, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cache_payload = {
        "portal_id": "TestPortal",
        "fetched_at": _now(),
        "pages": [
            {
                "organisms": [
                    {
                        "id": "dataset1",
                        "top_hit": {"_id": "top_hit_1"},
                        "files": [
                            {
                                "_id": "file1",
                                "file_name": "proteins.faa",
                                "file_size": 123,
                                "md5sum": "abc123",
                                "file_status": "RESTORED",
                                "file_status_id": 1,
                                "file_path": "/tmp/proteins.faa",
                                "file_group": "group1",
                                "data_group": "genome",
                                "modified_date": _now(),
                                "file_date": _now(),
                                "added_date": _now(),
                                "file_type": ["protein"],
                                "portal_detail_id": "TestPortal",
                                "metadata": {
                                    "mycocosm_portal_id": "TestPortal",
                                    "jat_label": "proteins_filtered",
                                    "file_format": "fasta",
                                },
                            }
                        ],
                    }
                ]
            }
        ],
    }
    paths.jgi_index_cache_dir.mkdir(parents=True, exist_ok=True)
    (paths.jgi_index_cache_dir / "TestPortal.json").write_text(
        json.dumps(cache_payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["fetch-index", "--ingest-from-cache", str(project_dir)])
    assert result.exit_code == 0, result.output

    conn = connect(paths.db_path)
    try:
        portal = conn.execute(
            "SELECT dataset_id, top_hit_id FROM portals WHERE portal_id = ?",
            ("TestPortal",),
        ).fetchone()
        file_row = conn.execute(
            "SELECT portal_id, kind, filename FROM portal_files WHERE file_id = ?",
            ("file1",),
        ).fetchone()
    finally:
        conn.close()

    assert portal["dataset_id"] == "dataset1"
    assert portal["top_hit_id"] == "top_hit_1"
    assert file_row["portal_id"] == "TestPortal"
    assert file_row["kind"] == "proteome"
    assert file_row["filename"] == "proteins.faa"
