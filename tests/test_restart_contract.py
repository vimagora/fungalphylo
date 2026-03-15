"""Tests that verify the restart contract documented in agent_context/restart_contract.md.

Each test corresponds to a specific contract clause. If a test here fails, the
implementation has drifted from the documented restart semantics.
"""
from __future__ import annotations

import csv
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


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_approved_portal(
    paths: ProjectPaths,
    portal_id: str = "ContractPortal",
    *,
    prot_file_id: str = "prot1",
    cds_file_id: str = "cds1",
) -> None:
    """Insert a portal with approved proteome+CDS files and raw FASTA on disk."""
    project_dir = paths.root
    long_seq = "M" + "AAAAAAAA" * 10  # 81 aa, above default min_aa=30

    _write_text(
        project_dir / "raw" / portal_id / prot_file_id / "proteins.faa",
        f">jgi|{portal_id}|1001|modelA\n{long_seq}\n>jgi|{portal_id}|1002|modelB\n{long_seq}\n",
    )
    _write_text(
        project_dir / "raw" / portal_id / cds_file_id / "cds.fna",
        f">jgi|{portal_id}|2001|modelA\n{'ATG' + 'GCC' * 30 + 'TAG'}\n"
        f">jgi|{portal_id}|2002|modelB\n{'ATG' + 'CCC' * 25 + 'TAA'}\n",
    )

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO portals(portal_id, name, created_at, is_published, dataset_id, top_hit_id, meta_json) VALUES(?,?,?,?,?,?,?)",
            (portal_id, "Test", _now(), 1, f"ds_{portal_id}", f"top_{portal_id}", "{}"),
        )
        conn.execute(
            "INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, created_at, meta_json) VALUES(?,?,?,?,?,?,?)",
            (prot_file_id, portal_id, "proteome", "proteins.faa", 0, _now(),
             json.dumps({"jat_label": "proteins_filtered", "file_format": "fasta"})),
        )
        conn.execute(
            "INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, created_at, meta_json) VALUES(?,?,?,?,?,?,?)",
            (cds_file_id, portal_id, "cds", "cds.fna", 0, _now(),
             json.dumps({"jat_label": "cds_filtered", "file_format": "fasta"})),
        )
        conn.execute(
            "INSERT INTO approvals(portal_id, proteome_file_id, cds_file_id, approved_at) VALUES(?,?,?,?)",
            (portal_id, prot_file_id, cds_file_id, _now()),
        )
        conn.commit()
    finally:
        conn.close()


def _staging_ids(paths: ProjectPaths) -> list[str]:
    conn = connect(paths.db_path)
    try:
        return [r["staging_id"] for r in conn.execute(
            "SELECT staging_id FROM stagings ORDER BY created_at"
        ).fetchall()]
    finally:
        conn.close()


# ── stage contracts ──────────────────────────────────────────────────────────


def test_stage_always_creates_new_staging_id(tmp_path: Path) -> None:
    """Contract: 'each non-dry-run execution creates a new staging_id'."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_approved_portal(paths)

    res1 = runner.invoke(app, ["stage", str(project_dir)])
    assert res1.exit_code == 0, res1.output
    res2 = runner.invoke(app, ["stage", str(project_dir)])
    assert res2.exit_code == 0, res2.output

    ids = _staging_ids(paths)
    assert len(ids) == 2
    assert ids[0] != ids[1], "Two stage runs must produce different staging_ids"


def test_stage_dry_run_writes_no_snapshot_or_db_rows(tmp_path: Path) -> None:
    """Contract: '--dry-run validates inputs without writing a snapshot or SQLite rows'."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_approved_portal(paths)

    res = runner.invoke(app, ["stage", "--dry-run", str(project_dir)])
    assert res.exit_code == 0, res.output

    assert _staging_ids(paths) == [], "Dry-run must not create staging rows"

    # No snapshot directories should exist
    staging_dirs = list(paths.staging_root.iterdir()) if paths.staging_root.exists() else []
    assert staging_dirs == [], "Dry-run must not create snapshot directories"


def test_stage_reuses_artifacts_by_cache_key(tmp_path: Path) -> None:
    """Contract: 'artifact-level reuse occurs by cache key unless --overwrite is set'."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_approved_portal(paths)

    runner.invoke(app, ["stage", str(project_dir)])
    runner.invoke(app, ["stage", str(project_dir)])

    ids = _staging_ids(paths)
    assert len(ids) == 2

    conn = connect(paths.db_path)
    try:
        reused = conn.execute(
            "SELECT COUNT(*) AS n FROM staging_files WHERE staging_id = ? AND reused_from_staging_id IS NOT NULL",
            (ids[1],),
        ).fetchone()["n"]
    finally:
        conn.close()

    assert reused > 0, "Second stage run must reuse artifacts from first"


def test_stage_overwrite_forces_regeneration(tmp_path: Path) -> None:
    """Contract: '--overwrite disables cache-key based artifact reuse'."""
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_approved_portal(paths)

    runner.invoke(app, ["stage", str(project_dir)])
    runner.invoke(app, ["stage", "--overwrite", str(project_dir)])

    ids = _staging_ids(paths)
    assert len(ids) == 2

    conn = connect(paths.db_path)
    try:
        reused = conn.execute(
            "SELECT COUNT(*) AS n FROM staging_files WHERE staging_id = ? AND reused_from_staging_id IS NOT NULL",
            (ids[1],),
        ).fetchone()["n"]
    finally:
        conn.close()

    assert reused == 0, "With --overwrite, no artifacts should be reused"


# ── fetch-index contracts ────────────────────────────────────────────────────


def test_fetch_index_ingest_from_cache_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    """Contract: 'rerunning is safe and idempotent at the DB level because file rows are upserted'."""
    monkeypatch.delenv("JGI_TOKEN", raising=False)
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)

    conn = connect(paths.db_path)
    try:
        conn.execute(
            "INSERT INTO portals(portal_id, name, created_at, is_published, meta_json) VALUES(?,?,?,?,?)",
            ("IdempotentPortal", "Test", _now(), 1, "{}"),
        )
        conn.commit()
    finally:
        conn.close()

    cache_payload = {
        "portal_id": "IdempotentPortal",
        "fetched_at": _now(),
        "pages": [{
            "organisms": [{
                "id": "ds1",
                "top_hit": {"_id": "top1"},
                "files": [{
                    "_id": "f1",
                    "file_name": "test.faa",
                    "file_size": 100,
                    "md5sum": "abc",
                    "file_status": "RESTORED",
                    "file_status_id": 1,
                    "file_path": "/data/test.faa",
                    "file_group": "g1",
                    "data_group": "genome",
                    "modified_date": _now(),
                    "file_date": _now(),
                    "added_date": _now(),
                    "file_type": ["protein"],
                    "portal_detail_id": "IdempotentPortal",
                    "metadata": {
                        "mycocosm_portal_id": "IdempotentPortal",
                        "jat_label": "proteins_filtered",
                        "file_format": "fasta",
                    },
                }],
            }],
        }],
    }
    paths.jgi_index_cache_dir.mkdir(parents=True, exist_ok=True)
    (paths.jgi_index_cache_dir / "IdempotentPortal.json").write_text(
        json.dumps(cache_payload) + "\n", encoding="utf-8"
    )

    # Run twice
    res1 = runner.invoke(app, ["fetch-index", "--ingest-from-cache", str(project_dir)])
    assert res1.exit_code == 0, res1.output
    res2 = runner.invoke(app, ["fetch-index", "--ingest-from-cache", str(project_dir)])
    assert res2.exit_code == 0, res2.output

    conn = connect(paths.db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM portal_files WHERE portal_id = 'IdempotentPortal'").fetchone()["n"]
    finally:
        conn.close()

    assert count == 1, "Upsert must not create duplicates on rerun"


# ── download skip contracts ──────────────────────────────────────────────────


def test_download_skips_files_already_in_staging_snapshot(tmp_path: Path, monkeypatch) -> None:
    """Contract: 'approved files already represented in any staging_files row are skipped'."""
    monkeypatch.delenv("JGI_TOKEN", raising=False)
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_approved_portal(paths)

    # Run stage so files appear in staging_files
    res = runner.invoke(app, ["stage", str(project_dir)])
    assert res.exit_code == 0, res.output

    # Now try download --dry-run — should skip everything
    res = runner.invoke(app, ["download", "--dry-run", str(project_dir)])
    assert res.exit_code == 0, res.output
    assert "Nothing to download" in res.output or "skip" in res.output.lower()
