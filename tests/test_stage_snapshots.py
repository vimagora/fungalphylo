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


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _seed_project(paths: ProjectPaths) -> None:
    project_dir = paths.root
    portal_id = "TestPortal"
    proteome_file_id = "prot1"
    cds_file_id = "cds1"

    proteome_name = "proteins.faa"
    cds_name = "transcripts.fna"

    _write_text(
        project_dir / "raw" / portal_id / proteome_file_id / proteome_name,
        ">jgi|TestPortal|1001|modelA\nMPEPTIDE*\n>jgi|TestPortal|1002|modelB\nMQQQQ\n",
    )
    _write_text(
        project_dir / "raw" / portal_id / cds_file_id / cds_name,
        ">jgi|TestPortal|2001|modelA\nATGGCCAAATAG\n>jgi|TestPortal|2002|modelB\nATGCCCGGGTAA\n",
    )

    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portals(
              portal_id, name, created_at, published_text, published_url, is_published, dataset_id, top_hit_id, meta_json
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                portal_id,
                "Test species",
                _now(),
                "paper",
                "https://example.org/paper",
                1,
                "dataset1",
                "top_hit_1",
                json.dumps({}, ensure_ascii=False),
            ),
        )
        conn.execute(
            """
            INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                proteome_file_id,
                portal_id,
                "proteome",
                proteome_name,
                0,
                None,
                _now(),
                json.dumps({"jat_label": "proteins_filtered", "file_format": "fasta"}, ensure_ascii=False),
            ),
        )
        conn.execute(
            """
            INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                cds_file_id,
                portal_id,
                "cds",
                cds_name,
                0,
                None,
                _now(),
                json.dumps({"jat_label": "cds_filtered", "file_format": "fasta"}, ensure_ascii=False),
            ),
        )
        conn.execute(
            """
            INSERT INTO approvals(portal_id, proteome_file_id, cds_file_id, approved_at, note)
            VALUES(?,?,?,?,?)
            """,
            (portal_id, proteome_file_id, cds_file_id, _now(), "approved"),
        )
        conn.commit()
    finally:
        conn.close()


def _staging_ids(paths: ProjectPaths) -> list[str]:
    conn = connect(paths.db_path)
    try:
        rows = conn.execute("SELECT staging_id FROM stagings ORDER BY created_at").fetchall()
    finally:
        conn.close()
    return [row["staging_id"] for row in rows]


def _snapshot_manifest(paths: ProjectPaths, staging_id: str) -> dict:
    return json.loads(paths.staging_manifest(staging_id).read_text(encoding="utf-8"))


def test_stage_writes_snapshot_scoped_artifacts(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_project(paths)

    result = runner.invoke(app, ["stage", str(project_dir)])
    assert result.exit_code == 0, result.output

    staging_ids = _staging_ids(paths)
    assert len(staging_ids) == 1
    staging_id = staging_ids[0]

    proteome_path = paths.staging_proteomes_dir(staging_id) / "TestPortal.faa"
    cds_path = paths.staging_cds_dir(staging_id) / "TestPortal.fna"
    idmap_path = paths.staging_generated_protein_id_map(staging_id, "TestPortal")
    checksums_path = paths.staging_checksums(staging_id)

    assert proteome_path.exists()
    assert cds_path.exists()
    assert idmap_path.exists()
    assert checksums_path.exists()

    manifest = _snapshot_manifest(paths, staging_id)
    assert manifest["staging_id"] == staging_id
    assert manifest["outputs"]["proteomes_dir"] == f"staging/{staging_id}/proteomes"
    assert manifest["outputs"]["cds_dir"] == f"staging/{staging_id}/cds"
    assert any(
        action["kind"] == "proteome" and action["action"] == "staged"
        for action in manifest["actions"]
    )

    conn = connect(paths.db_path)
    try:
        rows = conn.execute(
            """
            SELECT portal_id, kind, artifact_path, source_file_id, artifact_cache_key
            FROM staging_files
            WHERE staging_id = ?
            ORDER BY kind
            """,
            (staging_id,),
        ).fetchall()
    finally:
        conn.close()

    assert [row["kind"] for row in rows] == ["cds", "proteome"]
    assert rows[0]["portal_id"] == "TestPortal"
    assert rows[0]["artifact_cache_key"]
    assert rows[1]["artifact_cache_key"]


def test_stage_reuses_equivalent_artifacts_in_new_snapshot(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _seed_project(paths)

    first = runner.invoke(app, ["stage", str(project_dir)])
    assert first.exit_code == 0, first.output

    first_staging_id = _staging_ids(paths)[0]
    first_manifest = _snapshot_manifest(paths, first_staging_id)
    first_proteome = paths.staging_proteomes_dir(first_staging_id) / "TestPortal.faa"
    first_cds = paths.staging_cds_dir(first_staging_id) / "TestPortal.fna"

    second = runner.invoke(app, ["stage", str(project_dir)])
    assert second.exit_code == 0, second.output

    staging_ids = _staging_ids(paths)
    assert len(staging_ids) == 2
    second_staging_id = staging_ids[1]
    second_manifest = _snapshot_manifest(paths, second_staging_id)
    second_proteome = paths.staging_proteomes_dir(second_staging_id) / "TestPortal.faa"
    second_cds = paths.staging_cds_dir(second_staging_id) / "TestPortal.fna"

    assert first_proteome.read_text(encoding="utf-8") == second_proteome.read_text(encoding="utf-8")
    assert first_cds.read_text(encoding="utf-8") == second_cds.read_text(encoding="utf-8")

    assert any(
        action["kind"] == "proteome" and action["action"] == "reused"
        for action in second_manifest["actions"]
    )
    assert any(
        action["kind"] == "cds" and action["action"] == "reused"
        for action in second_manifest["actions"]
    )
    assert any(
        action["kind"] == "proteome" and action["action"] == "staged"
        for action in first_manifest["actions"]
    )

    conn = connect(paths.db_path)
    try:
        rows = conn.execute(
            """
            SELECT staging_id, kind, reused_from_staging_id
            FROM staging_files
            WHERE portal_id = 'TestPortal'
            ORDER BY staging_id, kind
            """
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 4
    reused_rows = [row for row in rows if row["staging_id"] == second_staging_id]
    assert all(row["reused_from_staging_id"] == first_staging_id for row in reused_rows)
