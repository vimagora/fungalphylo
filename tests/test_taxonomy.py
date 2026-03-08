from __future__ import annotations

import json
import sqlite3
import tarfile
from datetime import datetime, timezone
from pathlib import Path
import io

from typer.testing import CliRunner

from fungalphylo.cli.main import app
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect, init_db


runner = CliRunner()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _init_project(project_dir: Path) -> ProjectPaths:
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    return ProjectPaths(project_dir)


def _insert_portal(paths: ProjectPaths, portal_id: str, ncbi_taxon_id: int | None = None) -> None:
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portals(
              portal_id, name, created_at, published_text, published_url, is_published, ncbi_taxon_id, meta_json
            )
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                portal_id,
                f"{portal_id} species",
                _now(),
                "paper",
                "https://example.org/paper",
                1,
                ncbi_taxon_id,
                json.dumps({}, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_taxon(paths: ProjectPaths, portal_id: str) -> int | None:
    conn = connect(paths.db_path)
    try:
        row = conn.execute("SELECT ncbi_taxon_id FROM portals WHERE portal_id = ?", (portal_id,)).fetchone()
    finally:
        conn.close()
    return row["ncbi_taxon_id"]


def _insert_approval(paths: ProjectPaths, portal_id: str, proteome_file_id: str = "prot1") -> None:
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                proteome_file_id,
                portal_id,
                "proteome",
                f"{proteome_file_id}.faa",
                10,
                None,
                _now(),
                json.dumps({"jat_label": "proteins_filtered", "file_format": "fasta"}, ensure_ascii=False),
            ),
        )
        conn.execute(
            """
            INSERT INTO approvals(portal_id, proteome_file_id, cds_file_id, approved_at, note)
            VALUES(?,?,?,?,?)
            """,
            (portal_id, proteome_file_id, None, _now(), "approved"),
        )
        conn.commit()
    finally:
        conn.close()


def _taxdump_bytes(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, text in files.items():
            data = text.encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _seed_busco_run(paths: ProjectPaths, run_id: str, staging_id: str = "staging1") -> Path:
    run_dir = paths.run_dir(run_id)
    results_dir = run_dir / "busco_results"
    results_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = paths.run_manifest(run_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "kind": "busco",
                "created_at": _now(),
                "staging_id": staging_id,
                "paths": {"results_dir": str(results_dir.relative_to(paths.root))},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256)
            VALUES(?,?,?,?)
            ON CONFLICT(staging_id) DO NOTHING
            """,
            (staging_id, _now(), f"staging/{staging_id}/manifest.json", "dummy"),
        )
        conn.execute(
            """
            INSERT INTO runs(run_id, staging_id, kind, created_at, manifest_path, manifest_sha256)
            VALUES(?,?,?,?,?,?)
            """,
            (run_id, staging_id, "busco", _now(), str(manifest_path.relative_to(paths.root)), "dummy"),
        )
        conn.commit()
    finally:
        conn.close()
    return results_dir


def test_taxonomy_export_writes_all_portals_by_default(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths, "PortalA", 1234)
    _insert_portal(paths, "PortalB")

    out = project_dir / "review" / "taxonomy_export.tsv"
    result = runner.invoke(app, ["taxonomy", "export", "--out", str(out), str(project_dir)])
    assert result.exit_code == 0, result.output

    text = out.read_text(encoding="utf-8")
    assert "portal_id\tname\tncbi_taxon_id\tnote\n" in text
    assert "PortalA\tPortalA species\t1234\t\n" in text
    assert "PortalB\tPortalB species\t\t\n" in text


def test_taxonomy_export_can_limit_to_approved_portals(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths, "PortalA", 1234)
    _insert_portal(paths, "PortalB", 5678)
    _insert_approval(paths, "PortalB", "protB")

    out = project_dir / "review" / "taxonomy_approved.tsv"
    result = runner.invoke(app, ["taxonomy", "export", "--approved-only", "--out", str(out), str(project_dir)])
    assert result.exit_code == 0, result.output

    text = out.read_text(encoding="utf-8")
    assert "PortalB\tPortalB species\t5678\t\n" in text
    assert "PortalA\tPortalA species\t1234\t\n" not in text


def test_taxonomy_fetch_ncbi_downloads_and_extracts_archive(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)
    archive_bytes = _taxdump_bytes(
        {
            "nodes.dmp": "1\t|\t1\t|\tno rank\t|\n",
            "names.dmp": "1\t|\tall\t|\t\t|\tscientific name\t|\n",
        }
    )

    class FakeResponse:
        def __init__(self, payload: bytes):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int = 1024 * 1024):
            for i in range(0, len(self.payload), chunk_size):
                yield self.payload[i : i + chunk_size]

    monkeypatch.setattr(
        "fungalphylo.cli.commands.taxonomy.requests.get",
        lambda url, stream, timeout: FakeResponse(archive_bytes),
    )

    result = runner.invoke(app, ["taxonomy", "fetch-ncbi", str(project_dir)])
    assert result.exit_code == 0, result.output

    archive_path = project_dir / "cache" / "ncbi_taxonomy" / "new_taxdump.tar.gz"
    extract_dir = project_dir / "cache" / "ncbi_taxonomy" / "new_taxdump"
    assert archive_path.exists()
    assert (extract_dir / "nodes.dmp").exists()
    assert (extract_dir / "names.dmp").exists()


def test_taxonomy_fetch_ncbi_skips_when_present_without_force(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    _init_project(project_dir)
    target_root = project_dir / "cache" / "ncbi_taxonomy"
    target_root.mkdir(parents=True, exist_ok=True)
    (target_root / "new_taxdump.tar.gz").write_bytes(b"already-there")
    (target_root / "new_taxdump").mkdir(parents=True, exist_ok=True)

    def _unexpected_get(*args, **kwargs):
        raise AssertionError("download should be skipped when cache is already present")

    monkeypatch.setattr("fungalphylo.cli.commands.taxonomy.requests.get", _unexpected_get)

    result = runner.invoke(app, ["taxonomy", "fetch-ncbi", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "already present" in result.output


def test_taxonomy_busco_mockup_uses_latest_run_and_writes_html(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths, "PortalA", 561)
    _insert_portal(paths, "PortalB", 562)

    taxdump_dir = project_dir / "cache" / "ncbi_taxonomy" / "new_taxdump"
    taxdump_dir.mkdir(parents=True, exist_ok=True)
    (taxdump_dir / "names.dmp").write_text(
        "1\t|\troot\t|\t\t|\tscientific name\t|\n"
        "100\t|\tAscomycota\t|\t\t|\tscientific name\t|\n"
        "200\t|\tEurotiomycetes\t|\t\t|\tscientific name\t|\n"
        "300\t|\tEurotiales\t|\t\t|\tscientific name\t|\n"
        "400\t|\tTrichocomaceae\t|\t\t|\tscientific name\t|\n"
        "500\t|\tAspergillus\t|\t\t|\tscientific name\t|\n"
        "561\t|\tAspergillus fumigatus\t|\t\t|\tscientific name\t|\n"
        "562\t|\tAspergillus niger\t|\t\t|\tscientific name\t|\n",
        encoding="utf-8",
    )
    (taxdump_dir / "nodes.dmp").write_text(
        "1\t|\t1\t|\tno rank\t|\n"
        "100\t|\t1\t|\tphylum\t|\n"
        "200\t|\t100\t|\tclass\t|\n"
        "300\t|\t200\t|\torder\t|\n"
        "400\t|\t300\t|\tfamily\t|\n"
        "500\t|\t400\t|\tgenus\t|\n"
        "561\t|\t500\t|\tspecies\t|\n"
        "562\t|\t500\t|\tspecies\t|\n",
        encoding="utf-8",
    )

    older_results = _seed_busco_run(paths, "busco_old")
    (older_results / "summary.tsv").write_text(
        "query\tcomplete\tsingle\tfragmented\tduplicated\tmissing\nPortalA\t85\t80\t10\t5\t5\n",
        encoding="utf-8",
    )
    latest_results = _seed_busco_run(paths, "busco_latest")
    (latest_results / "summary.tsv").write_text(
        "query\tcomplete\tsingle\tfragmented\tduplicated\tmissing\n"
        "PortalA\t85\t80\t10\t5\t5\n"
        "PortalB\t80\t70\t10\t10\t10\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["taxonomy", "busco-mockup", "--summary-rank", "family", str(project_dir)])
    assert result.exit_code == 0, result.output

    out = project_dir / "runs" / "busco_latest" / "reports" / "taxonomy_busco_mockup.html"
    text = out.read_text(encoding="utf-8")
    assert "Taxonomy-Ordered BUSCO Mockup (busco_latest)" in text
    assert "PortalA" in text
    assert "PortalB" in text
    assert "Aspergillus fumigatus" in text
    assert "Aspergillus niger" in text
    assert "Trichocomaceae" in text
    assert "Family Summary" in text
    assert "C 80.0%" in text
    assert "low-quality" in text


def test_taxonomy_apply_updates_ncbi_taxon_ids(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths, "PortalA")
    _insert_portal(paths, "PortalB", 99)

    tsv = project_dir / "review" / "taxonomy.tsv"
    tsv.parent.mkdir(parents=True, exist_ok=True)
    tsv.write_text(
        "portal_id\tncbi_taxon_id\tnote\n"
        "PortalA\t1234\tnew assignment\n"
        "PortalB\t99\talready set\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["taxonomy", "apply", str(project_dir), str(tsv)])
    assert result.exit_code == 0, result.output
    assert "updated=1" in result.output
    assert "unchanged=1" in result.output
    assert _fetch_taxon(paths, "PortalA") == 1234
    assert _fetch_taxon(paths, "PortalB") == 99


def test_taxonomy_apply_supports_dry_run_and_clear(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths, "PortalA", 321)

    tsv = project_dir / "review" / "taxonomy_clear.tsv"
    tsv.parent.mkdir(parents=True, exist_ok=True)
    tsv.write_text("portal_id\tncbi_taxon_id\nPortalA\t\n", encoding="utf-8")

    dry_run = runner.invoke(app, ["taxonomy", "apply", "--dry-run", str(project_dir), str(tsv)])
    assert dry_run.exit_code == 0, dry_run.output
    assert "cleared=1" in dry_run.output
    assert _fetch_taxon(paths, "PortalA") == 321

    apply_result = runner.invoke(app, ["taxonomy", "apply", str(project_dir), str(tsv)])
    assert apply_result.exit_code == 0, apply_result.output
    assert _fetch_taxon(paths, "PortalA") is None


def test_taxonomy_apply_rejects_unknown_or_invalid_values(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths, "PortalA")

    bad_taxon = project_dir / "review" / "taxonomy_bad.tsv"
    bad_taxon.parent.mkdir(parents=True, exist_ok=True)
    bad_taxon.write_text("portal_id\tncbi_taxon_id\nPortalA\tnot_an_int\n", encoding="utf-8")

    result = runner.invoke(app, ["taxonomy", "apply", str(project_dir), str(bad_taxon)])
    assert result.exit_code != 0
    assert "invalid ncbi_taxon_id" in result.output

    unknown = project_dir / "review" / "taxonomy_unknown.tsv"
    unknown.write_text("portal_id\tncbi_taxon_id\nMissingPortal\t123\n", encoding="utf-8")
    result = runner.invoke(app, ["taxonomy", "apply", str(project_dir), str(unknown)])
    assert result.exit_code != 0
    assert "Unknown portal_id" in result.output


def test_init_db_migrates_existing_portals_table_with_ncbi_taxon_id(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE portals (
              portal_id TEXT PRIMARY KEY,
              name TEXT,
              created_at TEXT NOT NULL,
              published_text TEXT,
              published_url TEXT,
              is_published INTEGER NOT NULL DEFAULT 0,
              dataset_id TEXT,
              top_hit_id TEXT,
              meta_json TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    init_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(portals)").fetchall()}
    finally:
        conn.close()

    assert "ncbi_taxon_id" in cols
