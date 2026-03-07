from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from typer.testing import CliRunner

from fungalphylo.cli.commands.download import move_files_using_manifest
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


def _insert_portal_with_approval(
    paths: ProjectPaths,
    *,
    portal_id: str,
    dataset_id: str,
    top_hit_id: str,
    proteome_file_id: str,
    proteome_filename: str = "proteins.faa",
    cds_file_id: str = "",
    cds_filename: str = "",
) -> None:
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
                f"{portal_id} species",
                _now(),
                "paper",
                "https://example.org/paper",
                1,
                dataset_id,
                top_hit_id,
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
                proteome_filename,
                10,
                None,
                _now(),
                json.dumps({"jat_label": "proteins_filtered", "file_format": "fasta"}, ensure_ascii=False),
            ),
        )
        if cds_file_id:
            conn.execute(
                """
                INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    cds_file_id,
                    portal_id,
                    "cds",
                    cds_filename,
                    10,
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
            (portal_id, proteome_file_id, cds_file_id or None, _now(), "approved"),
        )
        conn.commit()
    finally:
        conn.close()


def _latest_child(root: Path) -> Path:
    children = sorted(root.iterdir())
    assert children, f"No children found under {root}"
    return children[-1]


def _fetch_one(paths: ProjectPaths, sql: str):
    conn = connect(paths.db_path)
    try:
        return conn.execute(sql).fetchone()
    finally:
        conn.close()


def _response_with_bytes(content: bytes, *, status_code: int = 200, headers: dict[str, str] | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = content
    response.headers.update(headers or {})
    return response


def _zip_bytes(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return buf.getvalue()


def test_restore_dry_run_does_not_require_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("JGI_TOKEN", raising=False)
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal_with_approval(
        paths,
        portal_id="PortalA",
        dataset_id="datasetA",
        top_hit_id="topA",
        proteome_file_id="protA",
    )

    result = runner.invoke(app, ["restore", "--dry-run", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "Dry-run complete" in result.output

    out_dir = _latest_child(project_dir / "restore_requests")
    assert (out_dir / "payload_001.json").exists()
    row = _fetch_one(paths, "SELECT dry_run, status, n_payloads, n_posted, n_errors, request_dir FROM restore_requests")
    assert row["dry_run"] == 1
    assert row["status"] == "planned"
    assert row["n_payloads"] == 1
    assert row["n_posted"] == 0
    assert row["n_errors"] == 0
    assert row["request_dir"].endswith(out_dir.name)


def test_download_dry_run_does_not_require_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("JGI_TOKEN", raising=False)
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal_with_approval(
        paths,
        portal_id="PortalA",
        dataset_id="datasetA",
        top_hit_id="topA",
        proteome_file_id="protA",
    )

    result = runner.invoke(app, ["download", "--dry-run", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "Dry-run complete" in result.output

    out_dir = _latest_child(project_dir / "download_requests")
    assert (out_dir / "payload_001.json").exists()
    row = _fetch_one(
        paths,
        "SELECT dry_run, status, n_payloads, n_payload_ok, n_errors, moved_files, missing_files, request_dir FROM download_requests",
    )
    assert row["dry_run"] == 1
    assert row["status"] == "planned"
    assert row["n_payloads"] == 1
    assert row["n_payload_ok"] == 0
    assert row["n_errors"] == 0
    assert row["moved_files"] == 0
    assert row["missing_files"] == 0
    assert row["request_dir"].endswith(out_dir.name)


def test_restore_continue_on_error_posts_remaining_payloads(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal_with_approval(
        paths,
        portal_id="PortalA",
        dataset_id="datasetA",
        top_hit_id="topA",
        proteome_file_id="protA",
    )
    _insert_portal_with_approval(
        paths,
        portal_id="PortalB",
        dataset_id="datasetB",
        top_hit_id="topB",
        proteome_file_id="protB",
    )

    calls: list[str] = []

    def fake_post_restore(payload: dict, token: str, timeout: int = 120) -> dict:
        calls.append(token)
        if len(calls) == 1:
            response = requests.Response()
            response.status_code = 500
            response._content = b"server error"
            raise requests.HTTPError("restore failed", response=response)
        return {"request_status_url": "https://example.org/restore/ok"}

    monkeypatch.setattr("fungalphylo.cli.commands.restore.post_restore", fake_post_restore)

    result = runner.invoke(
        app,
        ["restore", "--token", "test-token", "--max-chars", "1", str(project_dir)],
    )
    assert result.exit_code == 0, result.output
    assert "Posted 1 restore request(s)." in result.output

    out_dir = _latest_child(project_dir / "restore_requests")
    responses = (out_dir / "responses.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(responses) == 1
    row = _fetch_one(paths, "SELECT status, n_payloads, n_posted, n_errors FROM restore_requests")
    assert row["status"] == "partial"
    assert row["n_payloads"] == 2
    assert row["n_posted"] == 1
    assert row["n_errors"] == 1

    errors = [json.loads(line) for line in paths.errors_log.read_text(encoding="utf-8").splitlines()]
    assert any(record["event"] == "restore_error" and record["status_code"] == 500 for record in errors)


def test_status_reports_latest_request_ledger_rows(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal_with_approval(
        paths,
        portal_id="PortalA",
        dataset_id="datasetA",
        top_hit_id="topA",
        proteome_file_id="protA",
    )

    restore_result = runner.invoke(app, ["restore", "--dry-run", str(project_dir)])
    assert restore_result.exit_code == 0, restore_result.output
    download_result = runner.invoke(app, ["download", "--dry-run", str(project_dir)])
    assert download_result.exit_code == 0, download_result.output

    status_result = runner.invoke(app, ["status", str(project_dir)])
    assert status_result.exit_code == 0, status_result.output
    assert "Latest restore/download batches" in status_result.output
    assert "dry-run" in status_result.output
    assert "restore" in status_result.output
    assert "download" in status_result.output
    assert _fetch_one(paths, "SELECT COUNT(*) AS n FROM restore_requests")["n"] == 1
    assert _fetch_one(paths, "SELECT COUNT(*) AS n FROM download_requests")["n"] == 1


def test_move_files_using_manifest_records_ambiguous_matches(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = ProjectPaths(project_dir)
    content_root = tmp_path / "content"
    content_root.mkdir(parents=True)

    (content_root / "elsewhere_a").mkdir()
    (content_root / "elsewhere_b").mkdir()
    (content_root / "elsewhere_a" / "proteins.faa").write_text(">a\nMPEP\n", encoding="utf-8")
    (content_root / "elsewhere_b" / "proteins.faa").write_text(">b\nMPEP\n", encoding="utf-8")

    manifest_csv = tmp_path / "manifest.csv"
    manifest_csv.write_text(
        "\n".join(
            [
                "filename,file_id,jgi grouping id,directory/path,short organism name",
                "proteins.faa,file1,dataset1,expected_dir,PortalA",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    kept_manifest = tmp_path / "kept" / "manifest_001.csv"
    moved, missing, unmatched = move_files_using_manifest(
        content_root=content_root,
        manifest_csv=manifest_csv,
        paths=paths,
        keep_manifest_to=kept_manifest,
    )

    assert moved == 0
    assert missing == 1
    assert kept_manifest.exists()

    with unmatched.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    assert rows == [
        {
            "portal_id": "PortalA",
            "file_id": "file1",
            "filename": "proteins.faa",
            "expected_source_path": str(content_root / "expected_dir" / "proteins.faa"),
            "reason": "missing_or_ambiguous",
        }
    ]


def test_download_marks_failed_batch_for_non_zip_response(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal_with_approval(
        paths,
        portal_id="PortalA",
        dataset_id="datasetA",
        top_hit_id="topA",
        proteome_file_id="protA",
    )

    def fake_post_download(payload: dict, token: str, timeout: int = 300) -> requests.Response:
        return _response_with_bytes(b"not-a-zip", headers={"Content-Disposition": 'attachment; filename="bad.bin"'})

    monkeypatch.setattr("fungalphylo.cli.commands.download.post_download", fake_post_download)

    result = runner.invoke(app, ["download", "--token", "test-token", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "Errors: 1." in result.output

    row = _fetch_one(paths, "SELECT status, n_payloads, n_payload_ok, n_errors, moved_files, missing_files FROM download_requests")
    assert row["status"] == "failed"
    assert row["n_payloads"] == 1
    assert row["n_payload_ok"] == 0
    assert row["n_errors"] == 1
    assert row["moved_files"] == 0
    assert row["missing_files"] == 0

    errors = [json.loads(line) for line in paths.errors_log.read_text(encoding="utf-8").splitlines()]
    assert any(record["event"] == "download_error" and "not a zip" in record["exc_msg"] for record in errors)


def test_download_marks_failed_batch_when_manifest_missing(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal_with_approval(
        paths,
        portal_id="PortalA",
        dataset_id="datasetA",
        top_hit_id="topA",
        proteome_file_id="protA",
    )

    zip_content = _zip_bytes({"payload_dir/proteins.faa": ">x\nMPEP\n"})

    def fake_post_download(payload: dict, token: str, timeout: int = 300) -> requests.Response:
        return _response_with_bytes(zip_content, headers={"Content-Disposition": 'attachment; filename="bundle.zip"'})

    monkeypatch.setattr("fungalphylo.cli.commands.download.post_download", fake_post_download)

    result = runner.invoke(app, ["download", "--token", "test-token", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "Errors: 1." in result.output

    row = _fetch_one(paths, "SELECT status, n_payload_ok, n_errors FROM download_requests")
    assert row["status"] == "failed"
    assert row["n_payload_ok"] == 0
    assert row["n_errors"] == 1

    errors = [json.loads(line) for line in paths.errors_log.read_text(encoding="utf-8").splitlines()]
    assert any(record["event"] == "download_error" and "No manifest CSV found" in record["exc_msg"] for record in errors)
