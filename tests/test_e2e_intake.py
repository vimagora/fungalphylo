"""End-to-end integration test: ingest → fetch-index → autoselect → review → stage.

Exercises the full intake pipeline on a small two-portal fixture dataset with
no network access.  Every command is invoked through the real CLI runner.
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


# ── fixture helpers ──────────────────────────────────────────────────────────


def _write_ingest_tsv(path: Path, portals: list[dict]) -> None:
    """Write a minimal portal-only TSV suitable for `ingest --table`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["portal_id", "name", "Published"], delimiter="\t")
        w.writeheader()
        w.writerows(portals)


def _write_cache_json(cache_dir: Path, portal_id: str, files: list[dict]) -> None:
    """Write a minimal JGI-like cache JSON for `fetch-index --ingest-from-cache`."""
    payload = {
        "portal_id": portal_id,
        "fetched_at": _now(),
        "pages": [
            {
                "organisms": [
                    {
                        "id": f"ds_{portal_id}",
                        "top_hit": {"_id": f"top_{portal_id}"},
                        "files": files,
                    }
                ]
            }
        ],
    }
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{portal_id}.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _make_jgi_file(
    file_id: str,
    portal_id: str,
    filename: str,
    jat_label: str,
    data_group: str = "genome",
    file_format: str = "fasta",
) -> dict:
    """Build a single JGI Files-style record for the cache fixture."""
    return {
        "_id": file_id,
        "file_name": filename,
        "file_size": 1024,
        "md5sum": None,
        "file_status": "RESTORED",
        "file_status_id": 1,
        "file_path": f"/data/{filename}",
        "file_group": "group1",
        "data_group": data_group,
        "modified_date": _now(),
        "file_date": _now(),
        "added_date": _now(),
        "file_type": [],
        "portal_detail_id": portal_id,
        "metadata": {
            "mycocosm_portal_id": portal_id,
            "jat_label": jat_label,
            "file_format": file_format,
        },
    }


def _write_raw_fasta(
    raw_dir: Path, portal_id: str, file_id: str, filename: str, records: list[tuple[str, str]]
) -> None:
    """Write a small FASTA file under raw/<portal>/<file_id>/<filename>."""
    dest = raw_dir / portal_id / file_id / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for header, seq in records:
        lines.append(f">{header}\n{seq}\n")
    dest.write_text("".join(lines), encoding="utf-8")


def _latest_file(directory: Path, glob_pattern: str) -> Path:
    matches = sorted(directory.glob(glob_pattern))
    assert matches, f"No files matching {glob_pattern} in {directory}"
    return matches[-1]


# ── the test ─────────────────────────────────────────────────────────────────


def test_full_intake_pipeline(tmp_path: Path, monkeypatch) -> None:
    """Run init → ingest → fetch-index → autoselect → review → stage end-to-end."""
    monkeypatch.delenv("JGI_TOKEN", raising=False)

    project_dir = tmp_path / "project"

    # ── 1. init ──────────────────────────────────────────────────────────
    res = runner.invoke(app, ["init", str(project_dir)])
    assert res.exit_code == 0, res.output
    paths = ProjectPaths(project_dir)

    # ── 2. ingest (portal-only TSV) ──────────────────────────────────────
    tsv_path = tmp_path / "portals.tsv"
    _write_ingest_tsv(
        tsv_path,
        [
            {"portal_id": "AlphaFung1", "name": "Alpha fungus", "Published": "yes"},
            {"portal_id": "BetaFung2", "name": "Beta fungus", "Published": "yes"},
        ],
    )

    res = runner.invoke(app, ["ingest", "--table", str(tsv_path), str(project_dir)])
    assert res.exit_code == 0, res.output

    conn = connect(paths.db_path)
    assert conn.execute("SELECT COUNT(*) c FROM portals").fetchone()["c"] == 2
    conn.close()

    # ── 3. fetch-index (from cache fixtures, no network) ─────────────────
    for portal_id in ("AlphaFung1", "BetaFung2"):
        _write_cache_json(
            paths.jgi_index_cache_dir,
            portal_id,
            [
                _make_jgi_file(
                    f"{portal_id}_prot",
                    portal_id,
                    f"{portal_id}_proteins.faa",
                    "proteins_filtered",
                ),
                _make_jgi_file(
                    f"{portal_id}_cds",
                    portal_id,
                    f"{portal_id}_cds.fna",
                    "cds_filtered",
                ),
            ],
        )

    res = runner.invoke(app, ["fetch-index", "--ingest-from-cache", str(project_dir)])
    assert res.exit_code == 0, res.output

    conn = connect(paths.db_path)
    assert conn.execute("SELECT COUNT(*) c FROM portal_files").fetchone()["c"] == 4
    conn.close()

    # ── 4. autoselect ────────────────────────────────────────────────────
    res = runner.invoke(app, ["autoselect", str(project_dir)])
    assert res.exit_code == 0, res.output

    review_dir = project_dir / "review"
    # Pick the selection file (not the explain file)
    autoselect_candidates = sorted(
        p for p in review_dir.glob("autoselect_*.tsv") if "explain" not in p.name
    )
    assert autoselect_candidates, "No autoselect selection TSV found"
    autoselect_tsv = autoselect_candidates[-1]
    with autoselect_tsv.open("r", encoding="utf-8", newline="") as f:
        autoselect_rows = list(csv.DictReader(f, delimiter="\t"))

    assert len(autoselect_rows) == 2
    selected_portals = {r["portal_id"] for r in autoselect_rows}
    assert selected_portals == {"AlphaFung1", "BetaFung2"}

    # ── 5. review export + apply ─────────────────────────────────────────
    res = runner.invoke(
        app,
        [
            "review",
            "export",
            str(project_dir),
            "--from-autoselect",
            str(autoselect_tsv),
        ],
    )
    assert res.exit_code == 0, res.output

    review_edit_tsv = _latest_file(review_dir, "review_edit_*.tsv")

    res = runner.invoke(
        app,
        ["review", "apply", str(project_dir), str(review_edit_tsv), "--all"],
    )
    assert res.exit_code == 0, res.output

    conn = connect(paths.db_path)
    approvals = conn.execute("SELECT portal_id FROM approvals ORDER BY portal_id").fetchall()
    conn.close()
    assert [r["portal_id"] for r in approvals] == ["AlphaFung1", "BetaFung2"]

    # ── 6. plant raw FASTA files ─────────────────────────────────────────
    raw_dir = project_dir / "raw"
    # Sequences must be >= 30 aa (default min_aa) to survive staging filters
    long_seq_a = "M" + "PEPTIDE" * 10  # 71 aa
    long_seq_b = "M" + "QQQQAAA" * 8  # 57 aa
    cds_a = "ATG" + "GCCAAA" * 30 + "TAG"  # 183 nt + stop
    cds_b = "ATG" + "CCCGGG" * 25 + "TAA"  # 153 nt + stop
    for portal_id in ("AlphaFung1", "BetaFung2"):
        _write_raw_fasta(
            raw_dir,
            portal_id,
            f"{portal_id}_prot",
            f"{portal_id}_proteins.faa",
            [
                (f"jgi|{portal_id}|1001|modelA", long_seq_a),
                (f"jgi|{portal_id}|1002|modelB", long_seq_b),
            ],
        )
        _write_raw_fasta(
            raw_dir,
            portal_id,
            f"{portal_id}_cds",
            f"{portal_id}_cds.fna",
            [
                (f"jgi|{portal_id}|2001|modelA", cds_a),
                (f"jgi|{portal_id}|2002|modelB", cds_b),
            ],
        )

    # ── 7. stage ─────────────────────────────────────────────────────────
    res = runner.invoke(app, ["stage", str(project_dir)])
    assert res.exit_code == 0, res.output

    # Verify snapshot was created
    conn = connect(paths.db_path)
    staging_rows = conn.execute("SELECT staging_id FROM stagings").fetchall()
    assert len(staging_rows) == 1
    staging_id = staging_rows[0]["staging_id"]

    # Verify staging_files has entries for both portals, both kinds
    sf_rows = conn.execute(
        "SELECT portal_id, kind FROM staging_files WHERE staging_id = ? ORDER BY portal_id, kind",
        (staging_id,),
    ).fetchall()
    conn.close()

    staged = [(r["portal_id"], r["kind"]) for r in sf_rows]
    assert ("AlphaFung1", "cds") in staged
    assert ("AlphaFung1", "proteome") in staged
    assert ("BetaFung1", "cds") in staged or ("BetaFung2", "cds") in staged
    assert ("BetaFung2", "proteome") in staged

    # Verify actual FASTA files exist on disk
    for portal_id in ("AlphaFung1", "BetaFung2"):
        prot = paths.staging_proteomes_dir(staging_id) / f"{portal_id}.faa"
        assert prot.exists(), f"Missing proteome: {prot}"
        content = prot.read_text(encoding="utf-8")
        assert f"{portal_id}|" in content  # canonical headers present

    # Verify manifest exists and is valid JSON
    manifest_path = paths.staging_manifest(staging_id)
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["staging_id"] == staging_id
    assert len(manifest["actions"]) >= 4  # 2 portals × 2 kinds

    # Verify checksums file
    checksums = paths.staging_checksums(staging_id)
    assert checksums.exists()
    checksum_lines = checksums.read_text(encoding="utf-8").strip().splitlines()
    assert len(checksum_lines) >= 4  # at least 2 proteomes + 2 CDS
