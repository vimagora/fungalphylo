from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from fungalphylo.cli.commands.autoselect import Candidate, score_candidate
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


def _insert_portal(paths: ProjectPaths, portal_id: str = "PortalA") -> None:
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portals(portal_id, name, created_at, published_text, published_url, is_published, meta_json)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                portal_id,
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


def _insert_file(
    paths: ProjectPaths,
    *,
    file_id: str,
    portal_id: str,
    kind: str,
    filename: str,
    size_bytes: int,
    meta: dict,
) -> None:
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                file_id,
                portal_id,
                kind,
                filename,
                size_bytes,
                None,
                _now(),
                json.dumps(meta, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _latest_autoselect_output(review_dir: Path, pattern: str) -> Path:
    matches = sorted(p for p in review_dir.glob(pattern) if "explain" not in p.name)
    assert matches, f"No files matched {pattern}"
    return matches[-1]


def test_score_candidate_respects_explicit_weights() -> None:
    candidate = Candidate(
        file_id="file1",
        portal_id="PortalA",
        kind="proteome",
        filename="proteins.faa",
        size_bytes=2_000_000_000,
        md5=None,
        meta={},
        jat_label="proteins_filtered",
        file_format="fasta",
        data_group="genome",
        modified_date=datetime.now(timezone.utc),
        file_date=None,
        file_status="RESTORED",
    )

    default_score, _ = score_candidate(candidate, "proteome")
    custom_score, why = score_candidate(
        candidate,
        "proteome",
        weights={"proteome_label_filtered": 5.0, "data_group_genome": 1.0, "size_bonus_cap": 1.0},
    )

    assert default_score > custom_score
    assert why["jat_proteins_filtered"] == 5.0
    assert why["data_group_genome"] == 1.0


def test_autoselect_config_weights_change_ranking(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths)

    _insert_file(
        paths,
        file_id="filtered",
        portal_id="PortalA",
        kind="proteome",
        filename="proteins_filtered.faa",
        size_bytes=10,
        meta={
            "jat_label": "proteins_filtered",
            "file_format": "fasta",
            "data_group": "transcriptome",
            "file_status": "PURGED",
            "modified_date": _now(),
        },
    )
    _insert_file(
        paths,
        file_id="generic",
        portal_id="PortalA",
        kind="proteome",
        filename="protein_generic.faa",
        size_bytes=10,
        meta={
            "jat_label": "protein",
            "file_format": "fasta",
            "data_group": "genome",
            "file_status": "RESTORED",
            "modified_date": _now(),
        },
    )

    config = paths.config_yaml.read_text(encoding="utf-8")
    config += """
autoselect:
  weights:
    proteome_label_filtered: 0.0
    proteome_label_generic: 5.0
    data_group_genome: 80.0
    status_restored: 20.0
    status_purged_penalty: -10.0
"""
    paths.config_yaml.write_text(config, encoding="utf-8")

    result = runner.invoke(app, ["autoselect", str(project_dir)])
    assert result.exit_code == 0, result.output

    selection_path = _latest_autoselect_output(project_dir / "review", "autoselect_*.tsv")
    with selection_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    assert len(rows) == 1
    assert rows[0]["proteome_file_id"] == "generic"


def test_autoselect_config_ban_patterns_exclude_candidates(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    paths = _init_project(project_dir)
    _insert_portal(paths)

    _insert_file(
        paths,
        file_id="bad",
        portal_id="PortalA",
        kind="proteome",
        filename="forbidden_special.faa",
        size_bytes=10,
        meta={"jat_label": "proteins_filtered", "file_format": "fasta", "data_group": "genome", "file_status": "RESTORED"},
    )
    _insert_file(
        paths,
        file_id="ok",
        portal_id="PortalA",
        kind="proteome",
        filename="clean.faa",
        size_bytes=10,
        meta={"jat_label": "protein", "file_format": "fasta", "data_group": "genome", "file_status": "RESTORED"},
    )

    config = paths.config_yaml.read_text(encoding="utf-8")
    config += """
autoselect:
  ban_patterns:
    - forbidden_special
"""
    paths.config_yaml.write_text(config, encoding="utf-8")

    result = runner.invoke(app, ["autoselect", str(project_dir)])
    assert result.exit_code == 0, result.output

    explain_path = sorted((project_dir / "review").glob("autoselect_explain_*.tsv"))[-1]
    with explain_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    hard_excluded = next(row for row in rows if row["file_id"] == "bad" and row["target"] == "proteome")
    assert "bad_keyword" in hard_excluded["explain_json"]
