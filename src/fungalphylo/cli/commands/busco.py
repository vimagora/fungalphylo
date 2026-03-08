from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.busco import parse_batch_summary, resolve_batch_root, resolve_batch_summary
from fungalphylo.core.events import log_event
from fungalphylo.core.manifest import read_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect, init_db

app = typer.Typer(help="Inspect and ingest BUSCO result summaries.")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _latest_busco_run_id(paths: ProjectPaths) -> Optional[str]:
    conn = connect(paths.db_path)
    try:
        row = conn.execute(
            """
            SELECT run_id
            FROM runs
            WHERE kind = 'busco'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else str(row["run_id"])


@app.command("ingest-results")
def ingest_results(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="BUSCO run ID to ingest (default: latest BUSCO run)"),
    batch_summary: Optional[Path] = typer.Option(
        None, "--batch-summary", help="Override BUSCO batch_summary.txt path"
    ),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    selected_run_id = run_id or _latest_busco_run_id(paths)
    if selected_run_id is None:
        raise typer.BadParameter("No BUSCO run found. Generate a BUSCO run first or pass --run-id.")

    manifest = read_manifest(paths.run_manifest(selected_run_id))
    if manifest.get("kind") != "busco":
        raise typer.BadParameter(f"Run {selected_run_id} is not a BUSCO run.")

    summary_path = batch_summary.expanduser().resolve() if batch_summary else resolve_batch_summary(paths, selected_run_id, manifest)
    if not summary_path.exists():
        raise typer.BadParameter(f"BUSCO batch summary not found: {summary_path}")

    batch_root = resolve_batch_root(paths, selected_run_id, manifest)
    imported_at = _now()
    rows = parse_batch_summary(summary_path)
    if not rows:
        raise typer.BadParameter(f"BUSCO batch summary is empty: {summary_path}")

    conn = connect(paths.db_path)
    try:
        conn.execute("DELETE FROM busco_results WHERE run_id = ?", (selected_run_id,))
        for row in rows:
            portal_dir = batch_root / row["input_filename"]
            short_json = portal_dir / f"short_summary.specific.{row['lineage']}.{row['input_filename']}.json"
            short_txt = portal_dir / f"short_summary.specific.{row['lineage']}.{row['input_filename']}.txt"
            conn.execute(
                """
                INSERT INTO busco_results(
                  run_id, portal_id, input_filename, lineage, complete_pct, single_pct, duplicated_pct,
                  fragmented_pct, missing_pct, n_markers, batch_summary_path, portal_result_dir,
                  short_summary_json_path, short_summary_txt_path, imported_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    selected_run_id,
                    row["portal_id"],
                    row["input_filename"],
                    row["lineage"],
                    row["complete_pct"],
                    row["single_pct"],
                    row["duplicated_pct"],
                    row["fragmented_pct"],
                    row["missing_pct"],
                    row["n_markers"],
                    str(summary_path.relative_to(project_dir)),
                    str(portal_dir.relative_to(project_dir)) if portal_dir.exists() else None,
                    str(short_json.relative_to(project_dir)) if short_json.exists() else None,
                    str(short_txt.relative_to(project_dir)) if short_txt.exists() else None,
                    imported_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": imported_at,
            "event": "busco_ingest_results",
            "run_id": selected_run_id,
            "batch_summary": str(summary_path),
            "n_rows": len(rows),
        },
    )
    typer.echo(f"Ingested {len(rows)} BUSCO summary rows for run {selected_run_id}")
