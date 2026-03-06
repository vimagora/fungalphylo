from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.table import Table as RichTable

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.db.db import connect

app = typer.Typer(help="Show project status: portals, approvals, raw cache, restore/download batches, staging snapshots.")

console = Console()


def _latest_subdir(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    dirs = [p for p in path.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return sorted(dirs, key=lambda p: p.name)[-1]


def _read_first_n_lines(path: Path, n: int = 10) -> List[str]:
    lines: List[str] = []
    if not path.exists():
        return lines
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            lines.append(line.rstrip("\n"))
            if i + 1 >= n:
                break
    return lines


def _count_files_in_dir(path: Path, suffixes: Tuple[str, ...] = (".json", ".jsonl", ".bin", ".zip", ".gz")) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file() and p.suffix.lower() in suffixes)


@app.callback(invoke_without_command=True)
def status_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    sample_missing: int = typer.Option(10, "--sample-missing", help="How many missing raw files to list"),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    # Load config (for raw_layout)
    cfg = resolve_config(project_config=load_yaml(paths.config_yaml))
    raw_layout = cfg.get("staging", {}).get("raw_layout", "raw/{portal_id}/{file_id}/{filename}")

    conn = connect(paths.db_path)
    try:
        # Portal counts
        total_portals = conn.execute("SELECT COUNT(*) AS n FROM portals").fetchone()["n"]
        published_portals = conn.execute("SELECT COUNT(*) AS n FROM portals WHERE is_published=1").fetchone()["n"]

        # Approvals count
        approvals_n = conn.execute("SELECT COUNT(*) AS n FROM approvals").fetchone()["n"]

        # Recent staging count
        stagings_n = conn.execute("SELECT COUNT(*) AS n FROM stagings").fetchone()["n"]
        latest_staging = conn.execute(
            "SELECT staging_id, created_at, manifest_path FROM stagings ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        # For raw cache completeness: approvals joined with portal_files to get filenames
        rows = conn.execute(
            """
            SELECT
              a.portal_id,
              a.proteome_file_id, pf1.filename AS proteome_filename,
              a.cds_file_id, pf2.filename AS cds_filename
            FROM approvals a
            JOIN portal_files pf1 ON pf1.file_id = a.proteome_file_id
            LEFT JOIN portal_files pf2 ON pf2.file_id = a.cds_file_id
            ORDER BY a.portal_id
            """
        ).fetchall()
    finally:
        conn.close()

    # Compute raw presence
    present = 0
    missing = 0
    missing_samples: List[str] = []

    for r in rows:
        pid = r["portal_id"]

        prot_raw = resolve_raw_path(
            project_dir,
            raw_layout=raw_layout,
            portal_id=pid,
            file_id=r["proteome_file_id"],
            filename=r["proteome_filename"],
        )
        if prot_raw.exists():
            present += 1
        else:
            missing += 1
            if len(missing_samples) < sample_missing:
                missing_samples.append(f"{pid}\tproteome\t{prot_raw}")

        if r["cds_file_id"] and r["cds_filename"]:
            cds_raw = resolve_raw_path(
                project_dir,
                raw_layout=raw_layout,
                portal_id=pid,
                file_id=r["cds_file_id"],
                filename=r["cds_filename"],
            )
            if cds_raw.exists():
                present += 1
            else:
                missing += 1
                if len(missing_samples) < sample_missing:
                    missing_samples.append(f"{pid}\tcds\t{cds_raw}")

    # Find latest restore/download batches
    restore_root = project_dir / "restore_requests"
    download_root = project_dir / "download_requests"
    latest_restore = _latest_subdir(restore_root)
    latest_download = _latest_subdir(download_root)

    # Summaries for latest batches
    def batch_summary(batch_dir: Optional[Path]) -> Dict[str, str]:
        if not batch_dir:
            return {"dir": "-", "payloads": "0", "responses": "0"}
        payloads = len(list(batch_dir.glob("payload_*.json")))
        responses = 1 if (batch_dir / "responses.jsonl").exists() else 0
        return {"dir": str(batch_dir), "payloads": str(payloads), "responses": str(responses)}

    restore_info = batch_summary(latest_restore)
    download_info = batch_summary(latest_download)

    # Render
    console.print(f"[bold]Project:[/bold] {project_dir}")
    console.print(f"[dim]DB:[/dim] {paths.db_path}")
    console.print()

    t1 = RichTable(title="Portals & Approvals", show_lines=False)
    t1.add_column("Metric", style="bold")
    t1.add_column("Value")
    t1.add_row("Portals (total)", str(total_portals))
    t1.add_row("Portals (published)", str(published_portals))
    t1.add_row("Approvals", str(approvals_n))
    t1.add_row("Staging snapshots", str(stagings_n))
    console.print(t1)
    console.print()

    t2 = RichTable(title="Raw cache completeness (approved files)", show_lines=False)
    t2.add_column("Present", justify="right")
    t2.add_column("Missing", justify="right")
    t2.add_row(str(present), str(missing))
    console.print(t2)

    if missing_samples:
        console.print("\n[bold]Missing raw files (sample):[/bold]")
        for line in missing_samples:
            console.print("  " + line)

    console.print()

    t3 = RichTable(title="Latest restore/download batches", show_lines=False)
    t3.add_column("Type", style="bold")
    t3.add_column("Dir")
    t3.add_column("# payloads", justify="right")
    t3.add_column("responses.jsonl", justify="right")
    t3.add_row("restore", restore_info["dir"], restore_info["payloads"], restore_info["responses"])
    t3.add_row("download", download_info["dir"], download_info["payloads"], download_info["responses"])
    console.print(t3)
    console.print()

    if latest_staging:
        console.print("[bold]Latest staging:[/bold]")
        console.print(f"  id: {latest_staging['staging_id']}")
        console.print(f"  created_at: {latest_staging['created_at']}")
        console.print(f"  manifest: {project_dir / latest_staging['manifest_path']}")
    else:
        console.print("[bold]Latest staging:[/bold] -")