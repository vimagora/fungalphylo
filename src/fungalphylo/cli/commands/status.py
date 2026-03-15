from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.table import Table as RichTable

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.hash import file_matches_md5
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.db.db import connect, init_db

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
    init_db(paths.db_path)

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
        portals_with_taxon = conn.execute(
            "SELECT COUNT(*) AS n FROM portals WHERE ncbi_taxon_id IS NOT NULL"
        ).fetchone()["n"]
        approved_with_taxon = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM approvals a
            JOIN portals p ON p.portal_id = a.portal_id
            WHERE p.ncbi_taxon_id IS NOT NULL
            """
        ).fetchone()["n"]

        # Recent staging count
        stagings_n = conn.execute("SELECT COUNT(*) AS n FROM stagings").fetchone()["n"]
        latest_staging = conn.execute(
            "SELECT staging_id, created_at, manifest_path FROM stagings ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        latest_restore_row = conn.execute(
            """
            SELECT request_id, created_at, request_dir, dry_run, status, n_payloads, n_posted, n_errors
            FROM restore_requests
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        latest_download_row = conn.execute(
            """
            SELECT request_id, created_at, request_dir, dry_run, status, n_payloads, n_payload_ok, n_errors, moved_files, missing_files
            FROM download_requests
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()

        # For raw cache completeness: approvals joined with portal_files to get filenames
        rows = conn.execute(
            """
            SELECT
              a.portal_id,
              a.proteome_file_id, pf1.filename AS proteome_filename, pf1.md5 AS proteome_md5,
              a.cds_file_id, pf2.filename AS cds_filename, pf2.md5 AS cds_md5
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
    mismatched = 0
    missing_samples: List[str] = []
    mismatched_samples: List[str] = []

    for r in rows:
        pid = r["portal_id"]

        prot_raw = resolve_raw_path(
            project_dir,
            raw_layout=raw_layout,
            portal_id=pid,
            file_id=r["proteome_file_id"],
            filename=r["proteome_filename"],
        )
        if file_matches_md5(prot_raw, r["proteome_md5"]):
            present += 1
        elif prot_raw.exists():
            mismatched += 1
            if len(mismatched_samples) < sample_missing:
                mismatched_samples.append(f"{pid}\tproteome\t{prot_raw}")
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
            if file_matches_md5(cds_raw, r["cds_md5"]):
                present += 1
            elif cds_raw.exists():
                mismatched += 1
                if len(mismatched_samples) < sample_missing:
                    mismatched_samples.append(f"{pid}\tcds\t{cds_raw}")
            else:
                missing += 1
                if len(missing_samples) < sample_missing:
                    missing_samples.append(f"{pid}\tcds\t{cds_raw}")

    def ledger_batch_summary(row, kind: str) -> Dict[str, str]:
        if row is None:
            return {"dir": "-", "status": "-", "payloads": "0", "result": "-"}
        request_dir = project_dir / row["request_dir"]
        status = row["status"]
        if row["dry_run"]:
            status = f"{status} (dry-run)"
        if kind == "restore":
            result = f"posted={row['n_posted']} errors={row['n_errors']}"
        else:
            result = (
                f"ok={row['n_payload_ok']} errors={row['n_errors']} "
                f"moved={row['moved_files']} missing={row['missing_files']}"
            )
        return {"dir": str(request_dir), "status": status, "payloads": str(row["n_payloads"]), "result": result}

    restore_info = ledger_batch_summary(latest_restore_row, "restore")
    download_info = ledger_batch_summary(latest_download_row, "download")

    # Render
    console.print(f"[bold]Project:[/bold] {project_dir}")
    console.print(f"[dim]DB:[/dim] {paths.db_path}")
    console.print()

    t1 = RichTable(title="Portals & Approvals", show_lines=False)
    t1.add_column("Metric", style="bold")
    t1.add_column("Value")
    t1.add_row("Portals (total)", str(total_portals))
    t1.add_row("Portals (published)", str(published_portals))
    t1.add_row("Portals with NCBI taxon ID", str(portals_with_taxon))
    t1.add_row("Approvals", str(approvals_n))
    t1.add_row("Approvals with NCBI taxon ID", str(approved_with_taxon))
    t1.add_row("Staging snapshots", str(stagings_n))
    console.print(t1)
    console.print()

    t2 = RichTable(title="Raw cache completeness (approved files)", show_lines=False)
    t2.add_column("Present", justify="right")
    t2.add_column("Checksum mismatch", justify="right")
    t2.add_column("Missing", justify="right")
    t2.add_row(str(present), str(mismatched), str(missing))
    console.print(t2)

    if missing_samples:
        console.print("\n[bold]Missing raw files (sample):[/bold]")
        for line in missing_samples:
            console.print("  " + line)

    if mismatched_samples:
        console.print("\n[bold]Checksum-mismatched raw files (sample):[/bold]")
        for line in mismatched_samples:
            console.print("  " + line)

    console.print()

    t3 = RichTable(title="Latest restore/download batches", show_lines=False)
    t3.add_column("Type", style="bold")
    t3.add_column("Dir")
    t3.add_column("Status")
    t3.add_column("# payloads", justify="right")
    t3.add_column("Result")
    t3.add_row("restore", restore_info["dir"], restore_info["status"], restore_info["payloads"], restore_info["result"])
    t3.add_row("download", download_info["dir"], download_info["status"], download_info["payloads"], download_info["result"])
    console.print(t3)
    console.print()

    if latest_staging:
        sid = latest_staging["staging_id"]
        console.print("[bold]Latest staging:[/bold]")
        console.print(f"  id: {sid}")
        console.print(f"  created_at: {latest_staging['created_at']}")
        console.print(f"  manifest: {project_dir / latest_staging['manifest_path']}")
        console.print(f"  proteomes: {paths.staging_proteomes_dir(sid)}")
        console.print(f"  cds: {paths.staging_cds_dir(sid)}")
        console.print(f"  checksums: {paths.staging_checksums(sid)}")

        # Show staging artifact counts
        conn2 = connect(paths.db_path)
        try:
            sf_count = conn2.execute(
                "SELECT COUNT(*) AS n FROM staging_files WHERE staging_id = ?", (sid,)
            ).fetchone()["n"]
            sf_reused = conn2.execute(
                "SELECT COUNT(*) AS n FROM staging_files WHERE staging_id = ? AND reused_from_staging_id IS NOT NULL",
                (sid,),
            ).fetchone()["n"]
        finally:
            conn2.close()
        console.print(f"  artifacts: {sf_count} (reused: {sf_reused})")

        # Check manifest for failures
        manifest_path = project_dir / latest_staging["manifest_path"]
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            failures = manifest.get("failures") or []
            if failures:
                console.print(f"  [red]staging failures: {len(failures)}[/red]")
                for f in failures[:5]:
                    console.print(f"    {f['portal_id']}: {f['reason']}")
                if len(failures) > 5:
                    console.print(f"    ... and {len(failures) - 5} more")
    else:
        console.print("[bold]Latest staging:[/bold] -")


failures_app = typer.Typer(help="Inspect recent failures across restore, download, stage, and error logs.")


@failures_app.callback(invoke_without_command=True)
def failures_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    last_n: int = typer.Option(50, "--last", help="Number of recent error log entries to show"),
) -> None:
    """Inspect recent failures across restore, download, stage, and error logs."""
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    found_any = False

    # 1. Failed or partial restore/download batches
    conn = connect(paths.db_path)
    try:
        bad_restores = conn.execute(
            """
            SELECT request_id, created_at, status, n_payloads, n_posted, n_errors
            FROM restore_requests
            WHERE status IN ('partial', 'failed')
            ORDER BY created_at DESC
            LIMIT 5
            """
        ).fetchall()
        bad_downloads = conn.execute(
            """
            SELECT request_id, created_at, status, n_payloads, n_payload_ok, n_errors, moved_files, missing_files
            FROM download_requests
            WHERE status IN ('partial', 'failed')
            ORDER BY created_at DESC
            LIMIT 5
            """
        ).fetchall()
    finally:
        conn.close()

    if bad_restores:
        found_any = True
        t = RichTable(title="Failed/Partial Restore Batches", show_lines=False)
        t.add_column("request_id")
        t.add_column("created_at")
        t.add_column("status")
        t.add_column("payloads", justify="right")
        t.add_column("posted", justify="right")
        t.add_column("errors", justify="right")
        for r in bad_restores:
            t.add_row(
                r["request_id"], r["created_at"], r["status"],
                str(r["n_payloads"]), str(r["n_posted"]), str(r["n_errors"]),
            )
        console.print(t)
        console.print()

    if bad_downloads:
        found_any = True
        t = RichTable(title="Failed/Partial Download Batches", show_lines=False)
        t.add_column("request_id")
        t.add_column("created_at")
        t.add_column("status")
        t.add_column("payloads", justify="right")
        t.add_column("ok", justify="right")
        t.add_column("errors", justify="right")
        t.add_column("moved", justify="right")
        t.add_column("missing", justify="right")
        for r in bad_downloads:
            t.add_row(
                r["request_id"], r["created_at"], r["status"],
                str(r["n_payloads"]), str(r["n_payload_ok"]), str(r["n_errors"]),
                str(r["moved_files"]), str(r["missing_files"]),
            )
        console.print(t)
        console.print()

    # 2. Latest staging manifest failures
    conn = connect(paths.db_path)
    try:
        latest_staging = conn.execute(
            "SELECT staging_id, manifest_path FROM stagings ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    if latest_staging:
        manifest_path = project_dir / latest_staging["manifest_path"]
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            failures = manifest.get("failures") or []
            if failures:
                found_any = True
                t = RichTable(
                    title=f"Staging Failures ({latest_staging['staging_id']})",
                    show_lines=False,
                )
                t.add_column("portal_id")
                t.add_column("reason")
                for f in failures:
                    t.add_row(f["portal_id"], f["reason"])
                console.print(t)
                console.print()

        # Check for failed_portals.tsv
        failed_report = paths.staging_reports_dir(latest_staging["staging_id"]) / "failed_portals.tsv"
        if failed_report.exists():
            console.print(f"[dim]Detailed staging failure report:[/dim] {failed_report}")
            console.print()

    # 3. Recent errors from errors.jsonl
    errors_log = paths.errors_log
    if errors_log.exists():
        lines: List[str] = []
        with errors_log.open("r", encoding="utf-8") as f:
            for line in f:
                lines.append(line)
        recent = lines[-last_n:] if len(lines) > last_n else lines
        if recent:
            found_any = True
            records = []
            for line in recent:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

            # Summary by event type
            event_counts: Counter[str] = Counter()
            for rec in records:
                event_counts[rec.get("event", "unknown")] += 1

            t = RichTable(title=f"Recent Error Log ({len(records)} entries)", show_lines=False)
            t.add_column("event")
            t.add_column("count", justify="right")
            for event, count in event_counts.most_common():
                t.add_row(event, str(count))
            console.print(t)
            console.print()

            # Show last few errors with detail
            show_n = min(5, len(records))
            console.print(f"[bold]Last {show_n} errors:[/bold]")
            for rec in records[-show_n:]:
                ts = rec.get("ts", "?")
                event = rec.get("event", "?")
                portal = rec.get("portal_id", "")
                exc_type = rec.get("exc_type", "")
                exc_msg = rec.get("exc_msg", "")
                portal_str = f" [{portal}]" if portal else ""
                console.print(f"  {ts}  {event}{portal_str}  {exc_type}: {exc_msg}")
            console.print()
            console.print(f"[dim]Full error log:[/dim] {errors_log}")

    if not found_any:
        console.print("[green]No failures found.[/green]")
