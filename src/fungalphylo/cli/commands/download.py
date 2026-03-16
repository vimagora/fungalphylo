from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import requests
import typer
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.download import (
    build_blocks,
    chunk_payloads,
    find_manifest_csv,
    move_files_using_manifest,
    post_download_with_retries,
    safe_download_name,
    save_and_extract_zip_bundle,
)
from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.hash import file_matches_md5
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.jgi_auth import get_token
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.db.db import connect, init_db

app = typer.Typer(help="Download approved JGI files into raw/ via immutable batch directories; staging remains the normalized source.")


@app.callback(invoke_without_command=True)
def download_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    token: str | None = typer.Option(None, "--token", help="JGI token (else uses env JGI_TOKEN)."),
    portal_id: list[str] | None = typer.Option(None, "--portal-id", help="Limit to specific portal IDs."),
    max_chars: int = typer.Option(3500, "--max-chars", help="Max JSON character length per download request."),
    timeout: int = typer.Option(300, "--timeout", help="HTTP timeout seconds per request."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build payloads but do not download."),
    overwrite_staged: bool = typer.Option(
        False,
        "--overwrite-staged",
        help="If the approved file already exists in any staging snapshot, download again anyway.",
    ),
    skip_if_raw_present: bool = typer.Option(
        False,
        "--skip-if-raw-present",
        help="Skip downloading if the raw file already exists and, when source md5 is known, matches the approved file.",
    ),
    continue_on_error: bool = typer.Option(True, "--continue-on-error/--fail-fast", help="Continue if one payload fails."),
    retries: int = typer.Option(2, "--retries", min=0, help="Retries for transient download failures (429/5xx/timeouts)."),
    retry_backoff_seconds: float = typer.Option(
        2.0,
        "--retry-backoff-seconds",
        min=0.0,
        help="Base backoff in seconds for transient download retries.",
    ),
    retain: str = typer.Option("manifest", "--retain", help="manifest (default), zip, or all."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")
    if retain not in {"manifest", "zip", "all"}:
        raise typer.BadParameter("--retain must be one of: manifest, zip, all")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    cfg = resolve_config(project_config=load_yaml(paths.config_yaml))
    raw_layout = cfg["staging"]["raw_layout"]

    tok: str | None = None if dry_run else get_token(token)
    errors_log = paths.errors_log

    # Track all staged source file IDs per portal/kind so any matching snapshot can suppress a re-download.
    conn = connect(paths.db_path)
    try:
        staged = conn.execute(
            """
            SELECT DISTINCT portal_id, kind, source_file_id AS file_id
            FROM staging_files
            """
        ).fetchall()
    finally:
        conn.close()
    staged_file_ids: dict[tuple[str, str], set[str]] = {}
    for r in staged:
        key = (r["portal_id"], r["kind"])
        staged_file_ids.setdefault(key, set()).add(r["file_id"])

    # approvals + filenames
    conn = connect(paths.db_path)
    try:
        params: list[object] = []
        where = ""
        if portal_id:
            where = f"WHERE a.portal_id IN ({','.join('?' for _ in portal_id)})"
            params.extend(portal_id)

        rows = conn.execute(
            f"""
            SELECT
              a.portal_id,
              a.proteome_file_id,
              pf1.filename AS proteome_filename,
              pf1.md5 AS proteome_md5,
              a.cds_file_id,
              pf2.filename AS cds_filename,
              pf2.md5 AS cds_md5,
              p.dataset_id,
              p.top_hit_id
            FROM approvals a
            JOIN portals p ON p.portal_id = a.portal_id
            JOIN portal_files pf1 ON pf1.file_id = a.proteome_file_id
            LEFT JOIN portal_files pf2 ON pf2.file_id = a.cds_file_id
            {where}
            ORDER BY a.portal_id
            """,
            params,
        ).fetchall()

        if not rows:
            raise typer.BadParameter("No approvals found. Run review apply first.")

        missing_meta = [r["portal_id"] for r in rows if not r["dataset_id"] or not r["top_hit_id"]]
        if missing_meta:
            raise RuntimeError("Missing dataset_id/top_hit_id for portals: " + ", ".join(missing_meta))

        norm_rows = []
        for r in rows:
            pid = r["portal_id"]

            prot_id = r["proteome_file_id"]
            prot_fn = r["proteome_filename"]
            prot_md5 = r["proteome_md5"]

            cds_id = r["cds_file_id"]
            cds_fn = r["cds_filename"]
            cds_md5 = r["cds_md5"]

            # skip if the approved file already exists in any staging snapshot
            if not overwrite_staged:
                if prot_id and prot_id in staged_file_ids.get((pid, "proteome"), set()):
                    prot_id = None
                    prot_fn = None
                if cds_id and cds_id in staged_file_ids.get((pid, "cds"), set()):
                    cds_id = None
                    cds_fn = None

            # optional: skip if raw file already exists
            if skip_if_raw_present:
                if prot_id and prot_fn:
                    raw_prot = resolve_raw_path(
                        project_dir,
                        raw_layout=raw_layout,
                        portal_id=pid,
                        file_id=prot_id,
                        filename=prot_fn,
                    )
                    if file_matches_md5(raw_prot, prot_md5):
                        prot_id = None
                        prot_fn = None

                if cds_id and cds_fn:
                    raw_cds = resolve_raw_path(
                        project_dir,
                        raw_layout=raw_layout,
                        portal_id=pid,
                        file_id=cds_id,
                        filename=cds_fn,
                    )
                    if file_matches_md5(raw_cds, cds_md5):
                        cds_id = None
                        cds_fn = None

            if prot_id is None and (cds_id is None or cds_id == ""):
                continue

            norm_rows.append(
                {
                    "portal_id": pid,
                    "proteome_file_id": prot_id,
                    "cds_file_id": cds_id,
                    "dataset_id": str(r["dataset_id"]),
                    "top_hit_id": str(r["top_hit_id"]),
                }
            )
    finally:
        conn.close()

    if not norm_rows:
        typer.echo("Nothing to download (already present in staging snapshot(s) and/or raw cache).")
        return

    blocks = build_blocks(norm_rows)
    payloads = chunk_payloads(blocks, max_chars=max_chars)

    request_id = now_tag()
    out_dir = project_dir / "download_requests" / request_id
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, payload in enumerate(payloads, start=1):
        (out_dir / f"payload_{i:03d}.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    typer.echo(f"Wrote {len(payloads)} download payload(s) to: {out_dir}")
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO download_requests(
              request_id, created_at, request_dir, dry_run, status,
              n_payloads, n_payload_ok, n_errors, moved_files, missing_files,
              max_chars, timeout_seconds, continue_on_error, skip_if_raw_present,
              overwrite_staged, retain
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                request_id,
                now_iso(),
                str(out_dir.relative_to(project_dir)),
                1 if dry_run else 0,
                "planned" if dry_run else "running",
                len(payloads),
                0,
                0,
                0,
                0,
                max_chars,
                timeout,
                1 if continue_on_error else 0,
                1 if skip_if_raw_present else 0,
                1 if overwrite_staged else 0,
                retain,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    if dry_run:
        typer.echo("Dry-run complete (no downloads).")
        log_event(
            project_dir,
            {
                "ts": now_iso(),
                "event": "download",
                "dry_run": True,
                "n_payloads": len(payloads),
                "out_dir": str(out_dir),
                "skip_if_raw_present": skip_if_raw_present,
                "overwrite_staged": overwrite_staged,
                "retries": retries,
                "retry_backoff_seconds": retry_backoff_seconds,
            },
        )
        return

    bundles_dir = out_dir / "bundles"
    bundles_dir.mkdir(parents=True, exist_ok=True)

    n_errors = 0
    n_payload_ok = 0
    moved_total = 0
    missing_total = 0
    fatal_error: BaseException | None = None

    with Progress(
        TextColumn("Payload:"),
        TextColumn("{task.fields[payload]:<10}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Downloading", total=len(payloads), payload="-" * 10)

        for i, payload in enumerate(payloads, start=1):
            progress.update(task, payload=f"{i:03d}/{len(payloads):03d}".ljust(10))
            try:
                assert tok is not None
                resp = post_download_with_retries(
                    payload,
                    token=tok,
                    timeout=timeout,
                    retries=retries,
                    retry_backoff_seconds=retry_backoff_seconds,
                    log_retry=lambda attempt, max_attempts, delay, exc: progress.console.log(
                        f"[yellow]RETRY[/yellow] payload {i}: attempt {attempt + 1}/{max_attempts} in {delay:.1f}s after {type(exc).__name__}"
                    ),
                )

                zip_name = f"bundle_{i:03d}.zip"
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    name = cd.split("filename=")[-1].strip().strip('"')
                    zip_name = safe_download_name(name)
                    if not zip_name.lower().endswith(".zip"):
                        zip_name = zip_name + ".zip"

                zip_path, extracted_root = save_and_extract_zip_bundle(resp, bundles_dir, zip_name)

                manifest_csv = find_manifest_csv(extracted_root)
                content_root = extracted_root
                if manifest_csv is not None:
                    content_root = manifest_csv.parent
                if manifest_csv is None:
                    raise RuntimeError(f"No manifest CSV found in extracted zip for payload {i} at {extracted_root}")

                kept_manifest = out_dir / f"manifest_{i:03d}.csv"

                moved, missing, _unmatched_path = move_files_using_manifest(
                    content_root=content_root,
                    manifest_csv=manifest_csv,
                    paths=paths,
                    keep_manifest_to=kept_manifest,
                )
                moved_total += moved
                missing_total += missing
                n_payload_ok += 1

                if retain == "manifest":
                    shutil.rmtree(extracted_root, ignore_errors=True)
                    try:
                        zip_path.unlink()
                    except Exception:
                        pass
                elif retain == "zip":
                    shutil.rmtree(extracted_root, ignore_errors=True)
                else:
                    pass

            except requests.HTTPError as e:
                n_errors += 1
                resp = getattr(e, "response", None)
                log_error_jsonl(
                    errors_log,
                    {
                        "event": "download_error",
                        "stage": "http",
                        "payload_i": i,
                        "payload_path": str(out_dir / f"payload_{i:03d}.json"),
                        "status_code": getattr(resp, "status_code", None),
                        "response_text": (resp.text[:800] if resp is not None and getattr(resp, "text", None) else None),
                        **exception_record(e),
                    },
                )
                progress.console.log(f"[red]ERROR[/red] payload {i}: HTTP error (logged).")
                if not continue_on_error:
                    fatal_error = e
                    break

            except Exception as e:
                n_errors += 1
                log_error_jsonl(
                    errors_log,
                    {
                        "event": "download_error",
                        "stage": "unknown",
                        "payload_i": i,
                        "payload_path": str(out_dir / f"payload_{i:03d}.json"),
                        **exception_record(e),
                    },
                )
                progress.console.log(f"[red]ERROR[/red] payload {i}: {type(e).__name__} (logged).")
                if not continue_on_error:
                    fatal_error = e
                    break

            finally:
                progress.advance(task)

    summary: dict[str, Any] = {
        "ts": now_iso(),
        "n_payloads": len(payloads),
        "n_payload_ok": n_payload_ok,
        "n_errors": n_errors,
        "moved_files": moved_total,
        "missing_files": missing_total,
        "retain": retain,
        "raw_dir": str(paths.raw_dir),
        "skip_if_raw_present": skip_if_raw_present,
        "overwrite_staged": overwrite_staged,
        "retries": retries,
        "retry_backoff_seconds": retry_backoff_seconds,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "download",
            "dry_run": False,
            "n_payloads": len(payloads),
            "n_payload_ok": n_payload_ok,
            "n_errors": n_errors,
            "out_dir": str(out_dir),
            "retain": retain,
            "moved_files": moved_total,
            "missing_files": missing_total,
            "skip_if_raw_present": skip_if_raw_present,
            "overwrite_staged": overwrite_staged,
            "retries": retries,
            "retry_backoff_seconds": retry_backoff_seconds,
        },
    )

    final_status = "completed" if n_errors == 0 else ("partial" if n_payload_ok > 0 else "failed")
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            UPDATE download_requests
            SET status=?, n_payload_ok=?, n_errors=?, moved_files=?, missing_files=?
            WHERE request_id=?
            """,
            (final_status, n_payload_ok, n_errors, moved_total, missing_total, request_id),
        )
        conn.commit()
    finally:
        conn.close()

    typer.echo(f"Done. Payloads OK: {n_payload_ok}/{len(payloads)}. Errors: {n_errors}.")
    typer.echo(f"Moved into raw/: {moved_total}. Missing/unmoved entries: {missing_total}.")
    typer.echo(f"Kept in download_requests: payloads + manifest(s) + summary (retain={retain}).")

    if fatal_error is not None:
        raise fatal_error
