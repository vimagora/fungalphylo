from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import requests
import typer
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)

from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.jgi_auth import get_token
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect

app = typer.Typer(help="Fetch and cache JGI Files search index; ingest results into portal_files.")

SEARCH_URL = "https://files.jgi.doe.gov/search/"
PORTAL_WIDTH = 16


def classify_kind(file_name: str, file_format: str, jat_label: str, file_type: Any) -> str:
    name = (file_name or "").lower()
    fmt = (file_format or "").lower()
    jat = (jat_label or "").lower()

    if "protein" in jat:
        return "proteome"
    if "cds" in jat:
        return "cds"
    if "transcript" in jat:
        return "transcriptome"
    if "assembly" in jat:
        return "assembly"
    if "gff" in jat or "gene" in jat:
        return "gff"

    if isinstance(file_type, list):
        ft = " ".join(str(x).lower() for x in file_type)
        if "protein" in ft:
            return "proteome"
        if "cds" in ft:
            return "cds"
        if "transcript" in ft:
            return "transcriptome"
        if "assembly" in ft:
            return "assembly"
        if "gene" in ft or "gff" in ft:
            return "gff"

    if fmt in {"fasta", "fa"} and (".aa." in name or "protein" in name):
        return "proteome"
    if fmt in {"fasta", "fa"} and ("cds" in name or ".nt." in name):
        return "cds"

    return "other"


def fetch_search_json(
    portal_id: str,
    token: str,
    *,
    page: int = 1,
    page_size: int = 50,
    timeout: int = 120,
) -> dict[str, Any]:
    params = {
        "q": portal_id,
        "f": "mycocosm_portal_id",
        "a": "false",
        "h": "false",
        "d": "asc",
        "p": str(page),
        "x": str(page_size),
        "t": "simple",
        "api_version": "2",
    }
    headers = {"accept": "application/json", "Authorization": token}

    r = requests.get(SEARCH_URL, params=params, headers=headers, timeout=timeout)
    if r.status_code in (401, 403):
        raise RuntimeError(f"Auth failed ({r.status_code}). Check JGI_TOKEN / --token.")
    r.raise_for_status()
    return r.json()


def iter_org_and_files(payload: dict):
    orgs = payload.get("organisms") or []
    for org in orgs:
        dataset_id = org.get("id")
        top_hit = org.get("top_hit") or {}
        top_hit_id = top_hit.get("_id")
        for f in (org.get("files") or []):
            yield dataset_id, top_hit_id, f


def iter_file_entries(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    orgs = payload.get("organisms") or []
    for org in orgs:
        yield from org.get("files") or []


@app.callback(invoke_without_command=True)
def fetch_index_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    portal_id: list[str] | None = typer.Option(None, "--portal-id", help="Limit to specific portal IDs."),
    token: str | None = typer.Option(None, "--token", help="JGI token (else uses env JGI_TOKEN)."),
    page_size: int = typer.Option(50, "--page-size", help="Page size (API max is typically 50)."),
    cache_only: bool = typer.Option(False, "--cache-only", help="Only cache JSON, do not ingest into DB."),
    overwrite_cache: bool = typer.Option(False, "--overwrite-cache", help="Refetch even if cache exists."),
    ingest_from_cache: bool = typer.Option(
        False,
        "--ingest-from-cache",
        help="Do not fetch; ingest portal_files from existing cache JSON (requires cache files).",
    ),
    published_only: bool = typer.Option(False, "--published-only", help="Only operate on is_published=1 portals."),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error/--fail-fast", help="Continue even if some portals fail."
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    tok: str | None = None
    if not ingest_from_cache:
        tok = get_token(token)

    # Determine portal list AND portal_set for FK safety
    conn = connect(paths.db_path)
    try:
        if portal_id:
            portals = portal_id
        else:
            if published_only:
                rows = conn.execute("SELECT portal_id FROM portals WHERE is_published = 1 ORDER BY portal_id").fetchall()
            else:
                rows = conn.execute("SELECT portal_id FROM portals ORDER BY portal_id").fetchall()
            portals = [r["portal_id"] for r in rows]

        portal_set = {r["portal_id"] for r in conn.execute("SELECT portal_id FROM portals").fetchall()}
    finally:
        conn.close()

    if not portals:
        raise typer.BadParameter("No portals found in DB.")

    paths.jgi_index_cache_dir.mkdir(parents=True, exist_ok=True)

    total_files = 0
    total_portals = 0
    skipped_foreign_portal = 0
    errors_log = paths.errors_log
    n_errors = 0

    with Progress(
        TextColumn("Portal:"),
        TextColumn("{task.fields[portal]:<16}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Fetching", total=len(portals), portal="-" * PORTAL_WIDTH)

        for pid in portals:
            progress.update(task, portal=(pid[:PORTAL_WIDTH]).ljust(PORTAL_WIDTH))
            inserted = 0
            cache_path = paths.jgi_index_cache_dir / f"{pid}.json"

            try:
                merged_payload: dict[str, Any] | None = None

                if ingest_from_cache:
                    if not cache_path.exists():
                        raise FileNotFoundError(f"Cache not found for {pid}: {cache_path}")
                    merged_payload = json.loads(cache_path.read_text(encoding="utf-8"))
                else:
                    # skip fetch if cache exists and not overwriting
                    if cache_path.exists() and not overwrite_cache:
                        # You chose "skip completely" semantics here
                        total_portals += 1
                        progress.advance(task)
                        continue

                    # Fetch pages
                    page = 1
                    merged_payload = {"pages": [], "portal_id": pid, "fetched_at": now_iso()}
                    while True:
                        assert tok is not None
                        payload = fetch_search_json(pid, tok, page=page, page_size=page_size)
                        merged_payload["pages"].append(payload)
                        if not payload.get("next_page", False):
                            break
                        page += 1

                    cache_path.write_text(
                        json.dumps(merged_payload, indent=2, ensure_ascii=False) + "\n",
                        encoding="utf-8",
                    )

                    if cache_only:
                        total_portals += 1
                        progress.advance(task)
                        continue

                assert merged_payload is not None

                # Extract dataset_id/top_hit_id
                dataset_id_val = None
                top_hit_id_val = None
                for page_payload in merged_payload["pages"]:
                    for dataset_id, top_hit_id, _f in iter_org_and_files(page_payload):
                        if dataset_id_val is None and dataset_id:
                            dataset_id_val = str(dataset_id)
                        if top_hit_id_val is None and top_hit_id:
                            top_hit_id_val = str(top_hit_id)

                # Update portals
                conn = connect(paths.db_path)
                try:
                    conn.execute(
                        """
                        UPDATE portals
                        SET dataset_id = COALESCE(?, dataset_id),
                            top_hit_id = COALESCE(?, top_hit_id)
                        WHERE portal_id = ?
                        """,
                        (dataset_id_val, top_hit_id_val, pid),
                    )
                    conn.commit()
                finally:
                    conn.close()

                # Ingest portal_files
                conn = connect(paths.db_path)
                try:
                    for page_payload in merged_payload["pages"]:
                        for f in iter_file_entries(page_payload):
                            file_id = f.get("_id")
                            if file_id is None:
                                continue

                            file_name = f.get("file_name") or ""
                            file_size = f.get("file_size")
                            md5sum = f.get("md5sum")
                            file_status = f.get("file_status")
                            file_status_id = f.get("file_status_id")
                            file_path = f.get("file_path")
                            file_group = f.get("file_group")
                            data_group = f.get("data_group")
                            modified_date = f.get("modified_date")
                            file_date = f.get("file_date")
                            added_date = f.get("added_date")

                            meta = f.get("metadata") or {}
                            myco_pid = meta.get("mycocosm_portal_id") or f.get("portal_detail_id") or pid
                            myco_pid = str(myco_pid).strip()

                            # ✅ guard: only insert for portals we track (prevents FK failures)
                            if myco_pid not in portal_set:
                                skipped_foreign_portal += 1
                                continue

                            jat_label = meta.get("jat_label") or ""
                            file_format = meta.get("file_format") or ""

                            kind = classify_kind(file_name, file_format, jat_label, f.get("file_type"))

                            meta_json = {
                                "jat_label": jat_label,
                                "file_format": file_format,
                                "file_status": file_status,
                                "file_status_id": file_status_id,
                                "file_path": file_path,
                                "file_group": file_group,
                                "data_group": data_group,
                                "file_type": f.get("file_type"),
                                "portal_detail_id": f.get("portal_detail_id"),
                                "mycocosm_portal_id": myco_pid,
                                "file_date": file_date,
                                "modified_date": modified_date,
                                "added_date": added_date,
                                "dce": f.get("dce"),
                                "es_public": f.get("_es_public_data"),
                            }

                            conn.execute(
                                """
                                INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
                                VALUES(?,?,?,?,?,?,?,?)
                                ON CONFLICT(file_id) DO UPDATE SET
                                  portal_id=excluded.portal_id,
                                  kind=excluded.kind,
                                  filename=excluded.filename,
                                  size_bytes=excluded.size_bytes,
                                  md5=excluded.md5,
                                  meta_json=excluded.meta_json
                                """,
                                (
                                    str(file_id),
                                    myco_pid,
                                    kind,
                                    file_name,
                                    int(file_size) if isinstance(file_size, int) else (int(file_size) if file_size else None),
                                    md5sum,
                                    now_iso(),
                                    json.dumps(meta_json, ensure_ascii=False),
                                ),
                            )
                            inserted += 1

                    conn.commit()
                finally:
                    conn.close()

                total_files += inserted
                total_portals += 1

            except requests.HTTPError as e:
                n_errors += 1
                resp = getattr(e, "response", None)
                log_error_jsonl(
                    errors_log,
                    {
                        "event": "fetch_index_error",
                        "portal_id": pid,
                        "stage": "http",
                        "status_code": getattr(resp, "status_code", None),
                        "response_text": (resp.text[:500] if resp is not None and getattr(resp, "text", None) else None),
                        **exception_record(e),
                    },
                )
                progress.console.log(f"[red]ERROR[/red] {pid}: HTTP error (logged).")
                if not continue_on_error:
                    raise

            except Exception as e:
                n_errors += 1
                log_error_jsonl(
                    errors_log,
                    {
                        "event": "fetch_index_error",
                        "portal_id": pid,
                        "stage": "unknown",
                        **exception_record(e),
                    },
                )
                progress.console.log(f"[red]ERROR[/red] {pid}: {type(e).__name__} (logged).")
                if not continue_on_error:
                    raise

            finally:
                progress.advance(task)

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "fetch_index",
            "n_portals": total_portals,
            "n_files_upserted": total_files,
            "skipped_foreign_portal": skipped_foreign_portal,
            "cache_dir": str(paths.jgi_index_cache_dir),
            "cache_only": cache_only,
            "overwrite_cache": overwrite_cache,
            "ingest_from_cache": ingest_from_cache,
            "n_errors": n_errors,
        },
    )

    typer.echo(
        f"Done. Portals processed: {total_portals}. "
        f"File rows upserted: {total_files}. "
        f"Skipped foreign-portal rows: {skipped_foreign_portal}. "
        f"Errors: {n_errors}."
    )
