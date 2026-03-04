from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from rich.text import Text 
from rich.progress import (
    Progress,
    BarColumn,
    TimeRemainingColumn,
    TextColumn,
    MofNCompleteColumn,
)

import requests
import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect
from fungalphylo.core.errors import log_error_jsonl, exception_record

app = typer.Typer(help="Fetch and cache JGI Files search index; ingest results into portal_files.")


SEARCH_URL = "https://files.jgi.doe.gov/search/"
PORTAL_WIDTH = 16


def fixed(s: str, width: int = 28) -> str:
    # pad right, truncate if too long
    return (s[:width]).ljust(width)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_token(explicit: Optional[str]) -> str:
    """
    Token handling:
      - if --token provided, use it
      - else read env JGI_TOKEN
    """
    if explicit:
        return explicit.strip()
    env = os.getenv("JGI_TOKEN", "").strip()
    if not env:
        raise typer.BadParameter("Missing JGI token. Provide --token or set env var JGI_TOKEN.")
    return env


def classify_kind(file_name: str, file_format: str, jat_label: str, file_type: Any) -> str:
    """
    Minimal, robust kind classification.
    We'll refine in autoselect, but we need a good baseline.

    Based on your example:
      - proteins_filtered / proteins_all -> proteome
      - cds_filtered / cds_all -> cds
    """
    name = (file_name or "").lower()
    fmt = (file_format or "").lower()
    jat = (jat_label or "").lower()

    # Prefer jat_label if present
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

    # Fall back on file_type if list
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

    # Fall back on filename + format
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
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    Call:
      GET https://files.jgi.doe.gov/search/?q=<portal>&f=mycocosm_portal_id&a=false&h=false&d=asc&p=1&x=50&t=simple&api_version=2
    with Authorization: Bearer <token>
    """
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
    headers = {
        "accept": "application/json",
        "Authorization": f"{token}",
    }

    r = requests.get(SEARCH_URL, params=params, headers=headers, timeout=timeout)
    if r.status_code == 401 or r.status_code == 403:
        raise RuntimeError(f"Auth failed ({r.status_code}). Check JGI_TOKEN / --token.")
    r.raise_for_status()
    return r.json()


def iter_org_and_files(payload: dict):
    orgs = payload.get("organisms") or []
    for org in orgs:
        dataset_id = org.get("id")  # organism.id
        top_hit = org.get("top_hit") or {}
        top_hit_id = top_hit.get("_id")  # organism.top_hit._id
        for f in (org.get("files") or []):
            yield dataset_id, top_hit_id, f


def iter_file_entries(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Your payload format has:
      organisms: [ { files: [ ... file objects ... ] } ]
    We aggregate all organisms[].files[].

    Example entries contain file_id, file_name, file_size, md5sum, file_status,
    file_path, modified_date, file_date, metadata.* etc. :contentReference[oaicite:2]{index=2} :contentReference[oaicite:3]{index=3}
    """
    orgs = payload.get("organisms") or []
    for org in orgs:
        for f in (org.get("files") or []):
            yield f


@app.callback(invoke_without_command=True)
def fetch_index_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Limit to specific portal IDs."),
    token: Optional[str] = typer.Option(None, "--token", help="JGI token (else uses env JGI_TOKEN)."),
    page_size: int = typer.Option(50, "--page-size", help="Page size (API max is typically 50)."),
    cache_only: bool = typer.Option(False, "--cache-only", help="Only cache JSON, do not ingest into DB."),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error/--fail-fast", help="Continue fetching even if some portals fail."),
) -> None:
    """
    Fetch JGI Files search results for each portal and ingest into portal_files.

    Defaults:
      - uses env var JGI_TOKEN (recommended)
      - caches JSON under cache/jgi_index_json/<portal_id>.json
    """
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    tok = get_token(token)

    # Determine portal list
    conn = connect(paths.db_path)
    try:
        if portal_id:
            portals = portal_id
        else:
            rows = conn.execute("SELECT portal_id FROM portals ORDER BY portal_id").fetchall()
            portals = [r["portal_id"] for r in rows]
    finally:
        conn.close()

    if not portals:
        raise typer.BadParameter("No portals found in DB. Run ingest mycocosm.xlsx first.")

    paths.jgi_index_cache_dir.mkdir(parents=True, exist_ok=True)

    total_files = 0
    total_portals = 0

    with Progress(
        TextColumn("Portal:"),
        TextColumn("{task.fields[portal]:<16}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Fetching", total=len(portals), portal="-" * PORTAL_WIDTH)

        errors_log = paths.errors_log
        n_errors = 0
        
        for pid in portals:
            progress.update(task, portal=(pid[:PORTAL_WIDTH]).ljust(PORTAL_WIDTH))
            try:
                # Fetch pages until next_page=false
                page = 1
                merged_payload: Dict[str, Any] = {}
                merged_payload["pages"] = []
                merged_payload["portal_id"] = pid
                merged_payload["fetched_at"] = _now()

                while True:
                    payload = fetch_search_json(pid, tok, page=page, page_size=page_size)
                    merged_payload["pages"].append(payload)
                    if not payload.get("next_page", False):
                        break
                    page += 1

                # Cache raw JSON
                cache_path = paths.jgi_index_cache_dir / f"{pid}.json"
                cache_path.write_text(json.dumps(merged_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

                if cache_only:
                    typer.echo(f"{pid}: cached {cache_path}")
                    total_portals += 1
                    continue

                # Collect best-known dataset_id/top_hit_id (first non-empty seen)
                dataset_id_val = None
                top_hit_id_val = None

                for page_payload in merged_payload["pages"]:
                    for dataset_id, top_hit_id, f in iter_org_and_files(page_payload):
                        if dataset_id_val is None and dataset_id:
                            dataset_id_val = str(dataset_id)
                        if top_hit_id_val is None and top_hit_id:
                            top_hit_id_val = str(top_hit_id)
                
                # Upsert onto portals table
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

                # Ingest into DB (upsert)
                conn = connect(paths.db_path)
                inserted = 0
                try:
                    # Flatten all file entries across pages
                    for page_payload in merged_payload["pages"]:
                        for f in iter_file_entries(page_payload):
                            file_id = f.get("_id")
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
                            jat_label = meta.get("jat_label") or ""
                            file_format = meta.get("file_format") or ""

                            # Kind classification
                            kind = classify_kind(file_name, file_format, jat_label, f.get("file_type"))

                            # meta_json: keep important things autoselect will want
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

                            # DB schema uses TEXT primary key for file_id; normalize to string
                            if file_id is None:
                                continue
                            file_id_str = str(file_id)

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
                                    file_id_str,
                                    str(myco_pid),
                                    kind,
                                    file_name,
                                    int(file_size) if isinstance(file_size, int) else (int(file_size) if file_size else None),
                                    md5sum,
                                    _now(),
                                    json.dumps(meta_json, ensure_ascii=False),
                                ),
                            )
                            inserted += 1

                    conn.commit()
                finally:
                    conn.close()
            
            except requests.HTTPError as e:
                n_errors += 1
                # Try to capture response context
                resp = getattr(e, "response", None)
                log_error_jsonl(
                    errors_log,
                    {
                        "event": "fetch_index_error",
                        "portal_id": pid,
                        "stage": "http",
                        "status_code": getattr(resp, "status_code", None),
                        "response_text": (resp.text[:500] if resp is not None and resp.text else None),
                        **exception_record(e),
                    },
                )
                progress.console.log(f"[red]ERROR[/red] {pid}: HTTP error ({getattr(resp,'status_code', 'n/a')}). Logged.")
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
                progress.console.log(f"[red]ERROR[/red] {pid}: {type(e).__name__}. Logged.")
                if not continue_on_error:
                    raise
            finally:
                progress.advance(task)
            #typer.echo(f"{pid}: cached {cache_path.name}, upserted {inserted} file rows")
            #progress.advance(task)
            total_files += inserted
            total_portals += 1

    log_event(
        project_dir,
        {
            "ts": _now(),
            "event": "fetch_index",
            "n_portals": total_portals,
            "n_files_upserted": total_files,
            "cache_dir": str(paths.jgi_index_cache_dir),
            "cache_only": cache_only,
        },
    )

    typer.echo(f"Done. Portals processed: {total_portals}. File rows upserted: {total_files}.")