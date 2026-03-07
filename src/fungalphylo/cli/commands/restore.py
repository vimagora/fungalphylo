from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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
from fungalphylo.db.db import connect, init_db
from fungalphylo.core.errors import log_error_jsonl, exception_record

app = typer.Typer(help="Request that approved JGI files be restored from archive to disk (separate from download).")

RESTORE_URL = "https://files.jgi.doe.gov/request_archived_files/"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def get_token(explicit: Optional[str]) -> str:
    if explicit:
        return explicit.strip()
    env = os.getenv("JGI_TOKEN", "").strip()
    if not env:
        raise typer.BadParameter("Missing JGI token. Provide --token or set env var JGI_TOKEN.")
    return env


def compact_json(obj: Any) -> str:
    # compact to better approximate backend character limits
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def payload_stats(payload: dict) -> dict:
    ids = payload.get("ids") or {}
    n_datasets = len(ids)
    n_files = sum(len(v.get("file_ids") or []) for v in ids.values())
    return {"n_datasets": n_datasets, "n_file_ids": n_files}


@dataclass
class DatasetRestoreBlock:
    dataset_id: str
    file_ids: List[str]
    top_hit: str
    mycocosm_portal_id: Optional[str] = None

    def as_payload_entry(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "file_ids": self.file_ids,
            "top_hit": self.top_hit,
        }
        if self.mycocosm_portal_id:
            d["mycocosm_portal_id"] = self.mycocosm_portal_id
        return d


def build_dataset_blocks(rows: List[dict]) -> List[DatasetRestoreBlock]:
    """
    Input rows contain: portal_id, dataset_id, top_hit_id, proteome_file_id, cds_file_id
    We group by dataset_id. Each dataset block can include multiple portals' file_ids,
    but for MycoCosm portals it’s also fine if one dataset_id corresponds to one portal.
    """
    by_dataset: Dict[str, DatasetRestoreBlock] = {}

    for r in rows:
        portal_id = r["portal_id"]
        dataset_id = r["dataset_id"]
        top_hit_id = r["top_hit_id"]

        file_ids = [r["proteome_file_id"]]
        if r["cds_file_id"]:
            file_ids.append(r["cds_file_id"])

        if dataset_id not in by_dataset:
            by_dataset[dataset_id] = DatasetRestoreBlock(
                dataset_id=dataset_id,
                file_ids=[],
                top_hit=top_hit_id,
                mycocosm_portal_id=portal_id,  # safe for MycoCosm, harmless otherwise
            )

        block = by_dataset[dataset_id]

        # If the same dataset_id appears with different top_hit (shouldn’t, but be safe)
        if block.top_hit != top_hit_id:
            # Prefer the existing one but record mismatch by appending a warning file later
            # (we keep it simple here; restore usually still works)
            pass

        # Deduplicate file IDs
        for fid in file_ids:
            if fid and fid not in block.file_ids:
                block.file_ids.append(fid)

    return list(by_dataset.values())


def chunk_restore_payloads(
    blocks: List[DatasetRestoreBlock],
    *,
    send_mail: bool,
    api_version: str = "2",
    max_chars: int = 3500,
) -> List[Dict[str, Any]]:
    """
    Create a list of payload dicts, each compact JSON length <= max_chars.
    Splits at dataset block boundaries (never splits file_ids inside a dataset).
    """
    payloads: List[Dict[str, Any]] = []

    def new_payload() -> Dict[str, Any]:
        return {"ids": {}, "send_mail": send_mail, "api_version": api_version}

    current = new_payload()

    for b in blocks:
        # Try adding this dataset block to current payload
        current["ids"][b.dataset_id] = b.as_payload_entry()
        if len(compact_json(current)) <= max_chars:
            continue

        # Too big: remove it and start a new payload
        current["ids"].pop(b.dataset_id, None)
        if current["ids"]:
            payloads.append(current)

        current = new_payload()
        current["ids"][b.dataset_id] = b.as_payload_entry()

        # If even a single block is too big, we must still send it alone (or tell user to use -d @file)
        if len(compact_json(current)) > max_chars:
            # We still allow it; the user asked 3500 safety limit, but backend is 4094.
            # If it exceeds 4094 too, we should error.
            payload_len = len(compact_json(current))
            if payload_len > 4094:
                raise RuntimeError(
                    f"Single dataset payload is {payload_len} chars (>4094). "
                    f"Reduce file_ids per dataset or implement '-d @file' posting."
                )

    if current["ids"]:
        payloads.append(current)

    return payloads


def post_restore(payload: Dict[str, Any], token: str, timeout: int = 120) -> Dict[str, Any]:
    headers = {
        "accept": "application/json",
        "Authorization": f"{token}",
        "Content-Type": "application/json",
    }
    r = requests.post(RESTORE_URL, headers=headers, data=compact_json(payload).encode("utf-8"), timeout=timeout)
    if r.status_code in (401, 403):
        raise RuntimeError(f"Auth failed ({r.status_code}). Check JGI_TOKEN / --token.")
    r.raise_for_status()
    return r.json()


@app.callback(invoke_without_command=True)
def restore_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    token: Optional[str] = typer.Option(None, "--token", help="JGI token (else uses env JGI_TOKEN)."),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Limit to specific portal IDs."),
    send_mail: bool = typer.Option(True, "--send-mail/--no-send-mail", help="Email when restore is ready."),
    max_chars: int = typer.Option(3500, "--max-chars", help="Max JSON character length per restore request."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build and write payloads but do not POST."),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error/--fail-fast", help="Default: continue posting other payloads if one fails."
        ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    tok: Optional[str] = None if dry_run else get_token(token)

    # Fetch approvals + needed portal metadata
    conn = connect(paths.db_path)
    try:
        params: List[object] = []
        where = ""
        if portal_id:
            where = f"WHERE a.portal_id IN ({','.join('?' for _ in portal_id)})"
            params.extend(portal_id)

        rows = conn.execute(
            f"""
            SELECT
              a.portal_id,
              a.proteome_file_id,
              a.cds_file_id,
              p.dataset_id,
              p.top_hit_id
            FROM approvals a
            JOIN portals p ON p.portal_id = a.portal_id
            {where}
            ORDER BY a.portal_id
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise typer.BadParameter("No approvals found (or none matching --portal-id). Run review apply first.")

    # Validate that dataset_id/top_hit_id exist
    missing_meta = [r["portal_id"] for r in rows if not r["dataset_id"] or not r["top_hit_id"]]
    if missing_meta:
        raise RuntimeError(
            "Missing dataset_id/top_hit_id for portals: "
            + ", ".join(missing_meta)
            + ". Re-run fetch-index after storing these fields."
        )

    norm_rows = [
        {
            "portal_id": r["portal_id"],
            "proteome_file_id": r["proteome_file_id"],
            "cds_file_id": r["cds_file_id"],
            "dataset_id": str(r["dataset_id"]),
            "top_hit_id": str(r["top_hit_id"]),
        }
        for r in rows
    ]

    blocks = build_dataset_blocks(norm_rows)
    payloads = chunk_restore_payloads(blocks, send_mail=send_mail, max_chars=max_chars)

    # Write payloads + responses
    request_id = _now_tag()
    out_dir = project_dir / "restore_requests" / request_id
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, payload in enumerate(payloads, start=1):
        (out_dir / f"payload_{i:03d}.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    typer.echo(f"Wrote {len(payloads)} restore payload(s) to: {out_dir}")
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            INSERT INTO restore_requests(
              request_id, created_at, request_dir, dry_run, status,
              n_payloads, n_posted, n_errors, send_mail, max_chars, continue_on_error
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                request_id,
                _now_iso(),
                str(out_dir.relative_to(project_dir)),
                1 if dry_run else 0,
                "planned" if dry_run else "running",
                len(payloads),
                0,
                0,
                1 if send_mail else 0,
                max_chars,
                1 if continue_on_error else 0,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    responses_path = out_dir / "responses.jsonl"
    n_posted = 0

    n_errors = 0

    if not dry_run:
        with responses_path.open("w", encoding="utf-8") as rf:

            with Progress(
                TextColumn("Payload:"),
                TextColumn("{task.fields[payload]:<10}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Downloading", total=len(payloads), payload="-" * 10)

                errors_log = paths.errors_log
                for i, payload in enumerate(payloads, start=1):
                    progress.update(task, payload=f"{i:03d}/{len(payloads):03d}".ljust(10))
                    
                    try:
                        stats = payload_stats(payload)
                        payload_len = len(compact_json(payload))

                        assert tok is not None
                        resp = post_restore(payload, tok)

                        rf.write(json.dumps({
                            "i": i,
                            "payload_path": str((out_dir / f"payload_{i:03d}.json").relative_to(project_dir)),
                            "payload_len": payload_len,
                            **stats,
                            "response": resp,
                        }, ensure_ascii=False) + "\n")
                        n_posted += 1

                        # Many responses include a request_status_url; surface it
                        status_url = resp.get("request_status_url") or resp.get("request_url")
                        if status_url:
                            progress.console.log(f"[{i}/{len(payloads)}] status: {status_url}")
                        else:
                            progress.console.log(f"[{i}/{len(payloads)}] restore requested (no status url in response?)")

                    except requests.HTTPError as e:
                        n_errors += 1
                        resp = getattr(e, "response", None)
                        log_error_jsonl(errors_log, {
                            "event": "restore_error",
                            "i": i,
                            "payload_path": str(out_dir / f"payload_{i:03d}.json"),
                            "payload_len": len(compact_json(payload)),
                            **payload_stats(payload),
                            "stage": "http",
                            "status_code": getattr(resp, "status_code", None),
                            "response_text": (resp.text[:800] if resp is not None and resp.text else None),
                            **exception_record(e),
                        })
                        progress.console.log(f"[{i}/{len(payloads)}] ERROR: HTTP {getattr(resp,'status_code','?')} (logged)")
                        if not continue_on_error:
                            raise

                    except Exception as e:
                        n_errors += 1
                        log_error_jsonl(errors_log, {
                            "event": "restore_error",
                            "i": i,
                            "payload_path": str(out_dir / f"payload_{i:03d}.json"),
                            "payload_len": len(compact_json(payload)),
                            **payload_stats(payload),
                            "stage": "unknown",
                            **exception_record(e),
                        })
                        progress.console.log(f"[{i}/{len(payloads)}] ERROR: {type(e).__name__} (logged)")
                        if not continue_on_error:
                            raise
                    finally:
                        progress.advance(task)


    log_event(
        project_dir,
        {
            "ts": _now_iso(),
            "event": "restore",
            "n_portals": len(norm_rows),
            "n_datasets": len(blocks),
            "n_payloads": len(payloads),
            "send_mail": send_mail,
            "max_chars": max_chars,
            "dry_run": dry_run,
            "out_dir": str(out_dir),
            "n_posted": n_posted,
            "n_errors": n_errors,
        },
    )

    final_status = "planned" if dry_run else ("completed" if n_errors == 0 else ("partial" if n_posted > 0 else "failed"))
    conn = connect(paths.db_path)
    try:
        conn.execute(
            """
            UPDATE restore_requests
            SET status=?, n_posted=?, n_errors=?
            WHERE request_id=?
            """,
            (final_status, n_posted, n_errors, request_id),
        )
        conn.commit()
    finally:
        conn.close()

    if dry_run:
        typer.echo("Dry-run complete (no restore requests posted).")
    else:
        typer.echo(f"Posted {n_posted} restore request(s). Wait for email or check returned status URLs.")
