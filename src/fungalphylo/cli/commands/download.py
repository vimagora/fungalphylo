from __future__ import annotations

import json
import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import typer
from rich.progress import (
    Progress,
    BarColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    TextColumn,
    MofNCompleteColumn,
)

from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import connect

# Error logging utilities (same pattern as fetch-index)
from fungalphylo.core.errors import log_error_jsonl, exception_record

app = typer.Typer(
    help="Download approved (restored) JGI files into raw/ cache (processing happens in stage)."
)

DOWNLOAD_URL = "https://files-download.jgi.doe.gov/download_files/"
PORTAL_WIDTH = 18  # fixed-width column


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
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)[:200] or "bundle.bin"


@dataclass
class DatasetBlock:
    dataset_id: str
    file_ids: List[str]
    top_hit: str
    mycocosm_portal_id: Optional[str] = None

    def as_payload_entry(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"file_ids": self.file_ids, "top_hit": self.top_hit}
        if self.mycocosm_portal_id:
            d["mycocosm_portal_id"] = self.mycocosm_portal_id
        return d


def _collect_file_ids(rows: List[dict]) -> List[str]:
    fids: List[str] = []
    for r in rows:
        fids.append(r["proteome_file_id"])
        if r.get("cds_file_id"):
            fids.append(r["cds_file_id"])
    seen = set()
    out: List[str] = []
    for x in fids:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def build_blocks(rows: List[dict]) -> List[DatasetBlock]:
    by_dataset: Dict[str, DatasetBlock] = {}
    for r in rows:
        pid = r["portal_id"]
        ds = r["dataset_id"]
        top = r["top_hit_id"]
        fids = [r["proteome_file_id"]] + ([r["cds_file_id"]] if r.get("cds_file_id") else [])

        if ds not in by_dataset:
            by_dataset[ds] = DatasetBlock(dataset_id=ds, file_ids=[], top_hit=top, mycocosm_portal_id=pid)

        blk = by_dataset[ds]
        for fid in fids:
            if fid and fid not in blk.file_ids:
                blk.file_ids.append(fid)

    return list(by_dataset.values())


def chunk_payloads(blocks: List[DatasetBlock], *, max_chars: int = 3500) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []

    def newp() -> Dict[str, Any]:
        return {"ids": {}}

    cur = newp()
    for b in blocks:
        cur["ids"][b.dataset_id] = b.as_payload_entry()
        if len(compact_json(cur)) <= max_chars:
            continue

        # too big: finalize current (without this block) and start new
        cur["ids"].pop(b.dataset_id, None)
        if cur["ids"]:
            payloads.append(cur)

        cur = newp()
        cur["ids"][b.dataset_id] = b.as_payload_entry()

        L = len(compact_json(cur))
        if L > 4094:
            raise RuntimeError(
                f"Single dataset download payload is {L} chars (>4094). "
                f"Reduce file_ids per dataset or implement file-based POST."
            )

    if cur["ids"]:
        payloads.append(cur)
    return payloads


def post_download(payload: Dict[str, Any], token: str, timeout: int = 300) -> requests.Response:
    headers = {
        "accept": "application/json",
        "Authorization": f"{token}",
        "Content-Type": "application/json",
    }
    r = requests.post(
        DOWNLOAD_URL,
        headers=headers,
        data=compact_json(payload).encode("utf-8"),
        timeout=timeout,
    )
    if r.status_code in (401, 403):
        raise RuntimeError(f"Auth failed ({r.status_code}). Check JGI_TOKEN / --token.")
    r.raise_for_status()
    return r


def save_and_extract_bundle(
    *,
    resp: requests.Response,
    out_dir: Path,
    bundle_name: str,
) -> Tuple[Path, List[Path]]:
    """
    Save response body to out_dir/bundle_name.
    If it's a zip (by header or magic), extract into out_dir/<bundle>_extracted/.
    Returns (bundle_path, extracted_file_paths).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = out_dir / bundle_name
    with bundle_path.open("wb") as f:
        f.write(resp.content)

    # detect zip by header or magic
    is_zip = "zip" in (resp.headers.get("Content-Type", "").lower())
    if not is_zip:
        try:
            with bundle_path.open("rb") as f:
                magic = f.read(4)
            if magic == b"PK\x03\x04":
                is_zip = True
        except Exception:
            pass

    extracted: List[Path] = []
    if is_zip:
        extract_dir = out_dir / (bundle_name + "_extracted")
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(bundle_path, "r") as z:
            z.extractall(extract_dir)
        for p in extract_dir.rglob("*"):
            if p.is_file():
                extracted.append(p)
    else:
        extracted.append(bundle_path)

    return bundle_path, extracted


def copy_extracted_into_raw(
    *,
    extracted_paths: List[Path],
    file_meta: Dict[str, dict],
    paths: ProjectPaths,
    unmatched_dir: Path,
) -> Tuple[int, int]:
    """
    Copy extracted files into raw/<portal>/<file_id>/<filename> by matching on original filename.
    Returns (n_matched, n_unmatched).
    """
    n_matched = 0
    n_unmatched = 0

    # Map basename -> list of fids (in case of duplicates)
    by_basename: Dict[str, List[str]] = {}
    for fid, meta in file_meta.items():
        base = Path(meta["filename"]).name if meta.get("filename") else ""
        if base:
            by_basename.setdefault(base, []).append(fid)

    unmatched_dir.mkdir(parents=True, exist_ok=True)

    for p in extracted_paths:
        base = p.name
        fids = by_basename.get(base)

        if fids:
            # If multiple match, pick first (rare). Could be improved later.
            fid = fids[0]
            meta = file_meta[fid]
            portal_id = meta["portal_id"]

            dest_dir = paths.raw_file_dir(portal_id, fid)
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / base
            shutil.copy2(p, dest_path)
            n_matched += 1
        else:
            shutil.copy2(p, unmatched_dir / base)
            n_unmatched += 1

    return n_matched, n_unmatched


@app.callback(invoke_without_command=True)
def download_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    token: Optional[str] = typer.Option(None, "--token", help="JGI token (else uses env JGI_TOKEN)."),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Limit to specific portal IDs."),
    max_chars: int = typer.Option(3500, "--max-chars", help="Max JSON character length per download request."),
    timeout: int = typer.Option(300, "--timeout", help="HTTP timeout seconds per request."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build payloads but do not download."),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error/--fail-fast", help="Default: continue if one payload fails."
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    tok = get_token(token)
    errors_log = paths.errors_log

    # Load approvals + dataset metadata
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

        if not rows:
            raise typer.BadParameter("No approvals found (or none matching --portal-id). Run review apply first.")

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

        # Grab file metadata for path matching
        fids = _collect_file_ids(norm_rows)
        placeholders = ",".join("?" for _ in fids) if fids else "''"
        file_meta_rows = conn.execute(
            f"""
            SELECT file_id, portal_id, filename, md5, size_bytes
            FROM portal_files
            WHERE file_id IN ({placeholders})
            """,
            fids,
        ).fetchall()
    finally:
        conn.close()

    file_meta: Dict[str, dict] = {}
    for r in file_meta_rows:
        file_meta[str(r["file_id"])] = {
            "portal_id": r["portal_id"],
            "filename": r["filename"],
            "md5": r["md5"],
            "size_bytes": r["size_bytes"],
        }

    blocks = build_blocks(norm_rows)
    payloads = chunk_payloads(blocks, max_chars=max_chars)

    out_dir = project_dir / "download_requests" / _now_tag()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save payloads for reproducibility/debugging
    for i, payload in enumerate(payloads, start=1):
        (out_dir / f"payload_{i:03d}.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    typer.echo(f"Wrote {len(payloads)} download payload(s) to: {out_dir}")
    if dry_run:
        typer.echo("Dry-run complete (no downloads).")
        log_event(
            project_dir,
            {
                "ts": _now_iso(),
                "event": "download",
                "dry_run": True,
                "n_payloads": len(payloads),
                "out_dir": str(out_dir),
            },
        )
        return

    unmatched_dir = out_dir / "unmatched_files"
    bundles_dir = out_dir / "bundles"
    bundles_dir.mkdir(parents=True, exist_ok=True)

    n_errors = 0
    n_payload_ok = 0
    n_files_matched = 0
    n_files_unmatched = 0

    # Progress bar: one tick per payload (not per portal)
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
                resp = post_download(payload, tok, timeout=timeout)

                # Determine a bundle filename
                bundle_name = f"bundle_{i:03d}.bin"
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    bundle_name = cd.split("filename=")[-1].strip().strip('"')
                    bundle_name = _safe_name(bundle_name)

                bundle_path, extracted_paths = save_and_extract_bundle(
                    resp=resp,
                    out_dir=bundles_dir,
                    bundle_name=bundle_name,
                )

                matched, unmatched = copy_extracted_into_raw(
                    extracted_paths=extracted_paths,
                    file_meta=file_meta,
                    paths=paths,
                    unmatched_dir=unmatched_dir,
                )
                n_files_matched += matched
                n_files_unmatched += unmatched
                n_payload_ok += 1

                # optional: minimal logging without breaking progress bar
                # progress.console.log(f"payload {i}: matched={matched} unmatched={unmatched}")

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
                    raise

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
                    raise

            finally:
                progress.advance(task)

    log_event(
        project_dir,
        {
            "ts": _now_iso(),
            "event": "download",
            "dry_run": False,
            "n_payloads": len(payloads),
            "n_payload_ok": n_payload_ok,
            "n_errors": n_errors,
            "out_dir": str(out_dir),
            "raw_dir": str(paths.raw_dir),
            "n_files_matched": n_files_matched,
            "n_files_unmatched": n_files_unmatched,
        },
    )

    typer.echo(f"Done. Payloads OK: {n_payload_ok}/{len(payloads)}. Errors: {n_errors}.")
    typer.echo(f"Files copied into raw/: matched={n_files_matched}, unmatched={n_files_unmatched}.")
    if n_files_unmatched:
        typer.echo(f"Unmatched files kept at: {unmatched_dir}")