from __future__ import annotations

import csv
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

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.db.db import connect

app = typer.Typer(help="Download approved (restored) JGI files into raw/ cache (processing happens in stage).")

DOWNLOAD_URL = "https://files-download.jgi.doe.gov/download_files/"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def get_token(explicit: Optional[str]) -> str:
    if explicit:
        tok = explicit.strip()
    else:
        tok = os.getenv("JGI_TOKEN", "").strip()

    if not tok:
        raise typer.BadParameter("Missing JGI token. Provide --token or set env var JGI_TOKEN.")

    if not tok.lower().startswith("bearer "):
        tok = f"Bearer {tok}"
    return tok


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


def build_blocks(rows: List[dict]) -> List[DatasetBlock]:
    by_dataset: Dict[str, DatasetBlock] = {}
    for r in rows:
        pid = r["portal_id"]
        ds = r["dataset_id"]
        top = r["top_hit_id"]

        fids: List[str] = []
        if r.get("proteome_file_id"):
            fids.append(r["proteome_file_id"])
        if r.get("cds_file_id"):
            fids.append(r["cds_file_id"])

        if not fids:
            continue

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
        "accept": "*/*",
        "Authorization": token,
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


def save_and_extract_zip_bundle(resp: requests.Response, bundles_dir: Path, bundle_name: str) -> Tuple[Path, Path]:
    bundles_dir.mkdir(parents=True, exist_ok=True)
    zip_path = bundles_dir / bundle_name
    with zip_path.open("wb") as f:
        f.write(resp.content)

    with zip_path.open("rb") as f:
        magic = f.read(4)
    if magic != b"PK\x03\x04":
        raise RuntimeError(
            f"Download response is not a zip (magic={magic!r}). "
            f"Cannot use manifest-guided move. Keep --retain all and inspect."
        )

    extracted_root = bundles_dir / (bundle_name + "_extracted")
    extracted_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extracted_root)

    return zip_path, extracted_root


def find_manifest_csv(extracted_root: Path) -> Optional[Path]:
    for name in ("File_Manifest.csv", "Download_File_Manifest.csv"):
        p = extracted_root / name
        if p.exists():
            return p

    cands = sorted(extracted_root.rglob("*.csv"))
    for p in cands:
        if "manifest" in p.name.lower():
            return p
    return None


@dataclass
class ManifestRow:
    filename: str
    file_id: str
    dataset_id: str
    rel_dir: str
    portal_id: str


def _find_col(fieldnames: List[str], candidates: List[str]) -> Optional[str]:
    for cand in candidates:
        for fn in fieldnames:
            if fn.strip().lower() == cand.strip().lower():
                return fn
    for cand in candidates:
        cand_l = cand.lower()
        for fn in fieldnames:
            if cand_l in fn.lower():
                return fn
    return None


def parse_manifest(manifest_csv: Path) -> List[ManifestRow]:
    rows: List[ManifestRow] = []
    text = manifest_csv.read_text(encoding="utf-8", errors="replace")

    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=[",", "\t", ";"])
        delim = dialect.delimiter
    except Exception:
        delim = "\t"

    with manifest_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        if reader.fieldnames is None:
            raise ValueError(f"Manifest {manifest_csv} has no header row.")

        fn_col = _find_col(reader.fieldnames, ["filename", "file name"])
        id_col = _find_col(reader.fieldnames, ["file_id", "file id"])
        ds_col = _find_col(reader.fieldnames, ["jgi grouping id", "grouping id", "dataset_id"])
        dir_col = _find_col(reader.fieldnames, ["directory/path", "directory", "path"])
        pid_col = _find_col(reader.fieldnames, ["short organism name", "portal_id", "mycocosm_portal_id"])

        if not (fn_col and id_col and ds_col and dir_col and pid_col):
            raise ValueError(f"Manifest {manifest_csv} missing required columns. Found: {reader.fieldnames}")

        for r in reader:
            filename = (r.get(fn_col) or "").strip()
            file_id = (r.get(id_col) or "").strip()
            dataset_id = (r.get(ds_col) or "").strip()
            rel_dir = (r.get(dir_col) or "").strip().replace("\\", "/").strip("/").strip()
            portal_id = (r.get(pid_col) or "").strip()
            if not (filename and file_id and rel_dir and portal_id):
                continue
            rows.append(
                ManifestRow(
                    filename=filename,
                    file_id=file_id,
                    dataset_id=dataset_id,
                    rel_dir=rel_dir,
                    portal_id=portal_id,
                )
            )

    return rows


def move_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))


def move_files_using_manifest(
    *,
    content_root: Path,
    manifest_csv: Path,
    paths: ProjectPaths,
    keep_manifest_to: Path,
) -> Tuple[int, int, Path]:
    entries = parse_manifest(manifest_csv)

    unmatched_path = keep_manifest_to.parent / "unmatched_manifest.tsv"
    moved = 0
    missing = 0

    with unmatched_path.open("w", encoding="utf-8", newline="") as uf:
        w = csv.writer(uf, delimiter="\t")
        w.writerow(["portal_id", "file_id", "filename", "expected_source_path", "reason"])

        for e in entries:
            src = content_root / e.rel_dir / e.filename
            if not src.exists():
                hits = list(content_root.rglob(e.filename))
                if len(hits) == 1:
                    src = hits[0]
                else:
                    missing += 1
                    w.writerow([e.portal_id, e.file_id, e.filename, str(src), "missing_or_ambiguous"])
                    continue

            dest_dir = paths.raw_file_dir(e.portal_id, e.file_id)
            dest = dest_dir / e.filename
            try:
                move_file(src, dest)
                moved += 1
            except Exception as ex:
                missing += 1
                w.writerow([e.portal_id, e.file_id, e.filename, str(src), f"move_failed:{type(ex).__name__}"])

    keep_manifest_to.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(manifest_csv, keep_manifest_to)

    return moved, missing, unmatched_path


@app.callback(invoke_without_command=True)
def download_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    token: Optional[str] = typer.Option(None, "--token", help="JGI token (else uses env JGI_TOKEN)."),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Limit to specific portal IDs."),
    max_chars: int = typer.Option(3500, "--max-chars", help="Max JSON character length per download request."),
    timeout: int = typer.Option(300, "--timeout", help="HTTP timeout seconds per request."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build payloads but do not download."),
    overwrite_staged: bool = typer.Option(False, "--overwrite-staged", help="If already staged, download again."),
    skip_if_raw_present: bool = typer.Option(
        False,
        "--skip-if-raw-present",
        help="Skip downloading if the raw file already exists at raw/<portal>/<file_id>/<filename>.",
    ),
    continue_on_error: bool = typer.Option(True, "--continue-on-error/--fail-fast", help="Continue if one payload fails."),
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

    cfg = resolve_config(project_config=load_yaml(paths.config_yaml))
    raw_layout = cfg["staging"]["raw_layout"]

    tok = get_token(token)
    errors_log = paths.errors_log

    # staged map
    conn = connect(paths.db_path)
    try:
        staged = conn.execute("SELECT portal_id, kind, file_id FROM staged_files").fetchall()
    finally:
        conn.close()
    staged_map = {(r["portal_id"], r["kind"]): r["file_id"] for r in staged}

    # approvals + filenames
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
              pf1.filename AS proteome_filename,
              a.cds_file_id,
              pf2.filename AS cds_filename,
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

            cds_id = r["cds_file_id"]
            cds_fn = r["cds_filename"]

            # skip if staged with same file_id
            if not overwrite_staged:
                if staged_map.get((pid, "proteome")) == prot_id:
                    prot_id = None
                    prot_fn = None
                if cds_id and staged_map.get((pid, "cds")) == cds_id:
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
                    if raw_prot.exists():
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
                    if raw_cds.exists():
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
        typer.echo("Nothing to download (already staged and/or raw files already present).")
        return

    blocks = build_blocks(norm_rows)
    payloads = chunk_payloads(blocks, max_chars=max_chars)

    out_dir = project_dir / "download_requests" / _now_tag()
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, payload in enumerate(payloads, start=1):
        (out_dir / f"payload_{i:03d}.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
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
                "skip_if_raw_present": skip_if_raw_present,
                "overwrite_staged": overwrite_staged,
            },
        )
        return

    bundles_dir = out_dir / "bundles"
    bundles_dir.mkdir(parents=True, exist_ok=True)

    n_errors = 0
    n_payload_ok = 0
    moved_total = 0
    missing_total = 0

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

                zip_name = f"bundle_{i:03d}.zip"
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    name = cd.split("filename=")[-1].strip().strip('"')
                    zip_name = _safe_name(name)
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

    summary = {
        "ts": _now_iso(),
        "n_payloads": len(payloads),
        "n_payload_ok": n_payload_ok,
        "n_errors": n_errors,
        "moved_files": moved_total,
        "missing_files": missing_total,
        "retain": retain,
        "raw_dir": str(paths.raw_dir),
        "skip_if_raw_present": skip_if_raw_present,
        "overwrite_staged": overwrite_staged,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

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
            "retain": retain,
            "moved_files": moved_total,
            "missing_files": missing_total,
            "skip_if_raw_present": skip_if_raw_present,
            "overwrite_staged": overwrite_staged,
        },
    )

    typer.echo(f"Done. Payloads OK: {n_payload_ok}/{len(payloads)}. Errors: {n_errors}.")
    typer.echo(f"Moved into raw/: {moved_total}. Missing/unmoved entries: {missing_total}.")
    typer.echo(f"Kept in download_requests: payloads + manifest(s) + summary (retain={retain}).")