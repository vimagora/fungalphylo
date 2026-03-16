from __future__ import annotations

import csv
import json
import re
import shutil
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from fungalphylo.core.paths import ProjectPaths

DOWNLOAD_URL = "https://files-download.jgi.doe.gov/download_files/"
TRANSIENT_HTTP_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def compact_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def safe_download_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)[:200] or "bundle.bin"


@dataclass
class DatasetBlock:
    dataset_id: str
    file_ids: list[str]
    top_hit: str
    mycocosm_portal_id: str | None = None

    def as_payload_entry(self) -> dict[str, Any]:
        d: dict[str, Any] = {"file_ids": self.file_ids, "top_hit": self.top_hit}
        if self.mycocosm_portal_id:
            d["mycocosm_portal_id"] = self.mycocosm_portal_id
        return d


def build_blocks(rows: list[dict]) -> list[DatasetBlock]:
    by_dataset: dict[str, DatasetBlock] = {}
    for r in rows:
        pid = r["portal_id"]
        ds = r["dataset_id"]
        top = r["top_hit_id"]

        fids: list[str] = []
        if r.get("proteome_file_id"):
            fids.append(r["proteome_file_id"])
        if r.get("cds_file_id"):
            fids.append(r["cds_file_id"])

        if not fids:
            continue

        if ds not in by_dataset:
            by_dataset[ds] = DatasetBlock(
                dataset_id=ds, file_ids=[], top_hit=top, mycocosm_portal_id=pid
            )

        blk = by_dataset[ds]
        for fid in fids:
            if fid and fid not in blk.file_ids:
                blk.file_ids.append(fid)

    return list(by_dataset.values())


def chunk_payloads(blocks: list[DatasetBlock], *, max_chars: int = 3500) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []

    def newp() -> dict[str, Any]:
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

        length = len(compact_json(cur))
        if length > 4094:
            raise RuntimeError(
                f"Single dataset download payload is {length} chars (>4094). "
                f"Reduce file_ids per dataset or implement file-based POST."
            )

    if cur["ids"]:
        payloads.append(cur)
    return payloads


def post_download(payload: dict[str, Any], token: str, timeout: int = 300) -> requests.Response:
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


def is_transient_download_error(exc: BaseException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        return getattr(resp, "status_code", None) in TRANSIENT_HTTP_STATUS_CODES
    return False


def post_download_with_retries(
    payload: dict[str, Any],
    *,
    token: str,
    timeout: int,
    retries: int,
    retry_backoff_seconds: float,
    log_retry,
) -> requests.Response:
    max_attempts = max(1, retries + 1)
    attempt = 0
    while True:
        attempt += 1
        try:
            return post_download(payload, token, timeout=timeout)
        except Exception as exc:
            if attempt >= max_attempts or not is_transient_download_error(exc):
                raise
            delay = retry_backoff_seconds * (2 ** (attempt - 1))
            log_retry(attempt, max_attempts, delay, exc)
            time.sleep(delay)


def save_and_extract_zip_bundle(
    resp: requests.Response, bundles_dir: Path, bundle_name: str
) -> tuple[Path, Path]:
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


def find_manifest_csv(extracted_root: Path) -> Path | None:
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


def _find_col(fieldnames: list[str], candidates: list[str]) -> str | None:
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


def parse_manifest(manifest_csv: Path) -> list[ManifestRow]:
    rows: list[ManifestRow] = []
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
        pid_col = _find_col(
            reader.fieldnames, ["short organism name", "portal_id", "mycocosm_portal_id"]
        )

        if not (fn_col and id_col and ds_col and dir_col and pid_col):
            raise ValueError(
                f"Manifest {manifest_csv} missing required columns. Found: {reader.fieldnames}"
            )

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
) -> tuple[int, int, Path]:
    entries = parse_manifest(manifest_csv)

    unmatched_path = keep_manifest_to.parent / "unmatched_manifest.tsv"
    moved = 0
    missing = 0

    unmatched_path.parent.mkdir(parents=True, exist_ok=True)
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
                    w.writerow(
                        [e.portal_id, e.file_id, e.filename, str(src), "missing_or_ambiguous"]
                    )
                    continue

            dest_dir = paths.raw_file_dir(e.portal_id, e.file_id)
            dest = dest_dir / e.filename
            try:
                move_file(src, dest)
                moved += 1
            except Exception as ex:
                missing += 1
                w.writerow(
                    [
                        e.portal_id,
                        e.file_id,
                        e.filename,
                        str(src),
                        f"move_failed:{type(ex).__name__}",
                    ]
                )
    shutil.copy2(manifest_csv, keep_manifest_to)

    return moved, missing, unmatched_path
