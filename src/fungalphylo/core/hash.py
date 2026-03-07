from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple


def md5_bytes(data: bytes) -> str:
    h = hashlib.md5()
    h.update(data)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def md5_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """
    Stream a file and return its md5 hex digest.
    """
    path = path.expanduser().resolve()
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def file_matches_md5(path: Path, expected_md5: str | None) -> bool:
    path = path.expanduser().resolve()
    if not path.exists():
        return False
    if not expected_md5:
        return True
    return md5_file(path).lower() == expected_md5.strip().lower()


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """
    Stream a file and return its sha256 hex digest.
    """
    path = path.expanduser().resolve()
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_json(obj: Any) -> str:
    """
    Stable sha256 of a JSON-serializable object.

    Important: sort_keys=True ensures stable output regardless of dict ordering.
    """
    data = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return sha256_bytes(data)


def hash_dict(d: Mapping[str, Any]) -> str:
    return hash_json(d)


def write_checksums_tsv(rows: Sequence[Tuple[str, str]], out_path: Path) -> None:
    """
    Write checksums to a TSV file.

    rows: sequence of (relative_path, sha256)
    """
    out_path = out_path.expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("path\tsha256\n")
        for rel, digest in rows:
            f.write(f"{rel}\t{digest}\n")
