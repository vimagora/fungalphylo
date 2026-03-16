from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_manifest(path: Path, data: dict[str, Any]) -> None:
    """
    Write a JSON manifest atomically.
    """
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    tmp.replace(path)


def read_manifest(path: Path) -> dict[str, Any]:
    path = path.expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a JSON object. Got: {type(data)}")
    return data