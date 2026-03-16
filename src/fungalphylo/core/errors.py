from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(UTC).isoformat()


def log_error_jsonl(log_path: Path, record: dict[str, Any]) -> None:
    """
    Append one JSON record to a JSONL error log.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = dict(record)
    record.setdefault("ts", _now())
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def exception_record(exc: BaseException) -> dict[str, Any]:
    return {
        "exc_type": type(exc).__name__,
        "exc_msg": str(exc),
        "traceback": traceback.format_exc(),
    }






