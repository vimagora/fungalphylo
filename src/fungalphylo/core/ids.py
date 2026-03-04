from __future__ import annotations

import re
import secrets
from datetime import datetime, timezone
from typing import Any, Mapping

from fungalphylo.core.hash import hash_dict


_ID_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def sanitize_id(s: str) -> str:
    """
    Make an ID filesystem-friendly.
    """
    s = s.strip()
    s = _ID_SAFE.sub("_", s)
    return s[:200]  # keep it reasonable


def new_staging_id(prefix: str = "stg") -> str:
    """
    Create a new immutable staging_id.
    Human-friendly, unique, sortable by time.

    Example: stg_20260304T121530Z_a1b2c3
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rand = secrets.token_hex(3)  # 6 hex chars
    return f"{prefix}_{ts}_{rand}"


def run_id_from_cache_key(kind: str, cache_key: Mapping[str, Any], prefix: str = "run") -> str:
    """
    Deterministic run_id from a cache key dict.
    Good for caching: same inputs+params+versions => same run_id.
    """
    kind = sanitize_id(kind)
    digest = hash_dict(dict(cache_key))[:12]
    return f"{prefix}_{kind}_{digest}"