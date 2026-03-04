from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs


def log_event(project_dir: Path, event: Mapping[str, Any]) -> None:
    """
    Append a single JSON object to <project>/logs/events.jsonl.

    This is intentionally simple:
      - one line per event
      - command code can call it without worrying about logging frameworks
    """
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    paths.logs_dir.mkdir(parents=True, exist_ok=True)

    # Ensure event is JSON-serializable; fail loudly if not.
    line = json.dumps(event, ensure_ascii=False, sort_keys=True)

    with paths.events_log.open("a", encoding="utf-8") as f:
        f.write(line + "\n")