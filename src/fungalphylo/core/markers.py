from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class MarkerStatus:
    started_exists: bool
    done_exists: bool
    started_path: Path
    done_path: Path


def marker_status(run_dir: Path) -> MarkerStatus:
    run_dir = run_dir.expanduser().resolve()
    started = run_dir / "STARTED"
    done = run_dir / "DONE"
    return MarkerStatus(
        started_exists=started.exists(),
        done_exists=done.exists(),
        started_path=started,
        done_path=done,
    )


def should_run(run_dir: Path, *, force: bool = False, resume: bool = False) -> bool:
    """
    Decide whether to execute a step based on marker files.

    Rules:
      - If DONE exists: skip unless force
      - If STARTED exists but not DONE: require resume or force
      - If neither exists: run
    """
    st = marker_status(run_dir)

    if st.done_exists and not force:
        return False

    if st.started_exists and not st.done_exists and not (resume or force):
        raise RuntimeError(
            f"Found STARTED without DONE in {run_dir}. "
            f"Use --resume to continue or --force to re-run."
        )

    return True


def write_started(run_dir: Path, info: Optional[str] = None) -> None:
    run_dir = run_dir.expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "STARTED"
    ts = datetime.now(timezone.utc).isoformat()
    content = ts if info is None else f"{ts}\n{info}\n"
    path.write_text(content, encoding="utf-8")


def write_done(run_dir: Path, info: Optional[str] = None) -> None:
    run_dir = run_dir.expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "DONE"
    ts = datetime.now(timezone.utc).isoformat()
    content = ts if info is None else f"{ts}\n{info}\n"
    path.write_text(content, encoding="utf-8")