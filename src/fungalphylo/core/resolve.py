from __future__ import annotations

from pathlib import Path


def resolve_raw_path(
    project_dir: Path,
    *,
    raw_layout: str,
    portal_id: str,
    file_id: str,
    filename: str,
) -> Path:
    """
    Resolve the expected raw file path for a downloaded file.

    raw_layout is a format string like:
      raw/{portal_id}/{file_id}/{filename}

    Returns an absolute path under project_dir.
    """
    project_dir = project_dir.expanduser().resolve()
    rel = raw_layout.format(portal_id=portal_id, file_id=file_id, filename=filename)
    # Prevent escaping outside project directory:
    rel_path = Path(rel)
    if rel_path.is_absolute() or ".." in rel_path.parts:
        raise ValueError(f"Unsafe raw_layout resolved path: {rel!r}")
    return project_dir / rel_path
