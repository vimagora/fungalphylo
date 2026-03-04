from __future__ import annotations

import json
import re
import typer
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.core.tabular import read_table
from fungalphylo.db.db import connect

PORTAL_FROM_URL_RE = re.compile(r"https?://mycocosm\.jgi\.doe\.gov/([^/?#]+)")


app = typer.Typer(help="Ingest portal and file metadata tables into the project database.")


def portal_id_from_mycocosm_url(url: str) -> Optional[str]:
    m = PORTAL_FROM_URL_RE.search(url.strip())
    return m.group(1) if m else None


def _pick_col(fieldnames: list[str], *candidates: str) -> Optional[str]:
    lower = {c.lower(): c for c in fieldnames}
    for cand in candidates:
        for k in fieldnames:
            if k.lower() == cand.lower():
                return k
    return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.callback(invoke_without_command=True)
def ingest_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    table_path: Path = typer.Option(..., "--table", help="Path to a TSV/CSV(.gz) table to ingest"),
    delimiter: Optional[str] = typer.Option(None, "--delimiter", help="Override delimiter (default: auto)"),
) -> None:
    """
    Ingest a table into the DB. Auto-detects whether it contains:
      - portal rows only, or
      - portal+file candidate rows.

    Minimum required column:
      - portal_id (or synonyms: portal, portalID)

    If also present:
      - file_id (or synonyms: fileId, file_id)
      - filename (or synonyms: file_name, name)
    then portal_files are ingested too.
    """
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    meta, rows = read_table(table_path, delimiter=delimiter)

    # Resolve column names (support common variants)
    portal_col = _pick_col(meta.fieldnames, "portal_id", "portal", "portalid", "portalID")
    name_col = _pick_col(meta.fieldnames, "Name", "name")
    name_link_col = _pick_col(meta.fieldnames, "Name__link", "name__link")
    published_text_col = _pick_col(meta.fieldnames, "Published", "published")
    published_link_col = _pick_col(meta.fieldnames, "Published__link", "published__link")

    if portal_col is None:
        # XLSX portal list case: infer portal_id from Name hyperlink
        if name_col is None or name_link_col is None:
            raise typer.BadParameter(
                f"Table must include portal_id OR (Name with hyperlink). Found: {meta.fieldnames}"
            )

    file_id_col = _pick_col(meta.fieldnames, "file_id", "fileid", "fileId")
    filename_col = _pick_col(meta.fieldnames, "filename", "file_name", "name", "fileName")
    kind_col = _pick_col(meta.fieldnames, "kind", "type", "file_type", "category")

    has_files = file_id_col is not None and filename_col is not None

    conn = connect(paths.db_path)
    n_portals = 0
    n_files = 0

    try:
        for row in rows:
            portal_id = (row.get(portal_col) or "").strip() if portal_col else ""
            if not portal_id:
                # Infer from Name hyperlink
                url = (row.get(name_link_col) or "").strip() if name_link_col else ""
                portal_id = portal_id_from_mycocosm_url(url) or ""
            if not portal_id:
                continue
            
            published_text = (row.get(published_text_col) or "").strip()
            published_url = (row.get(published_link_col) or "").strip()
            is_published = 1 if (published_text or published_url) else 0

            # Upsert portal (name if available)
            name = row.get("name") or row.get("species") or row.get("organism") or None

            # meta_json: everything except the columns we explicitly store
            portal_meta = dict(row)
            # keep portal_id in meta too? not needed, but harmless
            portal_meta.pop(portal_col, None)
            portal_meta["published_ref_text"] = published_text
            portal_meta["published_url"] = published_url
            portal_meta["portal_url"] = (row.get(name_link_col) or "").strip()

            conn.execute(
                """
                INSERT INTO portals(portal_id, name, created_at, published_text, published_url, is_published, meta_json)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(portal_id) DO UPDATE SET
                name=COALESCE(excluded.name, portals.name),
                published_text=excluded.published_text,
                published_url=excluded.published_url,
                is_published=excluded.is_published,
                meta_json=excluded.meta_json
                """,
                (
                    portal_id,
                    name,
                    _now(),
                    published_text or None,
                    published_url or None,
                    is_published,
                    json.dumps(portal_meta, ensure_ascii=False),
                ),
            )
            n_portals += 1

            if has_files:
                file_id = (row.get(file_id_col) or "").strip()
                filename = (row.get(filename_col) or "").strip()
                if not file_id or not filename:
                    continue

                kind = (row.get(kind_col) or "").strip().lower() if kind_col else "other"
                # crude normalization for common kinds
                if "prot" in kind:
                    kind = "proteome"
                elif "cds" in kind or "coding" in kind or "cdna" in kind:
                    kind = "cds"

                size_bytes = None
                for size_key in ("size_bytes", "size", "bytes"):
                    if size_key in row and row[size_key]:
                        try:
                            size_bytes = int(row[size_key])
                        except Exception:
                            pass
                        break

                md5 = row.get("md5") or row.get("checksum") or None

                file_meta = dict(row)
                # remove the columns stored separately
                for c in [portal_col, file_id_col, filename_col]:
                    if c:
                        file_meta.pop(c, None)

                conn.execute(
                    """
                    INSERT INTO portal_files(file_id, portal_id, kind, filename, size_bytes, md5, created_at, meta_json)
                    VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(file_id) DO UPDATE SET
                      portal_id=excluded.portal_id,
                      kind=excluded.kind,
                      filename=excluded.filename,
                      size_bytes=excluded.size_bytes,
                      md5=excluded.md5,
                      meta_json=excluded.meta_json
                    """,
                    (
                        file_id,
                        portal_id,
                        kind,
                        filename,
                        size_bytes,
                        md5,
                        _now(),
                        json.dumps(file_meta, ensure_ascii=False),
                    ),
                )
                n_files += 1

        conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": _now(),
            "event": "ingest",
            "table": str(table_path),
            "delimiter": meta.delimiter,
            "n_portal_rows_processed": n_portals,
            "n_file_rows_processed": n_files,
            "has_files": has_files,
        },
    )

    typer.echo(f"Ingested table: {table_path}")
    typer.echo(f"Portals upserted: {n_portals}")
    typer.echo(f"Files upserted:   {n_files} (detected file table: {has_files})")