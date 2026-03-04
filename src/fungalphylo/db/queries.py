from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence

import sqlite3


def fetch_approved_portals(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT portal_id FROM approvals ORDER BY portal_id").fetchall()
    return [r["portal_id"] for r in rows]


def fetch_approvals_with_files(conn: sqlite3.Connection, portal_ids: Optional[Sequence[str]] = None) -> list[dict]:
    """
    Return per-portal approvals joined with portal_files to get filenames and kinds.

    Output dict keys:
      portal_id, proteome_file_id, proteome_filename,
      cds_file_id, cds_filename
    """
    params: list[object] = []
    where = ""
    if portal_ids:
        placeholders = ",".join("?" for _ in portal_ids)
        where = f"WHERE a.portal_id IN ({placeholders})"
        params.extend(list(portal_ids))

    sql = f"""
    SELECT
      a.portal_id,
      a.proteome_file_id,
      pf1.filename AS proteome_filename,
      a.cds_file_id,
      pf2.filename AS cds_filename
    FROM approvals a
    JOIN portal_files pf1 ON pf1.file_id = a.proteome_file_id
    LEFT JOIN portal_files pf2 ON pf2.file_id = a.cds_file_id
    {where}
    ORDER BY a.portal_id
    """
    rows = conn.execute(sql, params).fetchall()
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "portal_id": r["portal_id"],
                "proteome_file_id": r["proteome_file_id"],
                "proteome_filename": r["proteome_filename"],
                "cds_file_id": r["cds_file_id"],
                "cds_filename": r["cds_filename"],
            }
        )
    return out