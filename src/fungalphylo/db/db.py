from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: Path) -> sqlite3.Connection:
    db_path = db_path.expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Ensure FK enforcement (SQLite requires this per-connection)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _column_notnull(conn: sqlite3.Connection, table: str, column: str) -> bool:
    for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
        if str(row[1]) == column:
            return bool(row[3])
    return False


def _migrate_runs_staging_id_nullable(conn: sqlite3.Connection) -> None:
    """Drop NOT NULL on runs.staging_id for pre-family_phylo databases.

    Family-level compute runs (family_phylo, tree_export, clade_mark, etc.)
    have no staging_id. Older schemas declared the column NOT NULL, which
    makes ``register_run`` fail with IntegrityError on those databases.
    SQLite can't ALTER a column's NOT NULL in place, so rebuild + rename.
    """
    if not _column_notnull(conn, "runs", "staging_id"):
        return
    conn.executescript(
        """
        BEGIN;
        CREATE TABLE runs_new (
          run_id          TEXT PRIMARY KEY,
          staging_id      TEXT,
          kind            TEXT NOT NULL,
          created_at      TEXT NOT NULL,
          manifest_path   TEXT NOT NULL,
          manifest_sha256 TEXT NOT NULL,
          FOREIGN KEY (staging_id) REFERENCES stagings(staging_id)
        );
        INSERT INTO runs_new (run_id, staging_id, kind, created_at, manifest_path, manifest_sha256)
          SELECT run_id, staging_id, kind, created_at, manifest_path, manifest_sha256 FROM runs;
        DROP TABLE runs;
        ALTER TABLE runs_new RENAME TO runs;
        COMMIT;
        """
    )


def _apply_lightweight_migrations(conn: sqlite3.Connection) -> None:
    portal_columns = _column_names(conn, "portals")
    if "ncbi_taxon_id" not in portal_columns:
        conn.execute("ALTER TABLE portals ADD COLUMN ncbi_taxon_id INTEGER")
    _migrate_runs_staging_id_nullable(conn)


def init_db(db_path: Path) -> None:
    """
    Create (or migrate) the SQLite database using the bundled schema.sql.
    Safe to call repeatedly.
    """
    db_path = db_path.expanduser().resolve()
    schema_path = Path(__file__).with_name("schema.sql")
    if not schema_path.exists():
        raise FileNotFoundError(f"schema.sql not found at: {schema_path}")

    sql = schema_path.read_text(encoding="utf-8")

    conn = connect(db_path)
    try:
        conn.executescript(sql)
        _apply_lightweight_migrations(conn)
        conn.commit()
    finally:
        conn.close()
