-- fungalphylo SQLite schema (minimal + durable)

PRAGMA foreign_keys = ON;

-- Key-value metadata (project-level)
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Portals (MycoCosm/JGI portal IDs are immutable; version is encoded in the portal_id string)
CREATE TABLE IF NOT EXISTS portals (
  portal_id      TEXT PRIMARY KEY,
  name           TEXT,
  created_at     TEXT NOT NULL,
  published_text TEXT,
  published_url  TEXT,
  is_published   INTEGER NOT NULL DEFAULT 0,
  dataset_id   TEXT,
  top_hit_id   TEXT,
  meta_json      TEXT
);

-- Files available for a portal (candidates discovered by ingest/fetch-index)
-- 'kind' examples: proteome, cds, gff, transcriptome, other
CREATE TABLE IF NOT EXISTS portal_files (
  file_id     TEXT PRIMARY KEY,
  portal_id   TEXT NOT NULL,
  kind        TEXT NOT NULL,
  filename    TEXT NOT NULL,
  size_bytes  INTEGER,
  md5         TEXT,
  created_at  TEXT NOT NULL,
  meta_json   TEXT,
  FOREIGN KEY (portal_id) REFERENCES portals(portal_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portal_files_portal_kind
  ON portal_files(portal_id, kind);

-- Human-approved selection per portal (simple and user-friendly)
CREATE TABLE IF NOT EXISTS approvals (
  portal_id          TEXT PRIMARY KEY,
  proteome_file_id   TEXT NOT NULL,
  cds_file_id        TEXT,
  approved_at        TEXT NOT NULL,
  note              TEXT,
  FOREIGN KEY (portal_id) REFERENCES portals(portal_id) ON DELETE CASCADE,
  FOREIGN KEY (proteome_file_id) REFERENCES portal_files(file_id),
  FOREIGN KEY (cds_file_id) REFERENCES portal_files(file_id)
);

-- Immutable staging snapshots
CREATE TABLE IF NOT EXISTS stagings (
  staging_id      TEXT PRIMARY KEY,
  created_at      TEXT NOT NULL,
  manifest_path   TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL
);

-- Compute runs (orthofinder, interproscan, species_tree, family, etc.)
CREATE TABLE IF NOT EXISTS runs (
  run_id          TEXT PRIMARY KEY,
  staging_id      TEXT NOT NULL,
  kind            TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  manifest_path   TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  FOREIGN KEY (staging_id) REFERENCES stagings(staging_id)
);