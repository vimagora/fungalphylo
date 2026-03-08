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
  ncbi_taxon_id  INTEGER,
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

-- Legacy mutable staging table removed in favor of snapshot-scoped staging_files.
DROP TABLE IF EXISTS staged_files;

-- Snapshot-scoped staged artifacts. Multiple snapshots may reference equivalent
-- artifacts via identical cache keys, but each snapshot gets its own immutable
-- artifact path under staging/<staging_id>/...
CREATE TABLE IF NOT EXISTS staging_files (
  staging_id            TEXT NOT NULL,
  portal_id             TEXT NOT NULL,
  kind                  TEXT NOT NULL, -- 'proteome' or 'cds'
  source_file_id        TEXT NOT NULL, -- source portal_files.file_id
  raw_sha256            TEXT NOT NULL,
  artifact_path         TEXT NOT NULL,
  artifact_sha256       TEXT NOT NULL,
  artifact_cache_key    TEXT NOT NULL,
  reused_from_staging_id TEXT,
  created_at            TEXT NOT NULL,
  params_json           TEXT,
  PRIMARY KEY (staging_id, portal_id, kind),
  FOREIGN KEY (staging_id) REFERENCES stagings(staging_id) ON DELETE CASCADE,
  FOREIGN KEY (portal_id) REFERENCES portals(portal_id) ON DELETE CASCADE,
  FOREIGN KEY (source_file_id) REFERENCES portal_files(file_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_files_cache_key
  ON staging_files(kind, artifact_cache_key);

CREATE TABLE IF NOT EXISTS restore_requests (
  request_id         TEXT PRIMARY KEY,
  created_at         TEXT NOT NULL,
  request_dir        TEXT NOT NULL,
  dry_run            INTEGER NOT NULL DEFAULT 0,
  status             TEXT NOT NULL,
  n_payloads         INTEGER NOT NULL DEFAULT 0,
  n_posted           INTEGER NOT NULL DEFAULT 0,
  n_errors           INTEGER NOT NULL DEFAULT 0,
  send_mail          INTEGER NOT NULL DEFAULT 1,
  max_chars          INTEGER NOT NULL,
  continue_on_error  INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_restore_requests_created_at
  ON restore_requests(created_at DESC);

CREATE TABLE IF NOT EXISTS download_requests (
  request_id            TEXT PRIMARY KEY,
  created_at            TEXT NOT NULL,
  request_dir           TEXT NOT NULL,
  dry_run               INTEGER NOT NULL DEFAULT 0,
  status                TEXT NOT NULL,
  n_payloads            INTEGER NOT NULL DEFAULT 0,
  n_payload_ok          INTEGER NOT NULL DEFAULT 0,
  n_errors              INTEGER NOT NULL DEFAULT 0,
  moved_files           INTEGER NOT NULL DEFAULT 0,
  missing_files         INTEGER NOT NULL DEFAULT 0,
  max_chars             INTEGER NOT NULL,
  timeout_seconds       INTEGER NOT NULL,
  continue_on_error     INTEGER NOT NULL DEFAULT 1,
  skip_if_raw_present   INTEGER NOT NULL DEFAULT 0,
  overwrite_staged      INTEGER NOT NULL DEFAULT 0,
  retain                TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_download_requests_created_at
  ON download_requests(created_at DESC);
