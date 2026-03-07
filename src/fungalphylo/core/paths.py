from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """
    Canonical filesystem layout for a fungalphylo project directory.

    All commands should resolve paths via this object instead of hardcoding.
    """
    root: Path

    # --- top-level ---
    @property
    def config_yaml(self) -> Path:
        return self.root / "config.yaml"
    
    @property
    def tools_yaml(self) -> Path:
        return self.root / "tools.yaml"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def events_log(self) -> Path:
        return self.logs_dir / "events.jsonl"
    
    @property
    def errors_log(self) -> Path:
        return self.logs_dir / "errors.jsonl"

    # --- database ---
    @property
    def db_dir(self) -> Path:
        return self.root / "db"

    @property
    def db_path(self) -> Path:
        return self.db_dir / "fungalphylo.sqlite"

    # --- raw downloads cache ---
    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    def raw_file_dir(self, portal_id: str, file_id: str) -> Path:
        """
        Directory where a single downloaded source file lives.
        Convention: raw/<portal_id>/<file_id>/
        """
        return self.raw_dir / portal_id / file_id
    
    # --- jgi cache ---
    @property
    def cache_dir(self) -> Path:
        return self.root / "cache"

    @property
    def jgi_index_cache_dir(self) -> Path:
        return self.cache_dir / "jgi_index_json"

    # --- staging ---
    @property
    def staging_root(self) -> Path:
        return self.root / "staging"

    def staging_dir(self, staging_id: str) -> Path:
        return self.staging_root / staging_id

    def staging_manifest(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "manifest.json"

    def staging_proteomes_dir(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "proteomes"

    def staging_cds_dir(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "cds"

    def staging_protein_id_map(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "protein_id_map.tsv.gz"

    def staging_checksums(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "checksums.tsv"

    def staging_reports_dir(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "reports"

    def staging_idmaps_dir(self, staging_id: str) -> Path:
        return self.staging_dir(staging_id) / "idmaps"

    def staging_generated_idmaps_dir(self, staging_id: str) -> Path:
        return self.staging_idmaps_dir(staging_id) / "generated"

    def staging_generated_protein_id_map(self, staging_id: str, portal_id: str) -> Path:
        return self.staging_generated_idmaps_dir(staging_id) / f"{portal_id}.protein_id_map.tsv"

    # --- input idmaps ---
    @property
    def idmaps_dir(self) -> Path:
        return self.root / "idmaps"

    # --- compute runs ---
    @property
    def runs_root(self) -> Path:
        return self.root / "runs"

    def run_dir(self, run_id: str) -> Path:
        return self.runs_root / run_id

    def run_config_dir(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "config"

    def run_resolved_config(self, run_id: str) -> Path:
        return self.run_config_dir(run_id) / "resolved.yaml"

    def run_inputs_dir(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "inputs"

    def run_work_dir(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "work"

    def run_results_dir(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "results"

    def run_logs_dir(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "logs"

    def run_manifest(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "manifest.json"

    def run_marker_started(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "STARTED"

    def run_marker_done(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "DONE"


def ensure_project_dirs(p: ProjectPaths) -> None:
    """
    Create the base directories for a new or existing project.
    Safe to call repeatedly.
    """
    for d in [
        p.root,
        p.logs_dir,
        p.db_dir,
        p.raw_dir,
        p.staging_root,
        p.runs_root,
        p.cache_dir,
        p.idmaps_dir,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def is_project_dir(path: Path) -> bool:
    """
    Lightweight check: does this look like a fungalphylo project directory?
    """
    return (path / "db").exists() and (path / "config.yaml").exists()
