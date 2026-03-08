from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fungalphylo.core.manifest import read_manifest
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.core.tabular import read_table


def resolve_portal_id(text: str) -> str:
    text = str(text).strip()
    stem = Path(text).stem
    return stem[:-4] if stem.endswith(".faa") else stem


def batch_root_name(staging_id: str, run_id: str) -> str:
    return f"busco_{staging_id}_{run_id}"


def expected_batch_root(paths: ProjectPaths, run_id: str, staging_id: str) -> Path:
    return paths.run_dir(run_id) / "busco_results" / batch_root_name(staging_id, run_id)


def load_run_manifest(paths: ProjectPaths, run_id: str) -> dict[str, Any]:
    return read_manifest(paths.run_manifest(run_id))


def resolve_batch_root(paths: ProjectPaths, run_id: str, manifest: Optional[dict[str, Any]] = None) -> Path:
    if manifest is None:
        manifest = load_run_manifest(paths, run_id)
    batch_root = (
        manifest.get("paths", {}).get("batch_root")
        or manifest.get("paths", {}).get("results_batch_dir")
    )
    if batch_root:
        return (paths.root / batch_root).resolve()
    staging_id = manifest.get("staging_id")
    if not staging_id:
        raise ValueError(f"BUSCO manifest for run {run_id} is missing staging_id.")
    return expected_batch_root(paths, run_id, str(staging_id))


def resolve_batch_summary(paths: ProjectPaths, run_id: str, manifest: Optional[dict[str, Any]] = None) -> Path:
    if manifest is None:
        manifest = load_run_manifest(paths, run_id)
    summary_path = manifest.get("paths", {}).get("batch_summary")
    if summary_path:
        return (paths.root / summary_path).resolve()
    return resolve_batch_root(paths, run_id, manifest) / "batch_summary.txt"


def parse_batch_summary(path: Path) -> list[dict[str, Any]]:
    meta, rows = read_table(path)
    if meta.delimiter != "\t":
        raise ValueError(f"BUSCO batch summary must be tab-delimited: {path}")

    parsed: list[dict[str, Any]] = []
    for row in rows:
        input_filename = (row.get("Input_file") or "").strip()
        lineage = (row.get("Dataset") or "").strip()
        if not input_filename or not lineage:
            continue
        parsed.append(
            {
                "portal_id": resolve_portal_id(input_filename),
                "input_filename": input_filename,
                "lineage": lineage,
                "complete_pct": float((row.get("Complete") or "0").strip() or 0.0),
                "single_pct": float((row.get("Single") or "0").strip() or 0.0),
                "duplicated_pct": float((row.get("Duplicated") or "0").strip() or 0.0),
                "fragmented_pct": float((row.get("Fragmented") or "0").strip() or 0.0),
                "missing_pct": float((row.get("Missing") or "0").strip() or 0.0),
                "n_markers": int(float((row.get("n_markers") or "0").strip() or 0.0)),
            }
        )
    return parsed
