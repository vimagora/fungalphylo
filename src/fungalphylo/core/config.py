from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml


DEFAULT_CONFIG: Dict[str, Any] = {
    # General project-level settings
    "project": {
        # Put absolute paths here only if you need them; otherwise keep things relative to project_dir.
        "name": "fungalphylo-project",
    },
    # Staging (your accepted decisions)
    "staging": {
        "protein_id_scheme": "{portal_id}|{jgi_protein_id}",
        "min_aa": 30,
        "max_aa": 10000,
        # Where staging expects downloaded raw files to be located.
        # Convention: raw/<portal_id>/<file_id>/<filename>
        "raw_layout": "raw/{portal_id}/{file_id}/{filename}",
        "default_idmaps_dir": "idmaps",
    },
    # Autoselect explainability
    "autoselect": {
        "top_n": 5,
        # Keep this flexible; we’ll define scoring later in the autoselect module.
        "weights": {
            "label_priority": 10.0,
            "status_priority": 5.0,
            "newer_modified": 1.0,
            "larger_size": 0.5,
        },
        "ban_patterns": [],
    },
    # Compute defaults (placeholders for later)
    "compute": {
        "snakemake": {
            "cores": 8,
            "use_conda": True,
            "profile": None,
        },
        "orthofinder": {
            "enabled": True,
            "params": {},
        },
        "interproscan": {
            "enabled": False,
            "params": {},
        },
        "species_tree": {
            "occupancy_min": 0.75,
            # Optional heuristics; default off so users can explore first.
            "exclusion_heuristics": {
                "enabled": False,
                "min_aln_len": 200,
                "max_missingness": 0.5,
            },
        },
        "family": {
            "codon_mode_fail_soft": True,
            "filter_mode": "og_only",  # og_only | evalue_cutoff_from_seeds | shared_domain_architecture | evalue_then_architecture
        },
    },
}


def write_default_config(path: Path, *, overwrite: bool = True) -> None:
    """
    Write a default YAML config to `path`.
    """
    path = path.expanduser().resolve()
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(DEFAULT_CONFIG, f, sort_keys=False)


def load_yaml(path: Path) -> Dict[str, Any]:
    path = path.expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a YAML mapping (dict). Got: {type(data)}")
    return data


def deep_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge `override` into `base` and return a new dict.
    - dict values merge recursively
    - non-dict values replace
    """
    out: Dict[str, Any] = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)  # type: ignore[arg-type]
        else:
            out[k] = v
    return out


def resolve_config(
    *,
    project_config: Mapping[str, Any],
    run_overrides: Optional[Mapping[str, Any]] = None,
    cli_overrides: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Resolve config with precedence:
      1) cli_overrides
      2) run_overrides
      3) project_config
      4) DEFAULT_CONFIG (fallbacks)

    Returns a new dict.
    """
    cfg = deep_merge(DEFAULT_CONFIG, project_config)
    if run_overrides:
        cfg = deep_merge(cfg, run_overrides)
    if cli_overrides:
        cfg = deep_merge(cfg, cli_overrides)
    return cfg