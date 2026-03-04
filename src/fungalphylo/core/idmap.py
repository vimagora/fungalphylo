from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


_CANON_RE = re.compile(r"^[A-Za-z0-9_.-]+\|\d+$")


@dataclass(frozen=True)
class PortalIdMap:
    portal_id: str
    header_to_canon: Dict[str, str]        # original_header -> canonical_protein_id (primary)
    model_to_canon: Dict[str, str]         # model_id -> canonical_protein_id (fallback)
    model_to_transcript: Dict[str, str]    # model_id -> transcript_id (optional future use)


def _require_columns(path: Path, fieldnames: list[str] | None, required: set[str]) -> None:
    have = set(fieldnames or [])
    if not required.issubset(have):
        raise ValueError(f"{path} must have columns {sorted(required)}. Found: {fieldnames}")


def _read_per_portal_tsv(path: Path, portal_id: str) -> PortalIdMap:
    path = path.expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"ID map file not found for portal {portal_id}: {path}")

    header_to_canon: Dict[str, str] = {}
    model_to_canon: Dict[str, str] = {}
    model_to_transcript: Dict[str, str] = {}

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")

        if reader.fieldnames is None:
            raise ValueError(f"{path} is missing a header row.")

        fields = set(reader.fieldnames)

        # Detect format:
        # Format B: canonical_protein_id + original_header (model_id optional)
        # Format A: model_id + jgi_protein_id
        is_format_b = "canonical_protein_id" in fields and "original_header" in fields
        is_format_a = "model_id" in fields and "jgi_protein_id" in fields

        if not (is_format_a or is_format_b):
            raise ValueError(
                f"{path} must be either:\n"
                f"  Format B columns: canonical_protein_id, original_header, (optional model_id, transcript_id)\n"
                f"  OR Format A columns: model_id, jgi_protein_id\n"
                f"Found: {reader.fieldnames}"
            )

        for row in reader:
            if is_format_b:
                canon = (row.get("canonical_protein_id") or "").strip()
                orig = (row.get("original_header") or "").strip()
                model = (row.get("model_id") or "").strip()
                tx = (row.get("transcript_id") or "").strip()

                if not canon or not orig:
                    continue
                if not _CANON_RE.match(canon):
                    raise ValueError(f"{path}: invalid canonical_protein_id: {canon!r}")

                if orig in header_to_canon and header_to_canon[orig] != canon:
                    raise ValueError(f"{path}: conflicting mapping for original_header={orig!r}")
                header_to_canon[orig] = canon

                if model:
                    if model in model_to_canon and model_to_canon[model] != canon:
                        raise ValueError(f"{path}: conflicting mapping for model_id={model!r}")
                    model_to_canon[model] = canon
                    if tx:
                        model_to_transcript[model] = tx

            else:
                # Format A
                model = (row.get("model_id") or "").strip()
                jgi = (row.get("jgi_protein_id") or "").strip()
                if not model or not jgi:
                    continue
                canon = f"{portal_id}|{jgi}"
                if model in model_to_canon and model_to_canon[model] != canon:
                    raise ValueError(f"{path}: conflicting mapping for model_id={model!r}")
                model_to_canon[model] = canon

    if not header_to_canon and not model_to_canon:
        raise ValueError(f"{path} contains no usable mappings.")

    return PortalIdMap(
        portal_id=portal_id,
        header_to_canon=header_to_canon,
        model_to_canon=model_to_canon,
        model_to_transcript=model_to_transcript,
    )


def load_id_map(id_map_path: Path, portal_id: str) -> PortalIdMap:
    """
    Load mapping for one portal.

    id_map_path may be:
      - a directory containing <portal_id>.tsv
      - a single TSV containing portal_id column plus either Format A or Format B columns
    """
    id_map_path = id_map_path.expanduser().resolve()

    if id_map_path.is_dir():
        return _read_per_portal_tsv(id_map_path / f"{portal_id}.tsv", portal_id)

    if not id_map_path.exists():
        raise FileNotFoundError(f"--id-map not found: {id_map_path}")

    # Combined TSV. Filter rows by portal_id into a temp in-memory map.
    header_to_canon: Dict[str, str] = {}
    model_to_canon: Dict[str, str] = {}
    model_to_transcript: Dict[str, str] = {}

    with id_map_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        _require_columns(id_map_path, reader.fieldnames, {"portal_id"})

        fields = set(reader.fieldnames or [])
        is_format_b = "canonical_protein_id" in fields and "original_header" in fields
        is_format_a = "model_id" in fields and "jgi_protein_id" in fields

        if not (is_format_a or is_format_b):
            raise ValueError(
                f"{id_map_path} must include portal_id and either Format A or Format B columns."
            )

        for row in reader:
            pid = (row.get("portal_id") or "").strip()
            if pid != portal_id:
                continue

            if is_format_b:
                canon = (row.get("canonical_protein_id") or "").strip()
                orig = (row.get("original_header") or "").strip()
                model = (row.get("model_id") or "").strip()
                tx = (row.get("transcript_id") or "").strip()
                if not canon or not orig:
                    continue
                if not _CANON_RE.match(canon):
                    raise ValueError(f"{id_map_path}: invalid canonical_protein_id: {canon!r}")

                if orig in header_to_canon and header_to_canon[orig] != canon:
                    raise ValueError(f"{id_map_path}: conflicting mapping for original_header={orig!r}")
                header_to_canon[orig] = canon

                if model:
                    if model in model_to_canon and model_to_canon[model] != canon:
                        raise ValueError(f"{id_map_path}: conflicting mapping for model_id={model!r}")
                    model_to_canon[model] = canon
                    if tx:
                        model_to_transcript[model] = tx
            else:
                model = (row.get("model_id") or "").strip()
                jgi = (row.get("jgi_protein_id") or "").strip()
                if not model or not jgi:
                    continue
                canon = f"{portal_id}|{jgi}"
                if model in model_to_canon and model_to_canon[model] != canon:
                    raise ValueError(f"{id_map_path}: conflicting mapping for model_id={model!r}")
                model_to_canon[model] = canon

    if not header_to_canon and not model_to_canon:
        raise ValueError(f"No mappings for portal {portal_id} found in {id_map_path}.")

    return PortalIdMap(
        portal_id=portal_id,
        header_to_canon=header_to_canon,
        model_to_canon=model_to_canon,
        model_to_transcript=model_to_transcript,
    )