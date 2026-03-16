from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

from fungalphylo.core.fasta import iter_fasta

_CANONICAL_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+\|\d+$")  # {portal_id}|{jgi_protein_id}


def validate_canonical_protein_id(protein_id: str) -> None:
    if not _CANONICAL_ID_RE.match(protein_id):
        raise ValueError(
            f"Invalid canonical protein id {protein_id!r}. "
            f"Expected format '{{portal_id}}|{{jgi_protein_id}}' where jgi_protein_id is numeric."
        )


def validate_fasta_headers_are_canonical(path: Path, *, max_errors: int = 20) -> None:
    """
    Ensure all FASTA headers match the canonical pattern.
    """
    errors = []
    for rec in iter_fasta(path):
        header = rec.header.split()[0]  # just in case, but staged output should never have whitespace
        if not _CANONICAL_ID_RE.match(header):
            errors.append(header)
            if len(errors) >= max_errors:
                break

    if errors:
        sample = ", ".join(repr(e) for e in errors[:5])
        raise ValueError(f"Non-canonical FASTA headers in {path}: {sample} (and {len(errors)-5} more)")


def validate_mapping_file_rows(
    rows: Iterable[tuple[str, str, int]],
    *,
    max_errors: int = 20,
) -> None:
    """
    Validate mapping rows: (canonical_id, original_header, length)
    """
    errors: list[str] = []
    for canon, orig, length in rows:
        try:
            validate_canonical_protein_id(canon)
        except ValueError as e:
            errors.append(str(e))
        if not orig:
            errors.append(f"Empty original header for {canon}")
        if length <= 0:
            errors.append(f"Non-positive length for {canon}: {length}")
        if len(errors) >= max_errors:
            break

    if errors:
        raise ValueError("Mapping file validation failed:\n" + "\n".join(errors))