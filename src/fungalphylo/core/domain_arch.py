from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from fungalphylo.core.ipr_tsv import IprHit


def build_domain_architectures(
    hits: Iterable[IprHit],
    target_pfams: set[str] | None = None,
) -> dict[str, tuple[str, ...]]:
    """
    Build domain architectures per protein from IprHit records.

    Returns {protein_id: (accession1, accession2, ...)} ordered by start position.
    If target_pfams is provided, only those accessions are included.
    """
    protein_domains: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for hit in hits:
        if target_pfams and hit.accession not in target_pfams:
            continue
        protein_domains[hit.protein_id].append((hit.start, hit.accession))

    result: dict[str, tuple[str, ...]] = {}
    for protein_id, domains in protein_domains.items():
        domains.sort(key=lambda x: x[0])
        result[protein_id] = tuple(acc for _, acc in domains)
    return result


def compute_max_evalues(
    hits: Iterable[IprHit], target_pfams: set[str]
) -> dict[str, float]:
    """
    Compute the maximum (worst) e-value per target Pfam across all hits.

    Used to establish thresholds from characterized proteins.
    """
    max_evals: dict[str, float] = {}
    for hit in hits:
        if hit.accession not in target_pfams:
            continue
        current = max_evals.get(hit.accession, 0.0)
        if hit.evalue > current:
            max_evals[hit.accession] = hit.evalue
    return max_evals
