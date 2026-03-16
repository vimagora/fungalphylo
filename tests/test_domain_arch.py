from __future__ import annotations

from fungalphylo.core.domain_arch import build_domain_architectures, compute_max_evalues
from fungalphylo.core.ipr_tsv import IprHit


def _hit(protein_id: str, accession: str, start: int, end: int, evalue: float) -> IprHit:
    return IprHit(
        protein_id=protein_id,
        analysis="Pfam",
        accession=accession,
        description="test",
        start=start,
        end=end,
        evalue=evalue,
    )


def test_build_domain_architectures() -> None:
    hits = [
        _hit("p1", "PF00083", 260, 480, 1e-30),
        _hit("p1", "PF00324", 10, 250, 1e-50),
        _hit("p2", "PF00083", 5, 200, 1e-40),
    ]
    archs = build_domain_architectures(hits)
    # p1 should be sorted by start: PF00324 first, then PF00083
    assert archs["p1"] == ("PF00324", "PF00083")
    assert archs["p2"] == ("PF00083",)


def test_build_domain_architectures_with_filter() -> None:
    hits = [
        _hit("p1", "PF00083", 10, 250, 1e-50),
        _hit("p1", "PF00324", 260, 480, 1e-30),
        _hit("p1", "PF99999", 500, 600, 1e-10),
    ]
    archs = build_domain_architectures(hits, target_pfams={"PF00083", "PF00324"})
    assert archs["p1"] == ("PF00083", "PF00324")


def test_compute_max_evalues() -> None:
    hits = [
        _hit("p1", "PF00083", 10, 250, 1e-50),
        _hit("p2", "PF00083", 5, 200, 1e-30),
        _hit("p1", "PF00324", 260, 480, 1e-40),
        _hit("p3", "PF00324", 100, 300, 1e-20),
    ]
    maxes = compute_max_evalues(hits, {"PF00083", "PF00324"})
    assert maxes["PF00083"] == 1e-30  # worst (largest) evalue
    assert maxes["PF00324"] == 1e-20
