from __future__ import annotations

import math
from pathlib import Path

from fungalphylo.core.ipr_tsv import filter_by_accessions, parse_ipr_tsv

IPR_TSV_CONTENT = """\
protein1\t1234\t500\tPfam\tPF00083\tSugar transporter\t10\t250\t1.2e-50\tT\t2024-01-01\tIPR005828\tMFS_1
protein1\t1234\t500\tPfam\tPF00324\tAmino acid permease\t260\t480\t3.4e-20\tT\t2024-01-01\tIPR004841\tAA_permease
protein2\t567\t300\tPfam\tPF00083\tSugar transporter\t5\t200\t2.1e-40\tT\t2024-01-01\tIPR005828\tMFS_1
protein3\t890\t400\tGene3D\tG3DSA:1.20.1250.10\tSome domain\t1\t100\t-\tT\t2024-01-01
"""


def test_parse_ipr_tsv(tmp_path: Path) -> None:
    tsv = tmp_path / "test.tsv"
    tsv.write_text(IPR_TSV_CONTENT, encoding="utf-8")

    hits = list(parse_ipr_tsv(tsv))
    assert len(hits) == 4

    assert hits[0].protein_id == "protein1"
    assert hits[0].analysis == "Pfam"
    assert hits[0].accession == "PF00083"
    assert hits[0].description == "Sugar transporter"
    assert hits[0].start == 10
    assert hits[0].end == 250
    assert hits[0].evalue == 1.2e-50

    assert hits[1].accession == "PF00324"
    assert hits[1].evalue == 3.4e-20

    # "-" evalue should become inf
    assert math.isinf(hits[3].evalue)


def test_parse_skips_comments_and_blank_lines(tmp_path: Path) -> None:
    tsv = tmp_path / "test.tsv"
    tsv.write_text(
        "# header comment\n"
        "\n"
        "p1\t1\t100\tPfam\tPF00083\tdesc\t1\t50\t1e-10\tT\t2024-01-01\n",
        encoding="utf-8",
    )
    hits = list(parse_ipr_tsv(tsv))
    assert len(hits) == 1
    assert hits[0].protein_id == "p1"


def test_parse_skips_short_lines(tmp_path: Path) -> None:
    tsv = tmp_path / "test.tsv"
    tsv.write_text("too\tfew\tcolumns\n", encoding="utf-8")
    hits = list(parse_ipr_tsv(tsv))
    assert len(hits) == 0


def test_filter_by_accessions(tmp_path: Path) -> None:
    tsv = tmp_path / "test.tsv"
    tsv.write_text(IPR_TSV_CONTENT, encoding="utf-8")

    all_hits = parse_ipr_tsv(tsv)
    filtered = list(filter_by_accessions(all_hits, {"PF00083"}))
    assert len(filtered) == 2
    assert all(h.accession == "PF00083" for h in filtered)


def test_filter_empty_accessions(tmp_path: Path) -> None:
    tsv = tmp_path / "test.tsv"
    tsv.write_text(IPR_TSV_CONTENT, encoding="utf-8")

    filtered = list(filter_by_accessions(parse_ipr_tsv(tsv), set()))
    assert len(filtered) == 0
