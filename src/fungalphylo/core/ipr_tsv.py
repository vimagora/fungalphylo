from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class IprHit:
    """A single InterProScan TSV hit record."""

    protein_id: str
    analysis: str
    accession: str
    description: str
    start: int
    end: int
    evalue: float


def parse_ipr_tsv(path: Path) -> Iterator[IprHit]:
    """
    Stream IprHit records from an InterProScan TSV file.

    InterProScan TSV has 11-15 columns. Key columns (0-indexed):
      0: protein_id
      3: analysis (e.g. Pfam)
      4: accession (e.g. PF00083)
      5: description
      6: start
      7: end
      8: evalue (may be "-" or empty for some analyses)
    """
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n\r")
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) < 9:
                continue

            protein_id = cols[0]
            analysis = cols[3]
            accession = cols[4]
            description = cols[5]

            try:
                start = int(cols[6])
            except (ValueError, IndexError):
                start = 0
            try:
                end = int(cols[7])
            except (ValueError, IndexError):
                end = 0

            evalue_raw = cols[8].strip()
            if evalue_raw in ("", "-"):
                evalue = float("inf")
            else:
                try:
                    evalue = float(evalue_raw)
                except ValueError:
                    evalue = float("inf")

            yield IprHit(
                protein_id=protein_id,
                analysis=analysis,
                accession=accession,
                description=description,
                start=start,
                end=end,
                evalue=evalue,
            )


def filter_by_accessions(
    hits: Iterator[IprHit], accessions: set[str]
) -> Iterator[IprHit]:
    """Filter hits to only those matching the given accessions."""
    for hit in hits:
        if hit.accession in accessions:
            yield hit
