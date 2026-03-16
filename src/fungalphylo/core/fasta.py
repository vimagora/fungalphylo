from __future__ import annotations

import gzip
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


@dataclass(frozen=True)
class FastaRecord:
    header: str  # without leading '>'
    sequence: str  # uppercase, no whitespace


def _open_text(path: Path, mode: str) -> TextIO:
    """
    Open a text file that may be gzipped.
    mode must be 'rt' or 'wt'.
    """
    path = path.expanduser().resolve()
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8")  # type: ignore[return-value]
    return path.open(mode, encoding="utf-8")


def iter_fasta(path: Path) -> Iterator[FastaRecord]:
    """
    Stream FASTA records from a file (supports .gz).

    Rules:
      - header line starts with '>'
      - sequences may span multiple lines
      - yields header without '>' and sequence as uppercase with no whitespace
    """
    header: str | None = None
    seq_parts: list[str] = []

    with _open_text(path, "rt") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith(">"):
                if header is not None:
                    seq = "".join(seq_parts).replace(" ", "").replace("\t", "").upper()
                    yield FastaRecord(header=header, sequence=seq)
                header = line[1:].strip()
                seq_parts = []
            else:
                seq_parts.append(line)

        # last record
        if header is not None:
            seq = "".join(seq_parts).replace(" ", "").replace("\t", "").upper()
            yield FastaRecord(header=header, sequence=seq)


def write_fasta(
    records: Iterable[FastaRecord],
    out_path: Path,
    *,
    wrap: int = 60,
) -> None:
    """
    Write FASTA records to a file (supports .gz).
    """
    out_path = out_path.expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with _open_text(out_path, "wt") as f:
        for rec in records:
            if not rec.header:
                raise ValueError("FASTA record header is empty")
            if any(c.isspace() for c in rec.header):
                # Not fatal, but we enforce "no whitespace in output headers"
                raise ValueError(f"FASTA header contains whitespace: {rec.header!r}")

            seq = rec.sequence.replace(" ", "").replace("\t", "").replace("\n", "").upper()
            if not seq:
                # allow empty? usually not desirable
                raise ValueError(f"FASTA record has empty sequence for header: {rec.header!r}")

            f.write(f">{rec.header}\n")
            if wrap and wrap > 0:
                for i in range(0, len(seq), wrap):
                    f.write(seq[i : i + wrap] + "\n")
            else:
                f.write(seq + "\n")


def count_fasta(path: Path) -> tuple[int, int]:
    """
    Return (num_records, total_residues).
    """
    n = 0
    total = 0
    for rec in iter_fasta(path):
        n += 1
        total += len(rec.sequence)
    return n, total