from __future__ import annotations

from pathlib import Path

import pytest

from fungalphylo.cli.commands.fetch_index import classify_kind
from fungalphylo.core.fasta import FastaRecord, count_fasta, iter_fasta, write_fasta
from fungalphylo.core.idmap import load_id_map, resolve_id_map_file
from fungalphylo.core.resolve import resolve_raw_path


def test_resolve_raw_path_uses_project_relative_layout(tmp_path: Path) -> None:
    path = resolve_raw_path(
        tmp_path,
        raw_layout="raw/{portal_id}/{file_id}/{filename}",
        portal_id="PortalA",
        file_id="file1",
        filename="proteins.faa",
    )

    assert path == tmp_path / "raw" / "PortalA" / "file1" / "proteins.faa"


def test_resolve_raw_path_rejects_parent_escape(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsafe raw_layout"):
        resolve_raw_path(
            tmp_path,
            raw_layout="raw/{portal_id}/../{filename}",
            portal_id="PortalA",
            file_id="file1",
            filename="proteins.faa",
        )


def test_fasta_roundtrip_and_count_support_gzip(tmp_path: Path) -> None:
    path = tmp_path / "records.faa.gz"
    write_fasta(
        [
            FastaRecord(header="PortalA|1001", sequence="mpeq aa"),
            FastaRecord(header="PortalA|1002", sequence="TTtt"),
        ],
        path,
    )

    records = list(iter_fasta(path))

    assert records == [
        FastaRecord(header="PortalA|1001", sequence="MPEQAA"),
        FastaRecord(header="PortalA|1002", sequence="TTTT"),
    ]
    assert count_fasta(path) == (2, 10)


def test_classify_kind_uses_metadata_before_filename() -> None:
    assert classify_kind("weird.bin", "txt", "Proteins Filtered", ["misc"]) == "proteome"
    assert classify_kind("genes.gff3", "gff", "", ["gene models"]) == "gff"
    assert classify_kind("assembly.fa", "fasta", "", ["assembly"]) == "assembly"


def test_load_id_map_drops_na_rows_and_prefers_per_portal_file(tmp_path: Path) -> None:
    idmaps_dir = tmp_path / "idmaps"
    idmaps_dir.mkdir()
    proteome_map = idmaps_dir / "PortalA.proteome.tsv"
    proteome_map.write_text(
        "\n".join(
            [
                "canonical_protein_id\toriginal_header\tmodel_id\ttranscript_id",
                "PortalA|1001\torig_a\tmodelA\ttxA",
                "NA\torig_b\tmodelB\ttxB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    resolved = resolve_id_map_file(idmaps_dir, "PortalA", kind="proteome")
    portal_map = load_id_map(idmaps_dir, "PortalA", kind="proteome")

    assert resolved == proteome_map
    assert portal_map.header_to_canon == {"orig_a": "PortalA|1001"}
    assert portal_map.model_to_canon == {"modelA": "PortalA|1001"}
    assert portal_map.model_to_transcript == {"modelA": "txA"}

