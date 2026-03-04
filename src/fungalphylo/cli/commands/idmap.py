from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.fasta import iter_fasta
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.db.db import connect
from fungalphylo.db.queries import fetch_approvals_with_files
import re

app = typer.Typer(help="Create and manage ID mapping templates for non-JGI portals.")


def extract_model_token(header: str) -> str:
    h = header.strip()
    token = re.split(r"[\s|]+", h, maxsplit=1)[0]
    return token


@app.command("template")
def template(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    portal_id: str = typer.Option(..., "--portal-id", help="Portal to build a template for"),
    kind: str = typer.Option("proteome", "--kind", help="proteome or cds"),
    out_path: Optional[Path] = typer.Option(None, "--out", help="Output TSV path (default: <project>/idmaps/<portal>.tsv)"),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    cfg = resolve_config(project_config=load_yaml(paths.config_yaml))
    raw_layout = cfg["staging"]["raw_layout"]

    conn = connect(paths.db_path)
    try:
        approvals = fetch_approvals_with_files(conn, portal_ids=[portal_id])
    finally:
        conn.close()
    if not approvals:
        raise typer.BadParameter(f"No approval found for portal_id={portal_id}")

    a = approvals[0]
    if kind == "proteome":
        file_id = a["proteome_file_id"]
        filename = a["proteome_filename"]
    elif kind == "cds":
        if not a["cds_file_id"] or not a["cds_filename"]:
            raise typer.BadParameter(f"Portal {portal_id} has no approved CDS file.")
        file_id = a["cds_file_id"]
        filename = a["cds_filename"]
    else:
        raise typer.BadParameter("--kind must be 'proteome' or 'cds'")

    raw_path = resolve_raw_path(project_dir, raw_layout=raw_layout, portal_id=portal_id, file_id=file_id, filename=filename)
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw file: {raw_path}")

    if out_path is None:
        out_path = project_dir / "idmaps" / f"{portal_id}.{kind}.tsv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    seen_headers = set()
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["canonical_protein_id", "model_id", "original_header", "transcript_id"])
        for rec in iter_fasta(raw_path):
            if rec.header in seen_headers:
                continue
            seen_headers.add(rec.header)
            w.writerow(["", extract_model_token(rec.header), rec.header, ""])

    typer.echo(f"Wrote template: {out_path}")