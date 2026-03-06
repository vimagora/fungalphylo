from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Optional

import typer

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.fasta import iter_fasta
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.db.db import connect
from fungalphylo.db.queries import fetch_approvals_with_files

app = typer.Typer(help="Create ID mapping templates for non-JGI header portals.")


def extract_model_token(header: str) -> str:
    """
    Best-effort: first token before whitespace or '|'.
    For many non-JGI headers like 'PTRG_00001 | ...' -> 'PTRG_00001'
    """
    h = header.strip()
    return re.split(r"[\s|]+", h, maxsplit=1)[0]


@app.callback(invoke_without_command=True)
def idmap_command(ctx: typer.Context) -> None:
    # placeholder so `fungalphylo idmap ...` shows help
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


@app.command("template")
def template(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    portal_id: str = typer.Option(..., "--portal-id", help="Portal to build a template for"),
    kind: str = typer.Option("proteome", "--kind", help="proteome or cds"),
    out: Optional[Path] = typer.Option(
        None,
        "--out",
        help="Output TSV path (default: <project>/<staging.default_idmaps_dir>/<portal_id>.<kind>.tsv)",
    ),
    limit: int = typer.Option(0, "--limit", help="Optional limit on number of rows (0 = no limit)"),
) -> None:
    """
    Generate a universal idmap template TSV from the raw downloaded FASTA.

    Output columns:
      canonical_protein_id  model_id  original_header  transcript_id

    User fills canonical_protein_id (and optionally transcript_id).
    """
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    cfg = resolve_config(project_config=load_yaml(paths.config_yaml))
    raw_layout = cfg["staging"]["raw_layout"]
    idmaps_rel = cfg["staging"].get("default_idmaps_dir", "idmaps")

    # Identify approved file for portal/kind
    conn = connect(paths.db_path)
    try:
        approvals = fetch_approvals_with_files(conn, portal_ids=[portal_id])
    finally:
        conn.close()
    if not approvals:
        raise typer.BadParameter(f"No approval found for portal_id={portal_id}. Run review apply first.")

    a = approvals[0]
    if kind == "proteome":
        file_id = a["proteome_file_id"]
        filename = a["proteome_filename"]
    elif kind == "cds":
        if not a["cds_file_id"] or not a["cds_filename"]:
            raise typer.BadParameter(f"Portal {portal_id} has no approved CDS/transcript file.")
        file_id = a["cds_file_id"]
        filename = a["cds_filename"]
    else:
        raise typer.BadParameter("--kind must be 'proteome' or 'cds'")

    raw_path = resolve_raw_path(
        project_dir,
        raw_layout=raw_layout,
        portal_id=portal_id,
        file_id=file_id,
        filename=filename,
    )
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Missing raw file: {raw_path}\nRun download first (after restore completes)."
        )

    if out is None:
        out = project_dir / idmaps_rel / f"{portal_id}.{kind}.tsv"
    out = out.expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    seen = set()
    n = 0
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["canonical_protein_id", "model_id", "original_header", "transcript_id"])
        for rec in iter_fasta(raw_path):
            if rec.header in seen:
                continue
            seen.add(rec.header)

            w.writerow(["", extract_model_token(rec.header), rec.header, ""])
            n += 1
            if limit and n >= limit:
                break

    typer.echo(f"Wrote idmap template: {out}")
    typer.echo(f"Rows: {n}")