from __future__ import annotations

import csv
import json
from html import escape
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db


def _find_of_results_dir(results_root: Path) -> Path:
    candidates = sorted(results_root.glob("Results_*"), reverse=True)
    for c in candidates:
        if (c / "Orthogroups").is_dir():
            return c
    raise typer.BadParameter(
        f"No OrthoFinder Results_* directory found in {results_root}"
    )


def _parse_orthogroups_tsv(
    og_tsv: Path,
) -> tuple[list[str], dict[str, dict[str, list[str]]]]:
    """Parse Orthogroups.tsv.

    Returns (portal_names, {og_id: {portal: [gene_ids]}}).
    """
    with og_tsv.open(encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        header = next(reader)
        # Header: Orthogroup \t Portal0 \t Portal1 \t ...
        portals = header[1:]
        og_data: dict[str, dict[str, list[str]]] = {}
        for row in reader:
            if not row or not row[0].strip():
                continue
            og_id = row[0]
            genes: dict[str, list[str]] = {}
            for i, portal in enumerate(portals):
                cell = row[i + 1].strip() if i + 1 < len(row) else ""
                if cell:
                    genes[portal] = [g.strip() for g in cell.split(",") if g.strip()]
                else:
                    genes[portal] = []
            og_data[og_id] = genes
    return portals, og_data


def _load_characterized(char_tsv: Path) -> list[dict[str, str]]:
    with char_tsv.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        return list(reader)


def _match_characterized_to_ogs(
    og_data: dict[str, dict[str, list[str]]],
    char_rows: list[dict[str, str]],
) -> dict[str, set[str]]:
    """Return {og_id: {short_name, ...}} for OGs containing characterized genes.

    Matches characterized headers (short_name|protein_name) against gene IDs in OGs.
    """
    # Build set of characterized headers
    char_headers: dict[str, str] = {}  # header -> short_name
    for row in char_rows:
        sn = row.get("short_name", "").strip()
        pn = row.get("protein_name", "").strip()
        if sn and pn:
            char_headers[f"{sn}|{pn}"] = sn

    og_char: dict[str, set[str]] = {}
    for og_id, portal_genes in og_data.items():
        found: set[str] = set()
        for gene_list in portal_genes.values():
            for gene_id in gene_list:
                if gene_id in char_headers:
                    found.add(char_headers[gene_id])
        if found:
            og_char[og_id] = found
    return og_char


def _write_tsv(rows: list[list[str]], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        for row in rows:
            writer.writerow(row)


def _write_html_table(
    headers: list[str], rows: list[list[str]], path: Path, title: str
) -> None:
    html_parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        f"<title>{escape(title)}</title>",
        "<style>",
        "body { font-family: monospace; font-size: 13px; margin: 20px; }",
        "h1 { font-size: 18px; }",
        "table { border-collapse: collapse; }",
        "th, td { border: 1px solid #ccc; padding: 4px 8px; text-align: center; }",
        "th { background: #f0f0f0; position: sticky; top: 0; }",
        "tr:nth-child(even) { background: #fafafa; }",
        ".zero { color: #ccc; }",
        ".one { background: #d4edda; }",
        ".multi { background: #fff3cd; }",
        ".marked { background: #d4edda; font-weight: bold; }",
        "</style>",
        "</head><body>",
        f"<h1>{escape(title)}</h1>",
        "<table>",
        "<tr>" + "".join(f"<th>{escape(h)}</th>" for h in headers) + "</tr>",
    ]
    for row in rows:
        cells = []
        for cell in row:
            css = ""
            if cell == "x":
                css = ' class="marked"'
            elif cell.isdigit():
                n = int(cell)
                if n == 0:
                    css = ' class="zero"'
                elif n == 1:
                    css = ' class="one"'
                elif n > 1:
                    css = ' class="multi"'
            cells.append(f"<td{css}>{escape(str(cell))}</td>")
        html_parts.append("<tr>" + "".join(cells) + "</tr>")
    html_parts.extend(["</table>", "</body></html>"])
    path.write_text("\n".join(html_parts), encoding="utf-8")


def _write_decision_template(
    og_ids: list[str], path: Path
) -> None:
    lines = [
        "# OG Decision Template",
        "# Edit this file to select orthogroups for downstream analysis.",
        "#",
        "# include: OGs to keep as-is (comma-separated)",
        "# merge: groups of OGs to merge (comma-separated within group, semicolon between groups)",
        "#   e.g., merge: OG0000001,OG0000002;OG0000003,OG0000004",
        "#   creates two merged FASTAs",
        "#",
        "# Available OGs (containing characterized genes):",
    ]
    for og_id in og_ids:
        lines.append(f"#   {og_id}")
    lines.append("#")
    lines.append("include:")
    lines.append("")
    lines.append("merge:")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def og_report_command(
    project_dir: Path = typer.Argument(..., help="Project directory."),
    family_id: str = typer.Option(..., "--family-id", help="Gene family identifier."),
    run_id: str | None = typer.Option(
        None, "--run-id",
        help="OrthoFinder run ID (default: auto-detect latest for this family).",
    ),
    results_dir: Path | None = typer.Option(
        None, "--results-dir",
        help="Explicit path to OrthoFinder results root.",
    ),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    # Load characterized proteins
    char_tsv = paths.family_characterized_dir(family_id) / "characterized.tsv"
    if not char_tsv.is_file():
        raise typer.BadParameter(
            f"Characterized TSV not found: {char_tsv}\n"
            f"Run `protsetphylo init --family-id {family_id}` first."
        )
    char_rows = _load_characterized(char_tsv)
    short_names = sorted({r["short_name"].strip() for r in char_rows if r.get("short_name", "").strip()})

    # Resolve OrthoFinder results
    if results_dir is not None:
        of_root = results_dir.expanduser().resolve()
    elif run_id is not None:
        of_root = paths.run_dir(run_id) / "orthofinder_results"
    else:
        # Auto-detect: find latest orthofinder run
        candidates = []
        for manifest_path in paths.runs_root.glob("*/manifest.json"):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                if data.get("kind") == "orthofinder":
                    candidates.append((data.get("created_at", ""), data["run_id"]))
            except (json.JSONDecodeError, KeyError):
                continue
        if not candidates:
            raise typer.BadParameter(
                "No OrthoFinder runs found. Provide --run-id or --results-dir."
            )
        candidates.sort(reverse=True)
        run_id = candidates[0][1]
        of_root = paths.run_dir(run_id) / "orthofinder_results"
        typer.echo(f"Using OrthoFinder run: {run_id}")

    if not of_root.is_dir():
        raise typer.BadParameter(f"Results directory does not exist: {of_root}")

    of_results = _find_of_results_dir(of_root)
    og_tsv = of_results / "Orthogroups" / "Orthogroups.tsv"
    if not og_tsv.is_file():
        raise typer.BadParameter(f"Missing Orthogroups.tsv: {og_tsv}")

    # Parse and match
    portals, og_data = _parse_orthogroups_tsv(og_tsv)
    og_char = _match_characterized_to_ogs(og_data, char_rows)

    if not og_char:
        typer.echo("No orthogroups contain characterized genes.")
        raise typer.Exit(code=0)

    # Sort OGs by number of characterized genes (descending)
    og_ids = sorted(og_char.keys(), key=lambda x: (-len(og_char[x]), x))

    typer.echo(f"Characterized genes: {len(short_names)}")
    typer.echo(f"Total orthogroups:   {len(og_data)}")
    typer.echo(f"OGs with characterized genes: {len(og_ids)}")

    # Output directory
    report_dir = paths.family_og_report_dir(family_id)
    report_dir.mkdir(parents=True, exist_ok=True)

    # Report 1: Characterized × OG
    char_headers = ["orthogroup"] + short_names + ["characterized_count"]
    char_rows_out: list[list[str]] = []
    for og_id in og_ids:
        found = og_char[og_id]
        row = [og_id]
        for sn in short_names:
            row.append("x" if sn in found else "")
        row.append(str(len(found)))
        char_rows_out.append(row)

    _write_tsv(
        [char_headers] + char_rows_out,
        report_dir / "characterized_og_matrix.tsv",
    )
    _write_html_table(
        char_headers,
        char_rows_out,
        report_dir / "characterized_og_matrix.html",
        f"Characterized Genes x Orthogroups — {family_id}",
    )

    # Report 2: Portal × OG (gene counts, only OGs with characterized genes)
    portal_headers = ["orthogroup"] + portals + ["total_genes"]
    portal_rows_out: list[list[str]] = []
    for og_id in og_ids:
        genes = og_data[og_id]
        row = [og_id]
        total = 0
        for portal in portals:
            count = len(genes.get(portal, []))
            total += count
            row.append(str(count))
        row.append(str(total))
        portal_rows_out.append(row)

    _write_tsv(
        [portal_headers] + portal_rows_out,
        report_dir / "portal_og_matrix.tsv",
    )
    _write_html_table(
        portal_headers,
        portal_rows_out,
        report_dir / "portal_og_matrix.html",
        f"Portal Gene Counts x Orthogroups — {family_id}",
    )

    # Decision template
    template_path = report_dir / "og_decisions.txt"
    _write_decision_template(og_ids, template_path)

    typer.echo(f"Reports written to: {report_dir}")
    typer.echo(f"  characterized_og_matrix.tsv / .html")
    typer.echo(f"  portal_og_matrix.tsv / .html")
    typer.echo(f"  og_decisions.txt (edit to select/merge OGs)")

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_og_report",
            "family_id": family_id,
            "run_id": run_id,
            "n_characterized": len(short_names),
            "n_total_ogs": len(og_data),
            "n_ogs_with_char": len(og_ids),
            "report_dir": str(report_dir),
        },
    )
