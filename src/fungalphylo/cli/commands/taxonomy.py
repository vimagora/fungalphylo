from __future__ import annotations

import csv
import html
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import requests
import typer

from fungalphylo.core.busco import parse_batch_summary, resolve_batch_summary, resolve_portal_id
from fungalphylo.core.manifest import read_manifest
from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.tabular import read_table
from fungalphylo.db.db import connect, init_db

app = typer.Typer(help="Update portal taxonomy metadata such as NCBI taxon IDs.")
NCBI_NEW_TAXDUMP_URL = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz"
TAXONOMY_RANKS = ["phylum", "class", "order", "family", "genus", "species"]
SUMMARY_RANKS = {"family", "order"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _pick_col(fieldnames: list[str], *candidates: str) -> Optional[str]:
    lower = {c.lower(): c for c in fieldnames}
    for cand in candidates:
        hit = lower.get(cand.lower())
        if hit is not None:
            return hit
    return None


def _normalized_rows(table_path: Path) -> tuple[str | None, Iterator[dict[str, str]], str, Optional[str]]:
    meta, rows = read_table(table_path)
    portal_col = _pick_col(meta.fieldnames, "portal_id", "portal", "portalid")
    taxon_col = _pick_col(meta.fieldnames, "ncbi_taxon_id", "taxon_id", "ncbi_taxid", "taxid")
    note_col = _pick_col(meta.fieldnames, "note", "notes")
    if portal_col is None or taxon_col is None:
        raise typer.BadParameter(
            f"TSV must include portal_id and ncbi_taxon_id columns. Found: {meta.fieldnames}"
        )
    return portal_col, rows, taxon_col, note_col


def _safe_extract_tar(archive_path: Path, out_dir: Path) -> None:
    out_dir = out_dir.expanduser().resolve()
    with tarfile.open(archive_path, "r:gz") as tf:
        for member in tf.getmembers():
            target = (out_dir / member.name).resolve()
            if not str(target).startswith(str(out_dir)):
                raise RuntimeError(f"Refusing to extract unsafe tar member: {member.name}")
        try:
            tf.extractall(out_dir, filter="data")
        except TypeError:
            tf.extractall(out_dir)


def _parse_dmp_line(line: str) -> list[str]:
    parts = [part.strip() for part in line.rstrip("\n").split("\t|\t")]
    if parts and parts[-1].endswith("\t|"):
        parts[-1] = parts[-1][:-2].strip()
    return parts


def _load_taxdump_names(names_path: Path) -> dict[int, str]:
    names: dict[int, str] = {}
    with names_path.open("r", encoding="utf-8") as f:
        for line in f:
            parts = _parse_dmp_line(line)
            if len(parts) < 4:
                continue
            tax_id = int(parts[0])
            name_txt = parts[1]
            name_class = parts[3]
            if name_class == "scientific name":
                names[tax_id] = name_txt
    return names


def _load_taxdump_nodes(nodes_path: Path) -> dict[int, tuple[int, str]]:
    nodes: dict[int, tuple[int, str]] = {}
    with nodes_path.open("r", encoding="utf-8") as f:
        for line in f:
            parts = _parse_dmp_line(line)
            if len(parts) < 3:
                continue
            tax_id = int(parts[0])
            parent_id = int(parts[1])
            rank = parts[2]
            nodes[tax_id] = (parent_id, rank)
    return nodes


def _lineage_for_taxid(tax_id: int, nodes: dict[int, tuple[int, str]], names: dict[int, str]) -> dict[str, str]:
    lineage: dict[str, str] = {rank: "" for rank in TAXONOMY_RANKS}
    current = tax_id
    seen: set[int] = set()
    while current not in seen and current in nodes:
        seen.add(current)
        parent_id, rank = nodes[current]
        if rank in lineage and not lineage[rank]:
            lineage[rank] = names.get(current, str(current))
        if parent_id == current:
            break
        current = parent_id
    if not lineage["species"]:
        lineage["species"] = names.get(tax_id, str(tax_id))
    return lineage


def _coerce_pct(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().rstrip("%")
    if not text:
        return 0.0
    return float(text)


def _pick_first(row: dict[str, Any], *candidates: str) -> Any:
    lower = {k.lower(): v for k, v in row.items()}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def _busco_pct(row: dict[str, Any], kind: str) -> float:
    aliases = {
        "single": ("single", "single_pct", "single_copy", "single_copy_pct", "s", "s_pct"),
        "duplicated": ("duplicated", "duplicated_pct", "duplicate_pct", "d", "d_pct"),
        "fragmented": ("fragmented", "fragmented_pct", "f", "f_pct"),
        "missing": ("missing", "missing_pct", "m", "m_pct"),
        "complete": ("complete", "complete_pct", "c", "c_pct"),
    }
    value = _pick_first(row, *aliases[kind])
    return _coerce_pct(value)


def _latest_busco_run_row(paths: ProjectPaths):
    init_db(paths.db_path)
    conn = connect(paths.db_path)
    try:
        return conn.execute(
            """
            SELECT run_id, staging_id, created_at, manifest_path
            FROM runs
            WHERE kind = 'busco'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()


def _find_single_tsv(root: Path) -> Path:
    candidates = sorted(root.rglob("*.tsv"))
    if not candidates:
        raise typer.BadParameter(f"No TSV found under BUSCO results dir: {root}")
    if len(candidates) == 1:
        return candidates[0]
    return sorted(candidates, key=lambda p: p.stat().st_mtime)[-1]


def _read_delimited_rows(path: Path) -> list[dict[str, str]]:
    meta, rows = read_table(path)
    if meta.delimiter not in {None, "\t", ",", ";", "|"}:
        raise typer.BadParameter(f"Unsupported delimiter for {path}: {meta.delimiter}")
    return list(rows)


def _resolve_portal_for_busco_row(row: dict[str, Any]) -> str:
    value = _pick_first(row, "query", "portal_id", "portal", "sample", "label", "name")
    if value is None:
        raise typer.BadParameter("BUSCO TSV must contain a portal identifier column such as query or portal_id.")
    text = str(value).strip()
    if not text:
        raise typer.BadParameter("BUSCO TSV contains an empty portal identifier.")
    return resolve_portal_id(text)


def _load_busco_rows_from_db(paths: ProjectPaths, run_id: str) -> list[dict[str, Any]]:
    conn = connect(paths.db_path)
    try:
        rows = conn.execute(
            """
            SELECT portal_id, input_filename, lineage, complete_pct, single_pct, duplicated_pct,
                   fragmented_pct, missing_pct, n_markers
            FROM busco_results
            WHERE run_id = ?
            ORDER BY input_filename
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "portal_id": row["portal_id"],
            "query": row["input_filename"],
            "complete": row["complete_pct"],
            "single": row["single_pct"],
            "duplicated": row["duplicated_pct"],
            "fragmented": row["fragmented_pct"],
            "missing": row["missing_pct"],
            "n_markers": row["n_markers"],
            "dataset": row["lineage"],
        }
        for row in rows
    ]


def _load_busco_rows(paths: ProjectPaths, run_id: str, run_root: Path, busco_tsv: Optional[Path]) -> list[dict[str, Any]]:
    if busco_tsv is not None:
        resolved_busco_tsv = busco_tsv.expanduser().resolve()
        if not resolved_busco_tsv.exists():
            raise typer.BadParameter(f"BUSCO TSV not found: {resolved_busco_tsv}")
        return _read_delimited_rows(resolved_busco_tsv)

    db_rows = _load_busco_rows_from_db(paths, run_id)
    if db_rows:
        return db_rows

    manifest = read_manifest(paths.run_manifest(run_id))
    batch_summary = resolve_batch_summary(paths, run_id, manifest)
    if batch_summary.exists():
        parsed_rows = parse_batch_summary(batch_summary)
        return [
            {
                "portal_id": row["portal_id"],
                "query": row["input_filename"],
                "complete": row["complete_pct"],
                "single": row["single_pct"],
                "duplicated": row["duplicated_pct"],
                "fragmented": row["fragmented_pct"],
                "missing": row["missing_pct"],
                "n_markers": row["n_markers"],
                "dataset": row["lineage"],
            }
            for row in parsed_rows
        ]

    results_dir = run_root / "busco_results"
    resolved_busco_tsv = _find_single_tsv(results_dir)
    if not resolved_busco_tsv.exists():
        raise typer.BadParameter(f"BUSCO TSV not found: {resolved_busco_tsv}")
    return _read_delimited_rows(resolved_busco_tsv)


def _stacked_bar(row: dict[str, Any]) -> tuple[str, dict[str, float]]:
    single = _busco_pct(row, "single")
    duplicated = _busco_pct(row, "duplicated")
    fragmented = _busco_pct(row, "fragmented")
    missing = _busco_pct(row, "missing")
    if single == 0.0 and duplicated == 0.0:
        complete = _busco_pct(row, "complete")
        if complete > 0.0:
            single = complete
    total = single + duplicated + fragmented + missing
    if total <= 0.0:
        raise typer.BadParameter("BUSCO TSV row is missing usable percentage columns (expected C/S/D/F/M-style fields).")
    scale = 100.0 / total
    values = {
        "single": single * scale,
        "duplicated": duplicated * scale,
        "fragmented": fragmented * scale,
        "missing": missing * scale,
    }
    segments = []
    colors = {
        "single": "#4caf50",
        "duplicated": "#8bc34a",
        "fragmented": "#ffb300",
        "missing": "#ef5350",
    }
    for key in ("single", "duplicated", "fragmented", "missing"):
        width = max(values[key], 0.0)
        segments.append(
            f'<span class="seg {key}" style="width:{width:.3f}%;background:{colors[key]};"></span>'
        )
    return "".join(segments), values


def _summary_rows(rows: list[dict[str, Any]], rank: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        label = row["lineage"].get(rank) or f"Unassigned {rank}"
        group = groups.setdefault(
            label,
            {
                "label": label,
                "count": 0,
                "complete": 0.0,
                "single": 0.0,
                "duplicated": 0.0,
                "fragmented": 0.0,
                "missing": 0.0,
            },
        )
        complete = _busco_pct(row["busco"], "complete")
        if complete <= 0.0:
            complete = _busco_pct(row["busco"], "single") + _busco_pct(row["busco"], "duplicated")
        group["count"] += 1
        group["complete"] += complete
        group["single"] += _busco_pct(row["busco"], "single")
        group["duplicated"] += _busco_pct(row["busco"], "duplicated")
        group["fragmented"] += _busco_pct(row["busco"], "fragmented")
        group["missing"] += _busco_pct(row["busco"], "missing")

    out: list[dict[str, Any]] = []
    for group in groups.values():
        count = max(1, int(group["count"]))
        out.append(
            {
                "label": group["label"],
                "count": count,
                "complete": group["complete"] / count,
                "single": group["single"] / count,
                "duplicated": group["duplicated"] / count,
                "fragmented": group["fragmented"] / count,
                "missing": group["missing"] / count,
            }
        )
    return sorted(out, key=lambda item: (item["complete"], item["label"]))


def _render_taxonomy_busco_html(
    title: str,
    rows: list[dict[str, Any]],
    *,
    low_quality_threshold: float,
    summary_rank: str | None,
) -> str:
    body: list[str] = []
    current_groups = {rank: None for rank in TAXONOMY_RANKS[:-1]}
    summary_html = ""
    if summary_rank is not None:
        summary_lines = [
            f"<h2>{html.escape(summary_rank.title())} Summary</h2>",
            '<div class="summary-header"><span>Group</span><span>N</span><span>Mean complete</span><span>Mean S/D/F/M</span></div>',
        ]
        for item in _summary_rows(rows, summary_rank):
            css_class = "summary-row low-quality" if item["complete"] < low_quality_threshold else "summary-row"
            summary_lines.append(
                f'<div class="{css_class}">'
                f"<span>{html.escape(item['label'])}</span>"
                f"<span>{item['count']}</span>"
                f"<span>{item['complete']:.1f}%</span>"
                f"<span>S {item['single']:.1f} | D {item['duplicated']:.1f} | F {item['fragmented']:.1f} | M {item['missing']:.1f}</span>"
                "</div>"
            )
        summary_html = "".join(summary_lines)

    for row in rows:
        lineage = row["lineage"]
        for depth, rank in enumerate(TAXONOMY_RANKS[:-1]):
            value = lineage.get(rank) or f"Unassigned {rank}"
            if current_groups[rank] != value:
                for lower_rank in TAXONOMY_RANKS[depth + 1 : -1]:
                    current_groups[lower_rank] = None
                current_groups[rank] = value
                body.append(
                    f'<div class="group depth-{depth}"><span class="tree-label">{html.escape(value)}</span></div>'
                )

        bar_html, values = _stacked_bar(row["busco"])
        complete = _busco_pct(row["busco"], "complete")
        if complete <= 0.0:
            complete = values["single"] + values["duplicated"]
        species = lineage.get("species") or "Unassigned species"
        portal_name = row["portal_name"] or ""
        css_class = "leaf low-quality" if complete < low_quality_threshold else "leaf"
        body.append(
            f"<div class=\"{css_class}\">"
            f"<span class=\"leaf-label\">{html.escape(row['portal_id'])}</span>"
            f"<span class=\"leaf-name\">{html.escape(portal_name)}</span>"
            f"<span class=\"leaf-species\">{html.escape(species)}</span>"
            f"<span class=\"bar\">{bar_html}</span>"
            f"<span class=\"metrics\">C {complete:.1f}% | S {values['single']:.1f}% | D {values['duplicated']:.1f}% | "
            f"F {values['fragmented']:.1f}% | M {values['missing']:.1f}%</span>"
            "</div>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{
      font-family: Menlo, Consolas, Monaco, monospace;
      margin: 24px;
      background: #f7f4ee;
      color: #1f1a17;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    p.note {{
      margin: 0 0 18px;
      color: #5f544d;
      max-width: 1100px;
    }}
    h2 {{
      margin: 20px 0 10px;
      font-size: 18px;
    }}
    .legend {{
      display: flex;
      gap: 12px;
      margin: 0 0 18px;
      flex-wrap: wrap;
      font-size: 13px;
    }}
    .legend span::before {{
      content: "";
      display: inline-block;
      width: 12px;
      height: 12px;
      margin-right: 6px;
      vertical-align: -2px;
    }}
    .legend .single::before {{ background: #4caf50; }}
    .legend .duplicated::before {{ background: #8bc34a; }}
    .legend .fragmented::before {{ background: #ffb300; }}
    .legend .missing::before {{ background: #ef5350; }}
    .group, .leaf {{
      display: grid;
      grid-template-columns: 280px 220px 260px 280px 1fr;
      gap: 12px;
      align-items: center;
      margin: 4px 0;
    }}
    .group {{
      font-weight: 700;
      color: #5d4037;
      margin-top: 12px;
    }}
    .summary-header, .summary-row {{
      display: grid;
      grid-template-columns: 340px 80px 160px 1fr;
      gap: 12px;
      align-items: center;
      font-size: 13px;
      margin: 4px 0;
      padding: 6px 10px;
      border-radius: 8px;
      background: #efe6dc;
    }}
    .summary-header {{
      font-weight: 700;
      background: #dfd1c3;
    }}
    .depth-0 {{ padding-left: 0; }}
    .depth-1 {{ padding-left: 18px; }}
    .depth-2 {{ padding-left: 36px; }}
    .depth-3 {{ padding-left: 54px; }}
    .depth-4 {{ padding-left: 72px; }}
    .tree-label {{ grid-column: 1 / span 5; }}
    .leaf {{
      padding-left: 90px;
      font-size: 13px;
    }}
    .leaf-label {{ font-weight: 700; }}
    .leaf-name, .leaf-species, .metrics {{ color: #4e453f; }}
    .bar {{
      display: inline-flex;
      width: 280px;
      height: 14px;
      border-radius: 999px;
      overflow: hidden;
      background: #ddd3cc;
      border: 1px solid #c6b8ae;
    }}
    .seg {{ display: inline-block; height: 100%; }}
    .low-quality {{
      background: #fff1ee;
      box-shadow: inset 4px 0 0 #d84315;
    }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p class="note">Taxonomy-ordered BUSCO quality mockup. This is not a phylogeny; branch spacing is not evolutionary. Low-quality rows are highlighted when complete BUSCO is below {low_quality_threshold:.1f}%.</p>
  <div class="legend">
    <span class="single">Single-copy</span>
    <span class="duplicated">Duplicated</span>
    <span class="fragmented">Fragmented</span>
    <span class="missing">Missing</span>
  </div>
  {summary_html}
  {''.join(body)}
</body>
</html>
"""


@app.command("fetch-ncbi")
def fetch_ncbi_taxdump(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    url: str = typer.Option(NCBI_NEW_TAXDUMP_URL, "--url", help="NCBI taxdump archive URL"),
    out_dir: Optional[Path] = typer.Option(None, "--out-dir", help="Target directory for archive and extracted files"),
    extract: bool = typer.Option(True, "--extract/--no-extract", help="Extract the downloaded tar.gz archive"),
    force: bool = typer.Option(False, "--force", help="Redownload and re-extract even if files already exist"),
    timeout: int = typer.Option(300, "--timeout", help="HTTP timeout in seconds"),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)

    target_root = out_dir.expanduser().resolve() if out_dir else (paths.cache_dir / "ncbi_taxonomy")
    target_root.mkdir(parents=True, exist_ok=True)

    archive_name = Path(url).name or "new_taxdump.tar.gz"
    archive_path = target_root / archive_name
    extract_dir_name = archive_name.removesuffix(".tar.gz")
    extracted_dir = target_root / extract_dir_name

    if archive_path.exists() and (not extract or extracted_dir.exists()) and not force:
        typer.echo(f"NCBI taxdump already present: {archive_path}")
        if extract:
            typer.echo(f"Extracted dir: {extracted_dir}")
        return

    tmp_archive = archive_path.with_suffix(archive_path.suffix + f".{_now_tag()}.tmp")
    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with tmp_archive.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        tmp_archive.replace(archive_path)

        if extract:
            if extracted_dir.exists() and force:
                shutil.rmtree(extracted_dir)
            extracted_dir.mkdir(parents=True, exist_ok=True)
            _safe_extract_tar(archive_path, extracted_dir)

        log_event(
            project_dir,
            {
                "ts": _now(),
                "event": "taxonomy_fetch_ncbi",
                "url": url,
                "archive_path": str(archive_path),
                "extracted_dir": str(extracted_dir) if extract else None,
                "extract": extract,
                "force": force,
            },
        )
    except Exception as exc:
        if tmp_archive.exists():
            tmp_archive.unlink()
        log_error_jsonl(
            paths.errors_log,
            {
                "event": "taxonomy_fetch_ncbi_error",
                "url": url,
                "archive_path": str(archive_path),
                **exception_record(exc),
            },
        )
        raise

    typer.echo(f"Downloaded NCBI taxdump: {archive_path}")
    if extract:
        typer.echo(f"Extracted NCBI taxdump: {extracted_dir}")


@app.command("export")
def export_taxonomy(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output TSV path"),
    approved_only: bool = typer.Option(
        False,
        "--approved-only",
        help="Export only portals currently present in approvals.",
    ),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    if out is None:
        review_dir = project_dir / "review"
        review_dir.mkdir(parents=True, exist_ok=True)
        suffix = "approved" if approved_only else "all"
        out = review_dir / f"taxonomy_edit_{suffix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.tsv"
    else:
        out = out.expanduser().resolve()

    out.parent.mkdir(parents=True, exist_ok=True)

    conn = connect(paths.db_path)
    try:
        if approved_only:
            rows = conn.execute(
                """
                SELECT p.portal_id, p.name, p.ncbi_taxon_id
                FROM portals p
                JOIN approvals a ON a.portal_id = p.portal_id
                ORDER BY p.portal_id
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT portal_id, name, ncbi_taxon_id
                FROM portals
                ORDER BY portal_id
                """
            ).fetchall()
    finally:
        conn.close()

    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["portal_id", "name", "ncbi_taxon_id", "note"])
        for row in rows:
            w.writerow(
                [
                    row["portal_id"],
                    row["name"] or "",
                    "" if row["ncbi_taxon_id"] is None else str(row["ncbi_taxon_id"]),
                    "",
                ]
            )

    log_event(
        project_dir,
        {
            "ts": _now(),
            "event": "taxonomy_export",
            "out": str(out),
            "approved_only": approved_only,
            "rows_written": len(rows),
        },
    )
    typer.echo(f"Wrote taxonomy TSV: {out}")


@app.command("apply")
def apply_taxonomy(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    taxonomy_tsv: Path = typer.Argument(..., help="TSV/CSV/XLSX with portal_id and ncbi_taxon_id columns"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate and report changes without writing"),
    allow_clear: bool = typer.Option(
        True,
        "--allow-clear/--no-allow-clear",
        help="Treat blank ncbi_taxon_id values as clearing the stored taxon ID.",
    ),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    portal_col, rows, taxon_col, note_col = _normalized_rows(taxonomy_tsv)

    conn = connect(paths.db_path)
    updated = 0
    cleared = 0
    unchanged = 0
    unknown = 0
    invalid = 0

    try:
        for row in rows:
            portal_id = (row.get(portal_col) or "").strip()
            if not portal_id:
                continue

            existing = conn.execute(
                "SELECT ncbi_taxon_id FROM portals WHERE portal_id = ?",
                (portal_id,),
            ).fetchone()
            if existing is None:
                unknown += 1
                raise typer.BadParameter(f"Unknown portal_id in taxonomy table: {portal_id}")

            raw_taxon = (row.get(taxon_col) or "").strip()
            note = (row.get(note_col) or "").strip() if note_col else ""
            if raw_taxon == "":
                if not allow_clear:
                    unchanged += 1
                    continue
                new_taxon_id = None
            else:
                try:
                    new_taxon_id = int(raw_taxon)
                except ValueError as exc:
                    invalid += 1
                    raise typer.BadParameter(f"{portal_id}: invalid ncbi_taxon_id {raw_taxon!r}") from exc
                if new_taxon_id <= 0:
                    invalid += 1
                    raise typer.BadParameter(f"{portal_id}: ncbi_taxon_id must be a positive integer")

            if existing["ncbi_taxon_id"] == new_taxon_id:
                unchanged += 1
                continue

            if new_taxon_id is None:
                cleared += 1
            else:
                updated += 1

            if not dry_run:
                conn.execute(
                    "UPDATE portals SET ncbi_taxon_id = ? WHERE portal_id = ?",
                    (new_taxon_id, portal_id),
                )

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    log_event(
        project_dir,
        {
            "ts": _now(),
            "event": "taxonomy_apply",
            "table": str(taxonomy_tsv),
            "dry_run": dry_run,
            "updated": updated,
            "cleared": cleared,
            "unchanged": unchanged,
            "unknown": unknown,
            "invalid": invalid,
        },
    )
    mode = "Dry-run complete" if dry_run else "Applied taxonomy updates"
    typer.echo(
        f"{mode}: updated={updated} cleared={cleared} unchanged={unchanged} unknown={unknown} invalid={invalid}"
    )


@app.command("busco-mockup")
def busco_taxonomy_mockup(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    busco_tsv: Optional[Path] = typer.Option(None, "--busco-tsv", help="Override BUSCO summary TSV path"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="BUSCO run ID to use (default: latest BUSCO run)"),
    taxdump_dir: Optional[Path] = typer.Option(
        None, "--taxdump-dir", help="Directory containing names.dmp and nodes.dmp (default: cache/ncbi_taxonomy/new_taxdump)"
    ),
    summary_rank: Optional[str] = typer.Option(
        None, "--summary-rank", help="Optional taxonomy summary level: family or order"
    ),
    low_quality_threshold: float = typer.Option(
        85.0, "--low-quality-threshold", help="Highlight taxa below this complete BUSCO percentage"
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output HTML path"),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    if run_id is None:
        run_row = _latest_busco_run_row(paths)
        if run_row is None:
            raise typer.BadParameter("No BUSCO run found. Generate a BUSCO run first or pass --run-id.")
        selected_run_id = run_row["run_id"]
    else:
        selected_run_id = run_id

    run_root = paths.run_dir(selected_run_id)
    results_dir = run_root / "busco_results"
    if not results_dir.exists():
        raise typer.BadParameter(f"BUSCO results dir not found for run {selected_run_id}: {results_dir}")

    resolved_taxdump_dir = (
        taxdump_dir.expanduser().resolve() if taxdump_dir else paths.cache_dir / "ncbi_taxonomy" / "new_taxdump"
    )
    names_path = resolved_taxdump_dir / "names.dmp"
    nodes_path = resolved_taxdump_dir / "nodes.dmp"
    if not names_path.exists() or not nodes_path.exists():
        raise typer.BadParameter(
            f"Taxdump files not found under {resolved_taxdump_dir}. Run `fungalphylo taxonomy fetch-ncbi` first."
        )
    if summary_rank is not None and summary_rank not in SUMMARY_RANKS:
        raise typer.BadParameter(f"--summary-rank must be one of: {', '.join(sorted(SUMMARY_RANKS))}")

    if out is None:
        reports_dir = run_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        out = reports_dir / "taxonomy_busco_mockup.html"
    else:
        out = out.expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)

    busco_rows = _load_busco_rows(paths, selected_run_id, run_root, busco_tsv)
    if not busco_rows:
        raise typer.BadParameter(f"BUSCO summary is empty for run: {selected_run_id}")

    names = _load_taxdump_names(names_path)
    nodes = _load_taxdump_nodes(nodes_path)

    conn = connect(paths.db_path)
    try:
        portal_rows = conn.execute(
            """
            SELECT portal_id, name, ncbi_taxon_id
            FROM portals
            """
        ).fetchall()
    finally:
        conn.close()

    portals = {
        row["portal_id"]: {
            "portal_name": row["name"] or "",
            "ncbi_taxon_id": row["ncbi_taxon_id"],
        }
        for row in portal_rows
    }

    combined_rows: list[dict[str, Any]] = []
    for row in busco_rows:
        portal_id = _resolve_portal_for_busco_row(row)
        portal_info = portals.get(portal_id, {"portal_name": "", "ncbi_taxon_id": None})
        tax_id = portal_info["ncbi_taxon_id"]
        lineage = {rank: "" for rank in TAXONOMY_RANKS}
        if tax_id is not None and int(tax_id) in nodes:
            lineage = _lineage_for_taxid(int(tax_id), nodes, names)
        combined_rows.append(
            {
                "portal_id": portal_id,
                "portal_name": portal_info["portal_name"],
                "tax_id": tax_id,
                "lineage": lineage,
                "busco": row,
            }
        )

    combined_rows.sort(
        key=lambda item: tuple((item["lineage"].get(rank) or f"zzz_{rank}") for rank in TAXONOMY_RANKS)
        + (item["portal_id"],)
    )
    title = f"Taxonomy-Ordered BUSCO Mockup ({selected_run_id})"
    out.write_text(
        _render_taxonomy_busco_html(
            title,
            combined_rows,
            low_quality_threshold=low_quality_threshold,
            summary_rank=summary_rank,
        ),
        encoding="utf-8",
    )

    log_event(
        project_dir,
        {
            "ts": _now(),
            "event": "taxonomy_busco_mockup",
            "run_id": selected_run_id,
            "busco_source": ("explicit_tsv" if busco_tsv is not None else "run_summary"),
            "taxdump_dir": str(resolved_taxdump_dir),
            "out": str(out),
            "n_rows": len(combined_rows),
        },
    )
    typer.echo(f"Wrote taxonomy BUSCO mockup: {out}")
