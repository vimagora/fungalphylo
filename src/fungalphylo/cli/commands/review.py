from __future__ import annotations

import csv
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

app = typer.Typer(help="Human-in-the-loop review: export/edit/apply approvals.")


def _read_tsv(path: Path) -> list[dict]:
    path = path.expanduser().resolve()
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        if r.fieldnames is None:
            raise ValueError(f"{path} missing header row")
        return [row for row in r]


@app.command("export")
def export_review(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    from_autoselect: Path = typer.Option(..., "--from-autoselect", help="autoselect_*.tsv file"),
    out: Path | None = typer.Option(None, "--out", help="Output review TSV path"),
) -> None:
    """
    Convert autoselect output into an editable review TSV.
    Users edit proteome_file_id/cds_file_id, then run `review apply`.
    """
    project_dir = project_dir.expanduser().resolve()
    rows = _read_tsv(from_autoselect)
    if not rows:
        raise typer.BadParameter("Autoselect TSV is empty.")

    if out is None:
        review_dir = project_dir / "review"
        review_dir.mkdir(parents=True, exist_ok=True)
        out = review_dir / f"review_edit_{now_tag()}.tsv"

    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([
            "portal_id",
            "proteome_file_id",
            "cds_file_id",
            "approve",      # yes/no
            "note",
        ])
        for r in rows:
            pid = (r.get("portal_id") or "").strip()
            prot = (r.get("proteome_file_id") or "").strip()
            cds = (r.get("cds_or_transcript_file_id") or "").strip()
            note = (r.get("note") or "").strip()
            w.writerow([pid, prot, cds, "yes", note])

    typer.echo(f"Wrote review TSV: {out}")


@app.command("apply")
def apply_review(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    review_tsv: Path = typer.Argument(..., help="Edited review TSV from `review export`"),
    published_only: bool = typer.Option(True, "--published-only/--all", help="Default: enforce published portals only"),
) -> None:
    """
    Apply an edited review TSV into approvals table.
    """
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    rows = _read_tsv(review_tsv)
    if not rows:
        raise typer.BadParameter("Review TSV is empty.")

    conn = connect(paths.db_path)
    applied = 0
    skipped = 0
    try:
        for r in rows:
            pid = (r.get("portal_id") or "").strip()
            if not pid:
                continue

            approve = (r.get("approve") or "yes").strip().lower()
            if approve in {"no", "n", "false", "0"}:
                skipped += 1
                continue

            # enforce published-only if desired
            if published_only:
                pr = conn.execute("SELECT is_published FROM portals WHERE portal_id=?", (pid,)).fetchone()
                if pr is None:
                    raise typer.BadParameter(f"Unknown portal_id in TSV: {pid}")
                if int(pr["is_published"]) != 1:
                    skipped += 1
                    continue

            prot = (r.get("proteome_file_id") or "").strip()
            cds = (r.get("cds_file_id") or "").strip() or None
            note = (r.get("note") or "").strip() or None

            if not prot:
                raise typer.BadParameter(f"{pid}: proteome_file_id is required when approve=yes")

            # Validate file IDs exist and belong to portal
            x = conn.execute("SELECT portal_id FROM portal_files WHERE file_id=?", (prot,)).fetchone()
            if x is None or x["portal_id"] != pid:
                raise typer.BadParameter(f"{pid}: invalid proteome_file_id {prot} (not found or wrong portal)")

            if cds is not None:
                y = conn.execute("SELECT portal_id FROM portal_files WHERE file_id=?", (cds,)).fetchone()
                if y is None or y["portal_id"] != pid:
                    raise typer.BadParameter(f"{pid}: invalid cds_file_id {cds} (not found or wrong portal)")

            conn.execute(
                """
                INSERT INTO approvals(portal_id, proteome_file_id, cds_file_id, approved_at, note)
                VALUES(?,?,?,?,?)
                ON CONFLICT(portal_id) DO UPDATE SET
                  proteome_file_id=excluded.proteome_file_id,
                  cds_file_id=excluded.cds_file_id,
                  approved_at=excluded.approved_at,
                  note=excluded.note
                """,
                (pid, prot, cds, now_iso(), note),
            )
            applied += 1

        conn.commit()
    finally:
        conn.close()

    log_event(project_dir, {"ts": now_iso(), "event": "review_apply", "rows_applied": applied, "rows_skipped": skipped})
    typer.echo(f"Applied approvals: {applied} (skipped: {skipped})")


@app.command("show")
def show_approvals(
    project_dir: Path = typer.Argument(..., help="Project directory"),
    portal_id: list[str] | None = typer.Option(None, "--portal-id", help="Limit to portals"),
) -> None:
    """
    Show current approvals.
    """
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    conn = connect(paths.db_path)
    try:
        params: list[object] = []
        where = ""
        if portal_id:
            where = f"WHERE a.portal_id IN ({','.join('?' for _ in portal_id)})"
            params.extend(portal_id)

        rows = conn.execute(
            f"""
            SELECT a.portal_id, a.proteome_file_id, a.cds_file_id, a.approved_at, a.note
            FROM approvals a
            {where}
            ORDER BY a.portal_id
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    for r in rows:
        typer.echo(f"{r['portal_id']}\t{r['proteome_file_id']}\t{r['cds_file_id'] or ''}\t{r['approved_at']}\t{r['note'] or ''}")