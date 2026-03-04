from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import typer

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

app = typer.Typer(help="Automatically select best proteome/CDS per portal (explainable).")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    try:
        # Expect ISO-ish; tolerate trailing Z
        txt = str(s).replace("Z", "+00:00")
        return datetime.fromisoformat(txt)
    except Exception:
        return None


def _meta(row) -> Dict[str, Any]:
    try:
        return json.loads(row["meta_json"]) if row["meta_json"] else {}
    except Exception:
        return {}


def _contains_bad_keyword(filename: str) -> bool:
    s = (filename or "").lower()
    bad = ["deflines", "promoter", "alleles"]
    return any(b in s for b in bad)


@dataclass
class Candidate:
    file_id: str
    portal_id: str
    kind: str
    filename: str
    size_bytes: Optional[int]
    md5: Optional[str]
    meta: Dict[str, Any]

    # derived
    jat_label: str
    file_format: str
    data_group: str
    modified_date: Optional[datetime]
    file_date: Optional[datetime]
    file_status: str

    def newest_ts(self) -> Optional[datetime]:
        return self.modified_date or self.file_date


def row_to_candidate(r) -> Candidate:
    meta = _meta(r)
    return Candidate(
        file_id=str(r["file_id"]),
        portal_id=r["portal_id"],
        kind=r["kind"],
        filename=r["filename"],
        size_bytes=r["size_bytes"],
        md5=r["md5"],
        meta=meta,
        jat_label=str(meta.get("jat_label") or ""),
        file_format=str(meta.get("file_format") or ""),
        data_group=str(meta.get("data_group") or ""),
        modified_date=_parse_dt(meta.get("modified_date")),
        file_date=_parse_dt(meta.get("file_date")),
        file_status=str(meta.get("file_status") or ""),
    )


def score_candidate(c: Candidate, target: str) -> Tuple[float, Dict[str, Any]]:
    """
    target: "proteome" or "cds"
    Returns (score, breakdown dict)
    """
    score = 0.0
    why: Dict[str, Any] = {}

    # Hard excludes
    hard_reasons = []
    if c.kind.lower() == "gff":
        hard_reasons.append("kind=gff")
    if _contains_bad_keyword(c.filename):
        hard_reasons.append("bad_keyword")
    if hard_reasons:
        return -1e9, {"hard_exclude": "|".join(hard_reasons)}

    # Prefer genome data group
    if c.data_group.lower() == "genome":
        score += 50
        why["data_group_genome"] = 50

    # Prefer fasta format
    if c.file_format.lower() == "fasta":
        score += 20
        why["file_format_fasta"] = 20

    # Prefer status restored (vs purged)
    # (Keep purged candidates for later restore/download step)
    if c.file_status.upper() == "RESTORED":
        score += 10
        why["status_restored"] = 10
    elif c.file_status.upper() == "PURGED":
        score -= 2
        why["status_purged_penalty"] = -2

    # Label preference by target
    jat = c.jat_label.lower()
    if target == "proteome":
        if "proteins_filtered" in jat:
            score += 100
            why["jat_proteins_filtered"] = 100
        elif "proteins_all" in jat:
            score += 60
            why["jat_proteins_all"] = 60
        elif "protein" in jat:
            score += 30
            why["jat_protein_generic"] = 30
    else:
        if "cds_filtered" in jat:
            score += 100
            why["jat_cds_filtered"] = 100
        elif "cds_all" in jat:
            score += 60
            why["jat_cds_all"] = 60
        elif "transcripts_filtered" in jat or "transcript_filtered" in jat:
            score += 50
            why["jat_transcripts_filtered_fallback"] = 50
        elif "transcript" in jat:
            score += 20
            why["jat_transcript_generic"] = 20

    # Prefer newer files
    ts = c.newest_ts()
    if ts:
        # Convert to an increasing bonus; don't over-weight absolute dates
        # Bonus bucket by recency relative to others is handled by sorting later, but we add a small bump.
        score += 1
        why["has_date_bonus"] = 1

    # Prefer larger (weakly) as a tie-breaker
    if c.size_bytes:
        score += min(5.0, c.size_bytes / 1e9)  # cap at +5
        why["size_tiebreak"] = round(min(5.0, c.size_bytes / 1e9), 4)

    why["final_score"] = score
    return score, why


def top_n_sorted(cands: List[Candidate], target: str, n: int) -> List[Tuple[Candidate, float, Dict[str, Any]]]:
    scored = []
    for c in cands:
        s, why = score_candidate(c, target)
        scored.append((c, s, why))

    # sort: score desc, date desc, size desc, filename asc
    def key(item):
        c, s, _ = item
        dt = c.newest_ts()
        dt_val = dt.timestamp() if dt else 0.0
        size = c.size_bytes or 0
        return (s, dt_val, size, c.filename)

    scored.sort(key=key, reverse=True)
    return scored[:n]


@app.callback(invoke_without_command=True)
def autoselect_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    portal_id: Optional[List[str]] = typer.Option(None, "--portal-id", help="Limit to specific portal IDs"),
    published_only: bool = typer.Option(True, "--published-only/--all", help="Default: only published portals"),
    top_n: int = typer.Option(5, "--top-n", help="Top N candidates to include in explain report"),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)

    cfg = resolve_config(project_config=load_yaml(paths.config_yaml))

    review_dir = project_dir / "review"
    review_dir.mkdir(parents=True, exist_ok=True)

    tag = _now_tag()
    out_sel = review_dir / f"autoselect_{tag}.tsv"
    out_explain = review_dir / f"autoselect_explain_{tag}.tsv"

    conn = connect(paths.db_path)
    try:
        # portal list
        params: List[object] = []
        where = []
        if portal_id:
            where.append(f"portal_id IN ({','.join('?' for _ in portal_id)})")
            params.extend(portal_id)
        if published_only:
            where.append("is_published = 1")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        portals = conn.execute(f"SELECT portal_id, name, is_published FROM portals {where_sql} ORDER BY portal_id", params).fetchall()
        if not portals:
            raise typer.BadParameter("No portals match filters (published_only/portal_id).")

        # fetch candidates for all portals in one query
        portal_ids = [p["portal_id"] for p in portals]
        placeholders = ",".join("?" for _ in portal_ids)
        rows = conn.execute(
            f"""
            SELECT file_id, portal_id, kind, filename, size_bytes, md5, meta_json
            FROM portal_files
            WHERE portal_id IN ({placeholders})
            """,
            portal_ids,
        ).fetchall()

    finally:
        conn.close()

    by_portal: Dict[str, List[Candidate]] = {pid: [] for pid in portal_ids}
    for r in rows:
        c = row_to_candidate(r)
        by_portal[c.portal_id].append(c)

    # Write explain report
    with out_explain.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([
            "portal_id", "target", "rank",
            "file_id", "kind", "filename",
            "jat_label", "file_format", "data_group", "file_status",
            "modified_date", "file_date", "size_bytes",
            "score", "explain_json"
        ])

        selections: List[Tuple[str, str, Optional[str], Optional[str]]] = []

        for pid in portal_ids:
            cands = by_portal.get(pid, [])
            # filter out obvious wrong-kinds early, but keep "other" because some datasets mislabel kind
            # (scoring handles gff exclusion)
            top_prot = top_n_sorted(cands, "proteome", top_n)
            top_cds = top_n_sorted(cands, "cds", top_n)

            # Choose top scoring candidate with score > -1e8 (not hard excluded)
            best_prot = next((t for t in top_prot if t[1] > -1e8), None)
            best_cds = next((t for t in top_cds if t[1] > -1e8), None)

            prot_id = best_prot[0].file_id if best_prot else None
            cds_id = best_cds[0].file_id if best_cds else None

            selections.append((pid, "selected", prot_id, cds_id))

            for target, top in [("proteome", top_prot), ("cds", top_cds)]:
                for rank, (c, s, why) in enumerate(top, start=1):
                    w.writerow([
                        pid, target, rank,
                        c.file_id, c.kind, c.filename,
                        c.jat_label, c.file_format, c.data_group, c.file_status,
                        c.modified_date.isoformat() if c.modified_date else "",
                        c.file_date.isoformat() if c.file_date else "",
                        c.size_bytes if c.size_bytes is not None else "",
                        f"{s:.6f}",
                        json.dumps(why, ensure_ascii=False, sort_keys=True),
                    ])

    # Write selection summary (review surface)
    with out_sel.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([
            "portal_id",
            "proteome_file_id",
            "cds_or_transcript_file_id",
            "note",
        ])
        for pid, _, prot_id, cds_id in selections:
            note = ""
            if prot_id is None:
                note = "NO_PROTEOME_CANDIDATE"
            if cds_id is None:
                note = (note + "; " if note else "") + "NO_CDS_OR_TRANSCRIPT_CANDIDATE"
            w.writerow([pid, prot_id or "", cds_id or "", note])

    log_event(
        project_dir,
        {
            "ts": _now_iso(),
            "event": "autoselect",
            "published_only": published_only,
            "n_portals": len(portal_ids),
            "out_selection_tsv": str(out_sel),
            "out_explain_tsv": str(out_explain),
            "top_n": top_n,
        },
    )

    typer.echo(f"Wrote selections: {out_sel}")
    typer.echo(f"Wrote explain report: {out_explain}")