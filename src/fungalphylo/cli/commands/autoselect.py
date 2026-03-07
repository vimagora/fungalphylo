from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import typer

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.events import log_event
from fungalphylo.core.paths import ProjectPaths
from fungalphylo.db.db import connect

app = typer.Typer(help="Automatically select best proteome/CDS per portal (explainable).")

DEFAULT_BAD_PATTERNS = ("deflines", "promoter", "alleles")
DEFAULT_SCORE_WEIGHTS: Dict[str, float] = {
    "data_group_genome": 50.0,
    "file_format_fasta": 20.0,
    "status_restored": 10.0,
    "status_purged_penalty": -2.0,
    "proteome_label_filtered": 100.0,
    "proteome_label_all": 60.0,
    "proteome_label_generic": 30.0,
    "cds_label_filtered": 100.0,
    "cds_label_all": 60.0,
    "transcript_filtered_fallback": 50.0,
    "transcript_generic_fallback": 20.0,
    "has_date_bonus": 1.0,
    "size_gb_bonus": 1.0,
    "size_bonus_cap": 5.0,
}
LEGACY_WEIGHT_ALIASES = {
    "label_priority": (
        "proteome_label_filtered",
        "proteome_label_all",
        "proteome_label_generic",
        "cds_label_filtered",
        "cds_label_all",
        "transcript_filtered_fallback",
        "transcript_generic_fallback",
    ),
    "status_priority": ("status_restored", "status_purged_penalty"),
    "newer_modified": ("has_date_bonus",),
    "larger_size": ("size_gb_bonus", "size_bonus_cap"),
}


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


def _contains_bad_keyword(filename: str, ban_patterns: Optional[List[str]] = None) -> bool:
    s = (filename or "").lower()
    bad = [p.lower() for p in (ban_patterns or list(DEFAULT_BAD_PATTERNS)) if p]
    return any(b in s for b in bad)


def resolve_autoselect_weights(cfg: Mapping[str, Any]) -> Dict[str, float]:
    raw_weights = cfg.get("autoselect", {}).get("weights", {})
    resolved = dict(DEFAULT_SCORE_WEIGHTS)

    if isinstance(raw_weights, Mapping):
        for key, value in raw_weights.items():
            if key in resolved:
                resolved[key] = float(value)

        for legacy_key, mapped_keys in LEGACY_WEIGHT_ALIASES.items():
            if legacy_key not in raw_weights:
                continue
            legacy_value = float(raw_weights[legacy_key])
            if legacy_key == "status_priority":
                resolved["status_restored"] = legacy_value
                resolved["status_purged_penalty"] = -abs(legacy_value) / 2.5
            elif legacy_key == "larger_size":
                resolved["size_gb_bonus"] = legacy_value
                resolved["size_bonus_cap"] = max(resolved["size_bonus_cap"], legacy_value)
            else:
                for mapped_key in mapped_keys:
                    if mapped_key in DEFAULT_SCORE_WEIGHTS:
                        scale = DEFAULT_SCORE_WEIGHTS[mapped_key] / max(DEFAULT_SCORE_WEIGHTS[mapped_keys[0]], 1.0)
                        resolved[mapped_key] = legacy_value * scale

    return resolved


def resolve_ban_patterns(cfg: Mapping[str, Any]) -> List[str]:
    patterns = cfg.get("autoselect", {}).get("ban_patterns", [])
    if not isinstance(patterns, list):
        return list(DEFAULT_BAD_PATTERNS)
    merged = [p for p in DEFAULT_BAD_PATTERNS]
    merged.extend(str(p) for p in patterns if str(p).strip())
    return merged


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


def score_candidate(
    c: Candidate,
    target: str,
    *,
    weights: Optional[Mapping[str, float]] = None,
    ban_patterns: Optional[List[str]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    target: "proteome" or "cds"
    Returns (score, breakdown dict)
    """
    score_weights = dict(DEFAULT_SCORE_WEIGHTS)
    if weights:
        score_weights.update({k: float(v) for k, v in weights.items()})

    score = 0.0
    why: Dict[str, Any] = {}

    # Hard excludes
    hard_reasons = []
    if c.kind.lower() == "gff":
        hard_reasons.append("kind=gff")
    if _contains_bad_keyword(c.filename, ban_patterns=ban_patterns):
        hard_reasons.append("bad_keyword")
    if hard_reasons:
        return -1e9, {"hard_exclude": "|".join(hard_reasons)}

    # Prefer genome data group
    if c.data_group.lower() == "genome":
        score += score_weights["data_group_genome"]
        why["data_group_genome"] = score_weights["data_group_genome"]

    # Prefer fasta format
    if c.file_format.lower() == "fasta":
        score += score_weights["file_format_fasta"]
        why["file_format_fasta"] = score_weights["file_format_fasta"]

    # Prefer status restored (vs purged)
    # (Keep purged candidates for later restore/download step)
    if c.file_status.upper() == "RESTORED":
        score += score_weights["status_restored"]
        why["status_restored"] = score_weights["status_restored"]
    elif c.file_status.upper() == "PURGED":
        score += score_weights["status_purged_penalty"]
        why["status_purged_penalty"] = score_weights["status_purged_penalty"]

    # Label preference by target
    jat = c.jat_label.lower()
    if target == "proteome":
        if "proteins_filtered" in jat:
            score += score_weights["proteome_label_filtered"]
            why["jat_proteins_filtered"] = score_weights["proteome_label_filtered"]
        elif "proteins_all" in jat:
            score += score_weights["proteome_label_all"]
            why["jat_proteins_all"] = score_weights["proteome_label_all"]
        elif "protein" in jat:
            score += score_weights["proteome_label_generic"]
            why["jat_protein_generic"] = score_weights["proteome_label_generic"]
    else:
        if "cds_filtered" in jat:
            score += score_weights["cds_label_filtered"]
            why["jat_cds_filtered"] = score_weights["cds_label_filtered"]
        elif "cds_all" in jat:
            score += score_weights["cds_label_all"]
            why["jat_cds_all"] = score_weights["cds_label_all"]
        elif "transcripts_filtered" in jat or "transcript_filtered" in jat:
            score += score_weights["transcript_filtered_fallback"]
            why["jat_transcripts_filtered_fallback"] = score_weights["transcript_filtered_fallback"]
        elif "transcript" in jat:
            score += score_weights["transcript_generic_fallback"]
            why["jat_transcript_generic"] = score_weights["transcript_generic_fallback"]

    # Prefer newer files
    ts = c.newest_ts()
    if ts:
        # Convert to an increasing bonus; don't over-weight absolute dates
        # Bonus bucket by recency relative to others is handled by sorting later, but we add a small bump.
        score += score_weights["has_date_bonus"]
        why["has_date_bonus"] = score_weights["has_date_bonus"]

    # Prefer larger (weakly) as a tie-breaker
    if c.size_bytes:
        size_bonus = min(score_weights["size_bonus_cap"], (c.size_bytes / 1e9) * score_weights["size_gb_bonus"])
        score += size_bonus
        why["size_tiebreak"] = round(size_bonus, 4)

    why["final_score"] = score
    return score, why


def top_n_sorted(
    cands: List[Candidate],
    target: str,
    n: int,
    *,
    weights: Optional[Mapping[str, float]] = None,
    ban_patterns: Optional[List[str]] = None,
) -> List[Tuple[Candidate, float, Dict[str, Any]]]:
    scored = []
    for c in cands:
        s, why = score_candidate(c, target, weights=weights, ban_patterns=ban_patterns)
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
    score_weights = resolve_autoselect_weights(cfg)
    ban_patterns = resolve_ban_patterns(cfg)

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
            top_prot = top_n_sorted(cands, "proteome", top_n, weights=score_weights, ban_patterns=ban_patterns)
            top_cds = top_n_sorted(cands, "cds", top_n, weights=score_weights, ban_patterns=ban_patterns)

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
            "ban_patterns": ban_patterns,
            "weights": score_weights,
        },
    )

    typer.echo(f"Wrote selections: {out_sel}")
    typer.echo(f"Wrote explain report: {out_explain}")
