from __future__ import annotations

import csv
from pathlib import Path

import typer
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from fungalphylo.core.config import load_yaml, resolve_config
from fungalphylo.core.errors import exception_record, log_error_jsonl
from fungalphylo.core.events import log_event
from fungalphylo.core.hash import sha256_file
from fungalphylo.core.idmap import load_id_map, resolve_id_map_file
from fungalphylo.core.ids import new_staging_id, now_iso
from fungalphylo.core.manifest import write_manifest
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.resolve import resolve_raw_path
from fungalphylo.core.stage import (
    artifact_cache_key,
    detect_header_mode,
    find_reusable_artifact,
    insert_staging_file,
    link_or_copy,
    load_token_to_canon_map,
    resolve_default_idmap,
    stage_cds_jgi,
    stage_cds_non_jgi,
    stage_proteome_jgi,
    stage_proteome_non_jgi,
    write_sample_headers,
    write_snapshot_checksums,
)
from fungalphylo.db.db import connect, init_db
from fungalphylo.db.queries import fetch_approvals_with_files

app = typer.Typer(help="Stage approved proteomes/CDS into immutable staging snapshots.")

PORTAL_WIDTH = 18


@app.callback(invoke_without_command=True)
def stage_command(
    ctx: typer.Context,
    project_dir: Path = typer.Argument(None, help="Project directory"),
    portal_id: list[str] | None = typer.Option(None, "--portal-id", help="Stage only specific portal IDs."),
    min_aa: int | None = typer.Option(None, "--min-aa", help="Override staging.min_aa."),
    max_aa: int | None = typer.Option(None, "--max-aa", help="Override staging.max_aa."),
    probe_n: int = typer.Option(25, "--probe-n", help="Headers to probe when detecting JGI header mode."),
    id_map: Path | None = typer.Option(None, "--id-map", help="Mapping for non-JGI portals (dir or TSV)."),
    id_map_cds: Path | None = typer.Option(None, "--id-map-cds", help="Optional CDS mapping for non-JGI portals."),
    internal_stop: str = typer.Option("drop", "--internal-stop", help="Internal stop codon handling: drop (default), warn (keep + count), strip (remove * and keep)."),
    overwrite: bool = typer.Option(False, "--overwrite", help="Force regeneration instead of reusing equivalent artifacts."),
    continue_on_error: bool = typer.Option(True, "--continue-on-error/--fail-fast", help="Continue after portal errors."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preflight only (no writes)."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    if project_dir is None:
        raise typer.BadParameter("PROJECT_DIR is required.")

    if internal_stop not in ("drop", "warn", "strip"):
        raise typer.BadParameter(f"--internal-stop must be drop, warn, or strip. Got: {internal_stop!r}")

    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    base_cfg = load_yaml(paths.config_yaml)
    overrides = {"staging": {}}
    if min_aa is not None:
        overrides["staging"]["min_aa"] = min_aa
    if max_aa is not None:
        overrides["staging"]["max_aa"] = max_aa
    cfg = resolve_config(project_config=base_cfg, cli_overrides=overrides)

    stg_cfg = cfg["staging"]
    raw_layout = stg_cfg["raw_layout"]
    min_len = int(stg_cfg["min_aa"])
    max_len = int(stg_cfg["max_aa"])

    resolved_id_map = resolve_default_idmap(project_dir, cfg, id_map)
    resolved_id_map_cds = resolve_default_idmap(project_dir, cfg, id_map_cds) or resolved_id_map

    errors_log = paths.errors_log

    conn = connect(paths.db_path)
    try:
        approvals = fetch_approvals_with_files(conn, portal_ids=portal_id)
        if not approvals:
            raise typer.BadParameter("No approved portals found. Run `review apply` first.")

        staging_id = new_staging_id()
        snapshot_dir = paths.staging_dir(staging_id)
        proteomes_dir = paths.staging_proteomes_dir(staging_id)
        cds_dir = paths.staging_cds_dir(staging_id)
        reports_dir = paths.staging_reports_dir(staging_id)
        generated_idmaps_dir = paths.staging_generated_idmaps_dir(staging_id)

        if not dry_run:
            for path in (snapshot_dir, proteomes_dir, cds_dir, reports_dir, generated_idmaps_dir):
                path.mkdir(parents=True, exist_ok=True)

        actions: list[dict] = []
        failures: list[dict] = []
        staging_rows: list[dict] = []

        with Progress(
            TextColumn("Portal:"),
            TextColumn("{task.fields[p]:<18}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Staging", total=len(approvals), p="-" * PORTAL_WIDTH)

            for a in approvals:
                pid = a["portal_id"]
                progress.update(task, p=(pid[:PORTAL_WIDTH]).ljust(PORTAL_WIDTH))

                try:
                    prot_file_id = a["proteome_file_id"]
                    prot_filename = a["proteome_filename"]
                    raw_prot = resolve_raw_path(
                        project_dir,
                        raw_layout=raw_layout,
                        portal_id=pid,
                        file_id=prot_file_id,
                        filename=prot_filename,
                    )
                    if not raw_prot.exists():
                        raise FileNotFoundError(f"Missing raw proteome: {raw_prot}")

                    prot_mode = detect_header_mode(raw_prot, probe_n=probe_n)
                    prot_raw_sha256 = sha256_file(raw_prot)

                    prot_idmap_path: Path | None = None
                    prot_idmap_sha256: str | None = None
                    if prot_mode != "jgi_pipe":
                        if resolved_id_map is None:
                            if dry_run:
                                typer.echo(f"[dry-run] {pid}: proteome_mode=non_jgi idmap=missing")
                                continue
                            sample = reports_dir / f"sample_headers_{pid}_proteome.txt"
                            write_sample_headers(raw_prot, sample, n=20)
                            raise RuntimeError(f"{pid}: non-JGI headers require idmap. See {sample}")
                        prot_idmap_path = resolve_id_map_file(resolved_id_map, pid, kind="proteome")
                        prot_idmap_sha256 = sha256_file(prot_idmap_path)

                    prot_cache_payload = {
                        "schema_version": 2,
                        "kind": "proteome",
                        "portal_id": pid,
                        "source_file_id": prot_file_id,
                        "raw_sha256": prot_raw_sha256,
                        "min_aa": min_len,
                        "max_aa": max_len,
                        "internal_stop": internal_stop,
                        "probe_n": probe_n,
                        "header_mode": prot_mode,
                        "id_map_sha256": prot_idmap_sha256,
                    }
                    prot_cache_key = artifact_cache_key(prot_cache_payload)
                    prot_reusable = None if overwrite else find_reusable_artifact(conn, kind="proteome", cache_key=prot_cache_key)

                    out_prot = proteomes_dir / f"{pid}.faa"
                    map_out = paths.staging_generated_protein_id_map(staging_id, pid)
                    model_or_token_to_canon: dict[str, str] = {}
                    prot_action = "staged"
                    prot_params = {
                        "mode": prot_mode,
                        "min_aa": min_len,
                        "max_aa": max_len,
                        "id_map": str(prot_idmap_path) if prot_idmap_path else None,
                    }

                    if dry_run:
                        reuse_state = "reuse" if prot_reusable else "generate"
                        typer.echo(f"[dry-run] {pid}: proteome_mode={prot_mode} proteome_action={reuse_state}")
                        continue

                    if prot_reusable:
                        prev_prot = project_dir / prot_reusable["artifact_path"]
                        prev_map = paths.staging_generated_protein_id_map(prot_reusable["staging_id"], pid)
                        if prev_prot.exists() and prev_map.exists():
                            link_or_copy(prev_prot, out_prot)
                            link_or_copy(prev_map, map_out)
                            model_or_token_to_canon = load_token_to_canon_map(map_out)
                            prot_action = "reused"
                        else:
                            prot_reusable = None

                    if prot_reusable is None:
                        with map_out.open("w", encoding="utf-8", newline="") as mf:
                            mw = csv.writer(mf, delimiter="\t")
                            mw.writerow(["canonical_protein_id", "original_header", "length_aa", "model_id_or_token"])

                            if prot_mode == "jgi_pipe":
                                prot_stats, model_or_token_to_canon = stage_proteome_jgi(
                                    in_path=raw_prot,
                                    out_path=out_prot,
                                    portal_id=pid,
                                    min_len=min_len,
                                    max_len=max_len,
                                    internal_stop=internal_stop,
                                    map_writer=mw,
                                )
                            else:
                                pmap = load_id_map(resolved_id_map, pid, kind="proteome")
                                prot_stats, model_or_token_to_canon = stage_proteome_non_jgi(
                                    in_path=raw_prot,
                                    out_path=out_prot,
                                    portal_id=pid,
                                    min_len=min_len,
                                    max_len=max_len,
                                    internal_stop=internal_stop,
                                    idmap=pmap,
                                    map_writer=mw,
                                )
                                missing = prot_stats.get("dropped_missing_in_idmap", 0)
                                if missing:
                                    actions.append(
                                        {
                                            "portal_id": pid,
                                            "kind": "proteome",
                                            "action": "warn",
                                            "reason": f"dropped_missing_in_idmap={missing}",
                                        }
                                    )

                            n_internal_stop = prot_stats.get("internal_stop", 0)
                            if n_internal_stop:
                                action = "dropped" if internal_stop == "drop" else internal_stop
                                actions.append(
                                    {
                                        "portal_id": pid,
                                        "kind": "proteome",
                                        "action": "warn",
                                        "reason": f"internal_stop_codons={n_internal_stop} ({action})",
                                    }
                                )

                    prot_artifact_sha256 = sha256_file(out_prot)
                    insert_staging_file(
                        staging_rows,
                        staging_id=staging_id,
                        portal_id=pid,
                        kind="proteome",
                        source_file_id=prot_file_id,
                        raw_sha256=prot_raw_sha256,
                        artifact_path=str(out_prot.relative_to(project_dir)),
                        artifact_sha256=prot_artifact_sha256,
                        artifact_cache_key=prot_cache_key,
                        reused_from_staging_id=(prot_reusable["staging_id"] if prot_reusable else None),
                        params=prot_params,
                    )
                    actions.append(
                        {
                            "portal_id": pid,
                            "kind": "proteome",
                            "action": prot_action,
                            "file_id": prot_file_id,
                            "out": str(out_prot.relative_to(project_dir)),
                        }
                    )

                    if a["cds_file_id"] and a["cds_filename"]:
                        cds_file_id = a["cds_file_id"]
                        cds_filename = a["cds_filename"]
                        raw_cds = resolve_raw_path(
                            project_dir,
                            raw_layout=raw_layout,
                            portal_id=pid,
                            file_id=cds_file_id,
                            filename=cds_filename,
                        )
                        if not raw_cds.exists():
                            raise FileNotFoundError(f"Missing raw CDS/transcript: {raw_cds}")

                        cds_raw_sha256 = sha256_file(raw_cds)
                        cds_idmap_path: Path | None = None
                        cds_idmap_sha256: str | None = None
                        cds_map_obj = None

                        if prot_mode != "jgi_pipe" and resolved_id_map_cds is not None:
                            try:
                                cds_idmap_path = resolve_id_map_file(resolved_id_map_cds, pid, kind="cds")
                                cds_idmap_sha256 = sha256_file(cds_idmap_path)
                                cds_map_obj = load_id_map(resolved_id_map_cds, pid, kind="cds")
                            except Exception:
                                cds_idmap_path = None
                                cds_idmap_sha256 = None
                                cds_map_obj = None

                        cds_cache_payload = {
                            "schema_version": 1,
                            "kind": "cds",
                            "portal_id": pid,
                            "source_file_id": cds_file_id,
                            "raw_sha256": cds_raw_sha256,
                            "proteome_cache_key": prot_cache_key,
                            "proteome_artifact_sha256": prot_artifact_sha256,
                            "header_mode": prot_mode,
                            "id_map_cds_sha256": cds_idmap_sha256,
                        }
                        cds_cache_key = artifact_cache_key(cds_cache_payload)
                        cds_reusable = None if overwrite else find_reusable_artifact(conn, kind="cds", cache_key=cds_cache_key)

                        out_cds = cds_dir / f"{pid}.fna"
                        cds_mode = "jgi_pipe(model->protein)" if prot_mode == "jgi_pipe" else "non_jgi(header/token->canon)"

                        if cds_reusable:
                            prev_cds = project_dir / cds_reusable["artifact_path"]
                            if prev_cds.exists():
                                link_or_copy(prev_cds, out_cds)
                                cds_action = "reused"
                            else:
                                cds_reusable = None
                                cds_action = "staged"
                        else:
                            cds_action = "staged"

                        if cds_reusable is None:
                            if prot_mode == "jgi_pipe":
                                _ = stage_cds_jgi(
                                    in_path=raw_cds,
                                    out_path=out_cds,
                                    portal_id=pid,
                                    model_to_canon=model_or_token_to_canon,
                                )
                            else:
                                _ = stage_cds_non_jgi(
                                    in_path=raw_cds,
                                    out_path=out_cds,
                                    token_to_canon=model_or_token_to_canon,
                                    idmap_cds=cds_map_obj,
                                )

                        cds_artifact_sha256 = sha256_file(out_cds)
                        insert_staging_file(
                            staging_rows,
                            staging_id=staging_id,
                            portal_id=pid,
                            kind="cds",
                            source_file_id=cds_file_id,
                            raw_sha256=cds_raw_sha256,
                            artifact_path=str(out_cds.relative_to(project_dir)),
                            artifact_sha256=cds_artifact_sha256,
                            artifact_cache_key=cds_cache_key,
                            reused_from_staging_id=(cds_reusable["staging_id"] if cds_reusable else None),
                            params={"mode": cds_mode, "id_map_cds": str(cds_idmap_path) if cds_idmap_path else None},
                        )
                        actions.append(
                            {
                                "portal_id": pid,
                                "kind": "cds",
                                "action": cds_action,
                                "file_id": cds_file_id,
                                "out": str(out_cds.relative_to(project_dir)),
                            }
                        )

                except Exception as e:
                    log_error_jsonl(errors_log, {"event": "stage_error", "portal_id": pid, **exception_record(e)})
                    failures.append({"portal_id": pid, "reason": f"{type(e).__name__}: {e}"})
                    if not continue_on_error:
                        raise
                finally:
                    progress.advance(task)

        if failures:
            failed_report = reports_dir / "failed_portals.tsv"
            with failed_report.open("w", encoding="utf-8", newline="") as f:
                w = csv.writer(f, delimiter="\t")
                w.writerow(["portal_id", "reason"])
                for r in failures:
                    w.writerow([r["portal_id"], r["reason"]])

        if dry_run:
            log_event(
                project_dir,
                {
                    "ts": now_iso(),
                    "event": "stage",
                    "staging_id": staging_id,
                    "dry_run": True,
                    "n_actions": len(actions),
                    "n_failures": len(failures),
                },
            )
            typer.echo("Dry-run complete (no snapshot written).")
            return

        manifest = {
            "staging_id": staging_id,
            "created_at": now_iso(),
            "thresholds": {"min_aa": min_len, "max_aa": max_len},
            "raw_layout": raw_layout,
            "probe_n": probe_n,
            "id_map": str(resolved_id_map) if resolved_id_map else None,
            "id_map_cds": str(resolved_id_map_cds) if resolved_id_map_cds else None,
            "overwrite": overwrite,
            "actions": actions,
            "failures": failures,
            "outputs": {
                "proteomes_dir": str(proteomes_dir.relative_to(project_dir)),
                "cds_dir": str(cds_dir.relative_to(project_dir)),
                "idmaps_generated_dir": str(generated_idmaps_dir.relative_to(project_dir)),
                "reports_dir": str(reports_dir.relative_to(project_dir)),
            },
        }
        write_manifest(paths.staging_manifest(staging_id), manifest)
        write_snapshot_checksums(snapshot_dir, project_dir, paths.staging_checksums(staging_id))

        if not dry_run:
            manifest_rel = str(paths.staging_manifest(staging_id).relative_to(project_dir))
            manifest_sha256 = sha256_file(paths.staging_manifest(staging_id))
            conn.execute(
                """
                INSERT INTO stagings(staging_id, created_at, manifest_path, manifest_sha256)
                VALUES(?,?,?,?)
                """,
                (staging_id, now_iso(), manifest_rel, manifest_sha256),
            )
            for row in staging_rows:
                conn.execute(
                    """
                    INSERT INTO staging_files(
                      staging_id, portal_id, kind, source_file_id, raw_sha256,
                      artifact_path, artifact_sha256, artifact_cache_key,
                      reused_from_staging_id, created_at, params_json
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["staging_id"],
                        row["portal_id"],
                        row["kind"],
                        row["source_file_id"],
                        row["raw_sha256"],
                        row["artifact_path"],
                        row["artifact_sha256"],
                        row["artifact_cache_key"],
                        row["reused_from_staging_id"],
                        row["created_at"],
                        row["params_json"],
                    ),
                )
            conn.commit()

        log_event(
            project_dir,
            {"ts": now_iso(), "event": "stage", "staging_id": staging_id, "n_actions": len(actions), "n_failures": len(failures)},
        )

        typer.echo(f"Stage run recorded: {staging_id}")
        typer.echo(f"Manifest: {paths.staging_manifest(staging_id)}")
        typer.echo(f"Proteomes: {proteomes_dir}")
        if failures:
            typer.echo(f"Failures: {len(failures)} (see staging/{staging_id}/reports)")
    finally:
        conn.close()
