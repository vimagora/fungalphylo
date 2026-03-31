from __future__ import annotations

import json
import shutil
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.db.db import init_db


def _find_of_results_dir(results_root: Path) -> Path:
    candidates = sorted(results_root.glob("Results_*"), reverse=True)
    for c in candidates:
        og_seq = c / "Orthogroup_Sequences"
        if og_seq.is_dir():
            return c
    raise typer.BadParameter(
        f"No OrthoFinder Results_* with Orthogroup_Sequences/ found in {results_root}"
    )


def _parse_decisions(template_path: Path) -> tuple[list[str], list[list[str]]]:
    """Parse the decision template.

    Returns (include_list, merge_groups) where merge_groups is a list of lists.
    """
    include_ids: list[str] = []
    merge_groups: list[list[str]] = []

    text = template_path.read_text(encoding="utf-8")
    in_include = False
    in_merge = False

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            in_include = False
            in_merge = False
            if stripped.startswith("#"):
                continue
            # blank line resets section only if we haven't seen content yet
            continue

        if stripped.lower().startswith("include:"):
            in_include = True
            in_merge = False
            rest = stripped[len("include:"):].strip()
            if rest:
                include_ids.extend(og.strip() for og in rest.split(",") if og.strip())
            continue

        if stripped.lower().startswith("merge:"):
            in_merge = True
            in_include = False
            rest = stripped[len("merge:"):].strip()
            if rest:
                for group in rest.split(";"):
                    ogs = [og.strip() for og in group.split(",") if og.strip()]
                    if ogs:
                        merge_groups.append(ogs)
            continue

        # Continuation lines
        if in_include:
            include_ids.extend(og.strip() for og in stripped.split(",") if og.strip())
        elif in_merge:
            for group in stripped.split(";"):
                ogs = [og.strip() for og in group.split(",") if og.strip()]
                if ogs:
                    merge_groups.append(ogs)

    return include_ids, merge_groups


def og_apply_command(
    project_dir: Path = typer.Argument(..., help="Project directory."),
    family_id: str = typer.Option(..., "--family-id", help="Gene family identifier."),
    run_id: str | None = typer.Option(
        None, "--run-id",
        help="OrthoFinder run ID (default: auto-detect latest).",
    ),
    results_dir: Path | None = typer.Option(
        None, "--results-dir",
        help="Explicit path to OrthoFinder results root.",
    ),
    decisions: Path | None = typer.Option(
        None, "--decisions",
        help="Path to edited decision template (default: og_report/og_decisions.txt).",
    ),
    no_placed: bool = typer.Option(
        False, "--no-placed",
        help="Ignore og_placed/ and read from Orthogroup_Sequences/ directly.",
    ),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    # Find decision template
    if decisions is not None:
        template_path = decisions.expanduser().resolve()
    else:
        template_path = paths.family_og_report_dir(family_id) / "og_decisions.txt"
    if not template_path.is_file():
        raise typer.BadParameter(
            f"Decision template not found: {template_path}\n"
            f"Run `protsetphylo og-report --family-id {family_id}` first, then edit og_decisions.txt."
        )

    include_ids, merge_groups = _parse_decisions(template_path)

    if not include_ids and not merge_groups:
        typer.echo("No OGs specified in the decision template. Nothing to do.")
        raise typer.Exit(code=0)

    # Resolve OrthoFinder results to find Orthogroup_Sequences/
    if results_dir is not None:
        of_root = results_dir.expanduser().resolve()
    elif run_id is not None:
        of_root = paths.run_dir(run_id) / "orthofinder_results"
    else:
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

    # Prefer og_placed/ (has standalone genes appended) unless --no-placed
    og_placed_dir = paths.family_og_placed_dir(family_id)
    if not no_placed and og_placed_dir.is_dir() and any(og_placed_dir.glob("*.fa")):
        og_seq_dir = og_placed_dir
        typer.echo(f"Using placed OGs from: {og_placed_dir}")
    else:
        of_results = _find_of_results_dir(of_root)
        og_seq_dir = of_results / "Orthogroup_Sequences"
        if not no_placed and not og_placed_dir.is_dir():
            typer.echo("No og_placed/ found — using Orthogroup_Sequences/ directly")

    # Output directory
    out_dir = paths.family_og_selected_dir(family_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    merged = 0
    missing: list[str] = []

    # Process includes
    for og_id in include_ids:
        src = og_seq_dir / f"{og_id}.fa"
        if src.is_file():
            shutil.copy2(src, out_dir / f"{og_id}.fa")
            copied += 1
        else:
            missing.append(og_id)
            typer.echo(f"WARNING: {og_id}.fa not found in Orthogroup_Sequences/")

    # Process merges
    for group in merge_groups:
        first_og = group[0]
        out_name = f"merge_{first_og}.fa"
        parts: list[str] = []
        group_missing = False
        for og_id in group:
            src = og_seq_dir / f"{og_id}.fa"
            if src.is_file():
                content = src.read_text(encoding="utf-8")
                if not content.endswith("\n"):
                    content += "\n"
                parts.append(content)
            else:
                missing.append(og_id)
                group_missing = True
                typer.echo(f"WARNING: {og_id}.fa not found in Orthogroup_Sequences/")
        if parts:
            (out_dir / out_name).write_text("".join(parts), encoding="utf-8")
            merged += 1

    typer.echo(f"Copied {copied} OGs as-is")
    typer.echo(f"Created {merged} merged FASTAs")
    if missing:
        typer.echo(f"Missing: {len(missing)} OG files")
    typer.echo(f"Output: {out_dir}")
    typer.echo(f"Ready for: phylo-slurm --input-dir {out_dir}")

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_og_apply",
            "family_id": family_id,
            "run_id": run_id,
            "n_included": copied,
            "n_merged": merged,
            "n_merge_groups": len(merge_groups),
            "n_missing": len(missing),
            "output_dir": str(out_dir),
        },
    )
