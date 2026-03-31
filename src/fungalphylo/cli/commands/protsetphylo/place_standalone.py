from __future__ import annotations

import json
import subprocess
from pathlib import Path

import typer

from fungalphylo.core.events import log_event
from fungalphylo.core.ids import now_iso, now_tag
from fungalphylo.core.paths import ProjectPaths, ensure_project_dirs
from fungalphylo.core.slurm import infer_account_from_project_dir
from fungalphylo.core.tools import bin_dir_export_lines, load_tools
from fungalphylo.db.db import init_db

app = typer.Typer()


def _find_of_og_sequences_dir(results_root: Path) -> Path:
    """Find the Orthogroup_Sequences directory inside OrthoFinder results."""
    candidates = sorted(results_root.glob("Results_*"), reverse=True)
    for c in candidates:
        og_seq = c / "Orthogroup_Sequences"
        if og_seq.is_dir():
            return og_seq
    raise typer.BadParameter(
        f"No OrthoFinder Results_* with Orthogroup_Sequences/ found in {results_root}"
    )


def _resolve_of_root(
    paths: ProjectPaths,
    run_id: str | None,
    results_dir: Path | None,
) -> tuple[Path, str | None]:
    """Resolve OrthoFinder results root. Returns (of_root, resolved_run_id)."""
    if results_dir is not None:
        return results_dir.expanduser().resolve(), run_id
    if run_id is not None:
        return paths.run_dir(run_id) / "orthofinder_results", run_id
    # Auto-detect latest
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
    resolved = candidates[0][1]
    typer.echo(f"Using OrthoFinder run: {resolved}")
    return paths.run_dir(resolved) / "orthofinder_results", resolved


def _render_place_script(
    *,
    acct: str,
    rid: str,
    logs_dir: Path,
    time: str,
    cpus: int,
    mem_per_cpu: str,
    partition: str,
    og_sequences_dir: Path,
    og_placed_dir: Path,
    standalone_dir: Path,
    work_dir: Path,
    bin_export: str,
    mafft_cmd: str,
    hmmbuild_cmd: str,
    hmmsearch_cmd: str,
    evalue: float,
) -> str:
    return f"""#!/bin/bash
#SBATCH --account={acct}
#SBATCH --job-name=place_{rid}
#SBATCH --output={logs_dir.as_posix()}/%x_%j.out
#SBATCH --error={logs_dir.as_posix()}/%x_%j.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --partition={partition}

set -euo pipefail

module load biokit

{bin_export}
THREADS="${{SLURM_CPUS_PER_TASK:-1}}"

OG_SRC="{og_sequences_dir.as_posix()}"
OG_PLACED="{og_placed_dir.as_posix()}"
STANDALONE_DIR="{standalone_dir.as_posix()}"
WORK="{work_dir.as_posix()}"
EVALUE="{evalue}"

mkdir -p "$WORK/alignments" "$WORK/profiles" "$OG_PLACED"

# Copy all OG FASTAs to og_placed/ (working copies)
cp "$OG_SRC"/*.fa "$OG_PLACED/" 2>/dev/null || true

# Concatenate all standalone FASTAs
STANDALONE_FASTA="$WORK/standalone_all.faa"
cat "$STANDALONE_DIR"/*.faa > "$STANDALONE_FASTA"

N_STANDALONE=$(grep -c '^>' "$STANDALONE_FASTA" || true)
N_OGS=$(ls "$OG_PLACED"/*.fa 2>/dev/null | wc -l || true)

echo "=== place-standalone ==="
echo "OGs:                $N_OGS"
echo "Standalone seqs:    $N_STANDALONE"
echo "E-value threshold:  $EVALUE"
echo "Threads:            $THREADS"

if [ "$N_STANDALONE" -eq 0 ]; then
  echo "No standalone sequences found. Nothing to do."
  exit 0
fi

if [ "$N_OGS" -eq 0 ]; then
  echo "No OG FASTAs found. Nothing to do."
  exit 0
fi

# Step 1: Align each OG and build HMM profile
echo "--- Building HMM profiles ---"
for OG_FASTA in "$OG_PLACED"/*.fa; do
  OG_NAME=$(basename "$OG_FASTA" .fa)
  ALN="$WORK/alignments/$OG_NAME.aln"
  HMM="$WORK/profiles/$OG_NAME.hmm"

  "{mafft_cmd}" --auto --thread "$THREADS" "$OG_FASTA" > "$ALN" 2>/dev/null

  if [ ! -s "$ALN" ]; then
    echo "WARNING: MAFFT produced empty alignment for $OG_NAME, skipping" >&2
    continue
  fi

  "{hmmbuild_cmd}" --amino "$HMM" "$ALN" > /dev/null 2>&1
  echo "  Built profile: $OG_NAME"
done

# Step 2: Concatenate all profiles into one database
echo "--- Running hmmsearch ---"
ALLHMM="$WORK/all_profiles.hmm"
cat "$WORK/profiles"/*.hmm > "$ALLHMM"

# Step 3: Search standalone sequences against all profiles
RESULTS="$WORK/hmmsearch_results.tbl"
"{hmmsearch_cmd}" --tblout "$RESULTS" -E "$EVALUE" --cpu "$THREADS" \\
  "$ALLHMM" "$STANDALONE_FASTA" > "$WORK/hmmsearch_full.out" 2>&1

# Step 4: Parse results — best OG per standalone sequence
echo "--- Parsing placements ---"
PLACEMENTS="$WORK/placements.tsv"
echo -e "sequence\\tbest_og\\tevalue\\tscore" > "$PLACEMENTS"

# tblout columns: $1=target(sequence) $3=query(OG) $5=e-value $6=score
# Sort by sequence name then e-value, keep best OG per sequence
awk '!/^#/ {{ print $1, $3, $5, $6 }}' "$RESULTS" | \\
  sort -k1,1 -k3,3g | \\
  awk '!seen[$1]++ {{ print $1 "\\t" $2 "\\t" $3 "\\t" $4 }}' >> "$PLACEMENTS"

# Step 5: Append each standalone sequence to its best OG in og_placed/
echo "--- Appending sequences ---"
APPENDED=0
while IFS=$'\\t' read -r SEQ_ID BEST_OG SEQ_EVAL SEQ_SCORE; do
  [ "$SEQ_ID" = "sequence" ] && continue
  OG_FASTA="$OG_PLACED/$BEST_OG.fa"
  if [ -f "$OG_FASTA" ]; then
    # Extract the sequence from the standalone FASTA and append
    awk -v id="$SEQ_ID" '
      BEGIN {{ found=0 }}
      /^>/ {{ found=(substr($1,2)==id); if(found) print; next }}
      found {{ print }}
    ' "$STANDALONE_FASTA" >> "$OG_FASTA"
    APPENDED=$((APPENDED + 1))
    echo "  $SEQ_ID -> $BEST_OG (e=$SEQ_EVAL)"
  else
    echo "  WARNING: No matching OG file for $BEST_OG, skipping $SEQ_ID" >&2
  fi
done < "$PLACEMENTS"

echo ""
echo "=== Done ==="
echo "Placed $APPENDED standalone sequences"
echo "Placements report: $PLACEMENTS"
"""


def place_standalone_command(
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
    time: str | None = typer.Option(None, "--time", help="SLURM time (default: 02:00:00)"),
    cpus: int | None = typer.Option(None, "--cpus", help="CPUs per task (default: 8)"),
    mem_per_cpu: str | None = typer.Option(None, "--mem-per-cpu", help="Memory per CPU (default: 2G)"),
    partition: str | None = typer.Option(None, "--partition", help="SLURM partition"),
    account: str | None = typer.Option(None, "--account", help="SLURM account"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Skip account confirmation"),
    evalue: float = typer.Option(1e-5, "--evalue", help="hmmsearch e-value threshold"),
    submit: bool = typer.Option(False, "--submit", help="Submit with sbatch after writing script"),
) -> None:
    project_dir = project_dir.expanduser().resolve()
    paths = ProjectPaths(project_dir)
    ensure_project_dirs(paths)
    init_db(paths.db_path)

    tools = load_tools(project_dir)

    # Resolve OrthoFinder results
    of_root, run_id = _resolve_of_root(paths, run_id, results_dir)
    if not of_root.is_dir():
        raise typer.BadParameter(f"Results directory does not exist: {of_root}")
    og_sequences_dir = _find_of_og_sequences_dir(of_root)

    # Verify standalone directory exists
    standalone_dir = paths.family_selected_dir(family_id) / "standalone"
    if not standalone_dir.is_dir() or not any(standalone_dir.glob("*.faa")):
        raise typer.BadParameter(
            f"No standalone FASTAs found in: {standalone_dir}\n"
            "Nothing to place — all characterized genes may have portal IDs."
        )

    og_files = sorted(og_sequences_dir.glob("*.fa"))
    standalone_files = sorted(standalone_dir.glob("*.faa"))
    if not og_files:
        raise typer.BadParameter(f"No .fa files in {og_sequences_dir}")

    # Output directory
    og_placed_dir = paths.family_og_placed_dir(family_id)

    # Account
    inferred = infer_account_from_project_dir(project_dir)
    acct = account or inferred
    if not acct:
        raise typer.BadParameter(
            "Could not infer SLURM account. Provide --account explicitly."
        )
    if not no_confirm and account is None:
        ok = typer.confirm(
            f"Detected SLURM account '{acct}'. Use this account?", default=True,
        )
        if not ok:
            raise typer.BadParameter("Account not confirmed. Re-run with --account.")

    # Defaults
    rid = f"place_{family_id}_{now_tag()}"
    time = time or "02:00:00"
    cpus = cpus if cpus is not None else 8
    mem_per_cpu = mem_per_cpu or "2G"
    partition = partition or "small"

    # Work directory within the family
    work_dir = paths.family_dir(family_id) / "place_standalone"
    slurm_dir = work_dir / "slurm"
    logs_dir = paths.logs_dir / "slurm"

    slurm_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    bin_export = bin_dir_export_lines([
        tools.mafft.bin_dir,
        tools.hmmer.bin_dir,
    ])

    script = _render_place_script(
        acct=acct,
        rid=rid,
        logs_dir=logs_dir,
        time=time,
        cpus=cpus,
        mem_per_cpu=mem_per_cpu,
        partition=partition,
        og_sequences_dir=og_sequences_dir,
        og_placed_dir=og_placed_dir,
        standalone_dir=standalone_dir,
        work_dir=work_dir,
        bin_export=bin_export,
        mafft_cmd=tools.mafft.command,
        hmmbuild_cmd=tools.hmmer.hmmbuild_cmd,
        hmmsearch_cmd=tools.hmmer.hmmsearch_cmd,
        evalue=evalue,
    )

    script_path = slurm_dir / "place_standalone.sbatch"
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    n_standalone = sum(1 for f in standalone_files for _ in f.read_text().splitlines() if _.startswith(">"))

    typer.echo(f"Wrote place-standalone SLURM script: {script_path}")
    typer.echo(f"OGs to profile:      {len(og_files)}")
    typer.echo(f"Standalone files:    {len(standalone_files)} ({n_standalone} sequences)")
    typer.echo(f"OG source:           {og_sequences_dir}")
    typer.echo(f"OG placed output:    {og_placed_dir}")
    typer.echo(f"Work directory:      {work_dir}")

    log_event(
        project_dir,
        {
            "ts": now_iso(),
            "event": "protsetphylo_place_standalone",
            "family_id": family_id,
            "run_id": run_id,
            "n_ogs": len(og_files),
            "n_standalone_files": len(standalone_files),
            "n_standalone_seqs": n_standalone,
            "og_sequences_dir": str(og_sequences_dir),
            "og_placed_dir": str(og_placed_dir),
            "script_path": str(script_path),
            "account": acct,
            "submit": submit,
        },
    )

    if submit:
        try:
            res = subprocess.run(
                ["sbatch", str(script_path)], check=True, capture_output=True, text=True
            )
            typer.echo(res.stdout.strip() or "Submitted.")
        except FileNotFoundError:
            raise RuntimeError(
                "sbatch not found on PATH. Submit manually with: sbatch <script>"
            ) from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"sbatch failed: {e.stderr.strip() if e.stderr else str(e)}"
            ) from e
