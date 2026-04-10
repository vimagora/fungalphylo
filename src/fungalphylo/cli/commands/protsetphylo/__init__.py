from __future__ import annotations

import typer

from fungalphylo.cli.commands.protsetphylo.init_family import init_command
from fungalphylo.cli.commands.protsetphylo.interproscan import interproscan_command
from fungalphylo.cli.commands.protsetphylo.og_apply import og_apply_command
from fungalphylo.cli.commands.protsetphylo.og_report import og_report_command
from fungalphylo.cli.commands.protsetphylo.phylo_slurm import phylo_slurm_command
from fungalphylo.cli.commands.protsetphylo.place_standalone import place_standalone_command
from fungalphylo.cli.commands.protsetphylo.clade_mark import clade_mark_command
from fungalphylo.cli.commands.protsetphylo.select import select_command
from fungalphylo.cli.commands.protsetphylo.tree_export import tree_export_command

app = typer.Typer(
    help="Gene family phylogenomics sub-pipeline: init → interproscan → select → og-report → og-apply → place-standalone → phylo-slurm → tree-export → clade-mark.",
)

app.command(name="init")(init_command)
app.command(name="interproscan")(interproscan_command)
app.command(name="select")(select_command)
app.command(name="og-report")(og_report_command)
app.command(name="og-apply")(og_apply_command)
app.command(name="place-standalone")(place_standalone_command)
app.command(name="phylo-slurm")(phylo_slurm_command)
app.command(name="tree-export")(tree_export_command)
app.command(name="clade-mark")(clade_mark_command)
