from __future__ import annotations

import typer

from fungalphylo.cli.commands.protsetphylo.align import align_command
from fungalphylo.cli.commands.protsetphylo.build_fasta import build_fasta_command
from fungalphylo.cli.commands.protsetphylo.init_family import init_command
from fungalphylo.cli.commands.protsetphylo.interproscan import interproscan_command
from fungalphylo.cli.commands.protsetphylo.og_apply import og_apply_command
from fungalphylo.cli.commands.protsetphylo.og_report import og_report_command
from fungalphylo.cli.commands.protsetphylo.select import select_command
from fungalphylo.cli.commands.protsetphylo.tree import tree_command

app = typer.Typer(
    help="Gene family phylogenomics sub-pipeline: init → interproscan → select → og-report → og-apply → align → tree.",
)

app.command(name="init")(init_command)
app.command(name="interproscan")(interproscan_command)
app.command(name="select")(select_command)
app.command(name="build-fasta")(build_fasta_command)
app.command(name="og-report")(og_report_command)
app.command(name="og-apply")(og_apply_command)
app.command(name="align")(align_command)
app.command(name="tree")(tree_command)
