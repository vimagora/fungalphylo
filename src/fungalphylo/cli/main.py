from __future__ import annotations

import typer

from fungalphylo.cli.commands import autoselect as autoselect_cmd
from fungalphylo.cli.commands import busco as busco_cmd
from fungalphylo.cli.commands import busco_slurm as busco_slurm_cmd
from fungalphylo.cli.commands import db as db_cmd
from fungalphylo.cli.commands import download as download_cmd
from fungalphylo.cli.commands import fetch_index as fetch_index_cmd
from fungalphylo.cli.commands import idmap as idmap_cmd
from fungalphylo.cli.commands import ingest as ingest_cmd
from fungalphylo.cli.commands import init as init_cmd
from fungalphylo.cli.commands import interproscan_slurm as interproscan_slurm_cmd
from fungalphylo.cli.commands import orthofinder_slurm as orthofinder_slurm_cmd
from fungalphylo.cli.commands import protsetphylo as protsetphylo_cmd
from fungalphylo.cli.commands import restore as restore_cmd
from fungalphylo.cli.commands import review as review_cmd
from fungalphylo.cli.commands import stage as stage_cmd
from fungalphylo.cli.commands import status as status_cmd
from fungalphylo.cli.commands import taxonomy as taxonomy_cmd

app = typer.Typer(
    name="fungalphylo",
    help="Fungal phylogenomics pipeline: ingest → review → stage → compute.",
    add_completion=False,
)

# Workflow commands
app.add_typer(init_cmd.app, name="init")
app.add_typer(ingest_cmd.app, name="ingest")
app.add_typer(fetch_index_cmd.app, name="fetch-index")
app.add_typer(autoselect_cmd.app, name="autoselect")
app.add_typer(review_cmd.app, name="review")
app.add_typer(restore_cmd.app, name="restore")
app.add_typer(download_cmd.app, name="download")
app.add_typer(stage_cmd.app, name="stage")
app.add_typer(busco_cmd.app, name="busco")
app.add_typer(busco_slurm_cmd.app, name="busco-slurm")
app.add_typer(interproscan_slurm_cmd.app, name="interproscan-slurm")
app.add_typer(orthofinder_slurm_cmd.app, name="orthofinder-slurm")
app.add_typer(protsetphylo_cmd.app, name="protsetphylo")


# Helpers
app.add_typer(db_cmd.app, name="db")
app.add_typer(status_cmd.app, name="status")
app.add_typer(status_cmd.failures_app, name="failures")
app.add_typer(idmap_cmd.app, name="idmap")
app.add_typer(taxonomy_cmd.app, name="taxonomy")



def main() -> None:
    app()


if __name__ == "__main__":
    main()
