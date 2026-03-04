from __future__ import annotations

import typer

from fungalphylo.cli.commands import init as init_cmd
from fungalphylo.cli.commands import ingest as ingest_cmd
from fungalphylo.cli.commands import fetch_index as fetch_index_cmd
from fungalphylo.cli.commands import autoselect as autoselect_cmd
from fungalphylo.cli.commands import review as review_cmd
from fungalphylo.cli.commands import restore as restore_cmd
from fungalphylo.cli.commands import stage as stage_cmd
from fungalphylo.cli.commands import idmap as idmap_cmd

from fungalphylo.cli.commands import db as db_cmd


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
app.add_typer(stage_cmd.app, name="stage")
app.add_typer(idmap_cmd.app, name="idmap")


#helpers
app.add_typer(db_cmd.app, name="db")


def main() -> None:
    app()


if __name__ == "__main__":
    main()