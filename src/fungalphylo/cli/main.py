from __future__ import annotations

import typer

from fungalphylo.cli.commands import init as init_cmd

app = typer.Typer(
    name="fungalphylo",
    help="Fungal phylogenomics pipeline: ingest → review → stage → compute.",
    add_completion=False,
)

app.add_typer(init_cmd.app, name="init")


def main() -> None:
    app()


if __name__ == "__main__":
    main()