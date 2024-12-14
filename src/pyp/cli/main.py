from typing import Annotated

import typer
from typer import Typer

from pyp.cli.commands import SetupCommand
from pyp.database.engine import engine

from .currencies import currency_app
from .ingest import ingest
from .plot import plot_app
from .portfolio import portfolio_app
from .user import user_app

app = Typer(name="pyp", help="PyPortfolio is a python tool for visualizing your financial portfolio.")
app.add_typer(ingest, name=ingest.info.name, help=ingest.info.help)
app.add_typer(plot_app, name=plot_app.info.name, help=plot_app.info.help)
app.add_typer(currency_app, name=currency_app.info.name, help=currency_app.info.help)
app.add_typer(user_app, name=user_app.info.name, help=user_app.info.help)
app.add_typer(portfolio_app, name=portfolio_app.info.name, help=portfolio_app.info.help)

if __name__ == "__main__":
    app()


@app.command(name="setup", help="Creates the database and sets up the project.")
def setup(
    seed: Annotated[
        bool,
        typer.Option("--seed", "-s", help="Seed the database with common records."),
    ] = False,
) -> None:
    SetupCommand(engine, seed).execute()
