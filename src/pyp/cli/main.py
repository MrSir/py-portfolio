from datetime import datetime
from typing import Annotated

import typer
from typer import Typer

from pyp.cli.commands.output.breakdown import OutputBreakdownCommand
from pyp.cli.commands.output.growth import OutputGrowthCommand
from pyp.cli.commands.output.growth_breakdown import (
    OutputGrowthBreakdownCommand,
    OutputGrowthBreakdownMonthOverMonthCommand,
)
from pyp.cli.commands.output.summary import OutputSummaryCommand
from pyp.cli.commands.setup import SetupCommand
from pyp.cli.common import resolve_portfolio
from pyp.database.engine import engine

from .currencies import currency_app
from .ingest import ingest_app
from .portfolio import portfolio_app
from .user import user_app

app = Typer(name="pyp", help="PyPortfolio is a python tool for visualizing your financial portfolio.")
app.add_typer(currency_app, name=currency_app.info.name, help=currency_app.info.help)
app.add_typer(ingest_app, name=ingest_app.info.name, help=ingest_app.info.help)
app.add_typer(user_app, name=user_app.info.name, help=user_app.info.help)
app.add_typer(portfolio_app, name=portfolio_app.info.name, help=portfolio_app.info.help)

if __name__ == "__main__":
    app()


@app.command(name="setup", help="Creates the database and sets up the project.")
def setup(
    seed: Annotated[
        bool,
        typer.Option(
            "--seed",
            "-s",
            help="Seed the database with USD, CAD, EUR Currencies and Exchange Rates between "
            "(January, 2021 - January, 2025).",
        ),
    ] = False,
) -> None:
    SetupCommand(engine, seed).execute()


@app.command(name="output", help="Output various chart data of the portfolio.")
def output(
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio name.")],
    date: Annotated[
        datetime,
        typer.Option(
            "--date",
            "-d",
            metavar="YYYY-MM-DD",
            help="The date to output for.",
        ),
    ] = datetime.today(),
    currency_code: Annotated[
        str,
        typer.Option(
            "--currency",
            "-c",
            metavar="CUR",
            help="The 3 letter code of the currency to convert all values to.",
        ),
    ] = "USD",
) -> None:
    portfolio_id = resolve_portfolio(username, portfolio_name).id

    OutputSummaryCommand(engine, username, portfolio_name, portfolio_id, date, currency_code).execute()
    OutputGrowthCommand(engine, portfolio_id, date, currency_code).execute()
    OutputBreakdownCommand(engine, portfolio_id, date, currency_code).execute()
    OutputGrowthBreakdownCommand(engine, portfolio_id, date, currency_code).execute()
    OutputGrowthBreakdownMonthOverMonthCommand(engine, portfolio_id, date, currency_code).execute()
