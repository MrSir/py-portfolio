from datetime import datetime
from typing import Annotated

import typer
from typer import Typer

from pyp.services.exporters.breakdown import BreakdownExporter
from pyp.services.exporters.growth import GrowthExporter
from pyp.services.exporters.growth_breakdown import (
    GrowthBreakdownExporter,
    GrowthBreakdownMonthOverMonthExporter,
)
from pyp.services.exporters.summary import SummaryExporter
from pyp.cli.commands.setup import SetupCommand
from pyp.cli.common import resolve_portfolio
from pyp.database.engine import engine

from pyp.cli.currencies.app import currency_app
from pyp.cli.ingest.app import ingest_app
from pyp.cli.portfolio.app import portfolio_app
from pyp.cli.user.app import user_app

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
            help="Seed the database with USD, CAD, EUR, CHF Currencies and Exchange Rates between "
            "(January 2020 - February 2025).",
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

    SummaryExporter(engine, username, portfolio_name, portfolio_id, date, currency_code).execute()
    GrowthExporter(engine, portfolio_id, date, currency_code).execute()
    BreakdownExporter(engine, portfolio_id, date, currency_code).execute()
    # GrowthBreakdownExporter(engine, portfolio_id, date, currency_code).execute()
    # GrowthBreakdownMonthOverMonthExporter(engine, portfolio_id, date, currency_code).execute()
