from datetime import datetime
from typing import Annotated, Optional

import typer
from dotenv import dotenv_values
from typer import Typer

from pyp.cli.ingest.commands.exchange_rates import IngestExchangeRatesCommand
from pyp.cli.ingest.commands.stocks import IngestStocksCommand
from pyp.database.engine import engine

ingest_app = Typer(name="ingest", help="Ingest various market data from external services.")


@ingest_app.command(
    name="exchange-rates",
    help="Ingest exchange rates from currencyapi.com.",
    epilog="WARNING: Careful with the range size. It depends on your API Key tier with currencyapi.com.",
)
def exchange_rates(
    start_date: Annotated[
        datetime,
        typer.Argument(
            metavar="START_DATE",
            help="The start date to fetch data from. (Format: YYYY-MM-DD)",
            show_default=False,
        ),
    ],
    end_date: Annotated[
        datetime,
        typer.Argument(
            metavar="END_DATE",
            help="The end date to fetch data to. (Format: YYYY-MM-DD)",
            show_default=False,
        ),
    ],
) -> None:
    config = dotenv_values()

    if "FREE_CURRENCY_API_KEY" not in config or config["FREE_CURRENCY_API_KEY"] is None:
        raise ValueError("FREE_CURRENCY_API_KEY environment variable is not set.")

    IngestExchangeRatesCommand(engine, start_date, end_date, config["FREE_CURRENCY_API_KEY"]).execute()


@ingest_app.command(name="stocks", help="Ingest various stocks from Yahoo Finance.")
def stocks(
    start_date: Annotated[
        Optional[datetime],
        typer.Option(
            "--start-date",
            "-sd",
            metavar="YYYY-MM-DD",
            help="The start date to fetch data from.",
        ),
    ] = None,
    end_date: Annotated[
        Optional[datetime],
        typer.Option(
            "--end-date", "-ed", metavar="YYYY-MM-DD", help="The end date to fetch data to. (YYYY-MM-DD format)"
        ),
    ] = None,
    monikers: Annotated[
        Optional[list[str]],
        typer.Option(
            "--moniker",
            "-m",
            metavar="TICKER",
            help="The monikers to include. If not provided the system will detect all monikers in the DB.",
        ),
    ] = None,
    exclude_monikers: Annotated[
        Optional[list[str]],
        typer.Option(
            "--exclude-moniker",
            "-em",
            metavar="TICKER",
            help="The monikers to exclude. Takes precedence over monikers to include.",
        ),
    ] = None,
) -> None:
    IngestStocksCommand(
        engine,
        start_date=start_date,
        end_date=end_date,
        monikers=monikers,
        exclude_monikers=exclude_monikers,
    ).execute()
