from typing import Annotated, Optional

import typer
from typer import Typer

from pyp.cli.ingest.commands.currencies import IngestCurrencies
from pyp.cli.ingest.commands.stocks import IngestStocks

ingest = Typer(name="ingest", help="Ingest various market data from external services.")


@ingest.command(name="stocks", help="Ingests stock market prices for specific monikers.")
def stocks(
    start_date: Annotated[
        Optional[str], typer.Argument(metavar="YYYY-MM-DD", help="The start date to fetch pricing from.")
    ] = None,
    end_date: Annotated[
        Optional[str],
        typer.Argument(metavar="YYYY-MM-DD", help="The start date to fetch pricing from. (YYYY-MM-DD format)"),
    ] = None,
) -> None:
    IngestStocks().ingest(start_date=start_date, end_date=end_date)


@ingest.command(name="currencies", help="Ingests currency rates for specific currency pairs.")
def currencies() -> None:
    IngestCurrencies().ingest()
