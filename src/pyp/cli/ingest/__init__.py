from typer import Typer

from pyp.cli.ingest.commands.currencies import IngestCurrencies
from pyp.cli.ingest.commands.stocks import IngestStocks

ingest = Typer(name="ingest", help="Ingest various market data from external services.")


@ingest.command(name="stocks", help="Ingests stock market prices for specific monikers.")
def stocks() -> None:
    IngestStocks().ingest()


@ingest.command(name="currencies", help="Ingests currency rates for specific currency pairs.")
def currencies() -> None:
    IngestCurrencies().ingest()
