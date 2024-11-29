from typer import Typer

ingest = Typer(name="ingest", help="Ingest various market data from external services.")


@ingest.command(
    name="stocks",
    help="Ingests stock market prices for specific monikers."
)
def stocks() -> None:
    pass


@ingest.command(
    name="currency",
    help="Ingests currency rates for specific currency pairs."
)
def currency() -> None:
    pass
