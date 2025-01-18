from typing import Annotated

import typer
from typer import Typer

from pyp.cli.currencies.commands import AddCurrencyCommand
from pyp.database.engine import engine

currency_app = Typer(name="currency", help="Manage currency DB entities.")


@currency_app.command(name="add", help="Add a moniker to the portfolio.")
def add(
    code: Annotated[str, typer.Argument(help="The short code of the currency. (e.g. USD)")],
    name: Annotated[str, typer.Argument(help="The name of the currency. (e.g. United States Dollar)")],
) -> None:
    AddCurrencyCommand(engine, code, name).execute()
