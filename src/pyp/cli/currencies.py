from typing import Annotated

import typer
from sqlalchemy.orm import Session
from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import Currency

currency_app = Typer(name="currency", help="Manage currency DB entities.")


@currency_app.command(name="add", help="Add a moniker to the portfolio.")
def add(
    name: Annotated[str, typer.Argument(help="The short name of the currency. (e.g. USD)")],
    full_name: Annotated[str, typer.Argument(help="The full name of the currency. (e.g. United States Dollar)")],
) -> None:
    with Session(engine) as session:
        currency = Currency(name=name, full_name=full_name)
        session.add(currency)
        session.commit()
